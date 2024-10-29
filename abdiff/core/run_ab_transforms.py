"""abdiff.core.run_ab_transforms"""

import glob
import logging
import os
import re
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from time import perf_counter

import docker
from docker.models.containers import Container

from abdiff.config import Config
from abdiff.core.exceptions import (
    DockerContainerRuntimeError,
    DockerContainerTimeoutError,
    OutputValidationError,
)
from abdiff.core.utils import create_subdirectories, update_or_create_run_json

CONFIG = Config()

logger = logging.getLogger(__name__)


def run_ab_transforms(
    run_directory: str,
    image_tag_a: str,
    image_tag_b: str,
    input_files: list[str],
    docker_client: docker.client.DockerClient | None = None,
) -> tuple[list[str], ...]:
    """Run Docker containers with versioned images of Transmogrifier.

    The following steps are performed in sequential order:
        1. Directories are created to capture the transformed files.
        2. For all input files, an A and B version of Transmogrifier is run.
        3. Wait for all containers to complete.
        4. Aggregate logs from all containers.
        5. Validate output.
        6. Update run.json with lists describing input and transformed files.
        7. Return a tuple containing two lists representing all A and B transformed files.

    Parallelization is handled by invoking the Docker containers via threads, limited by
    the ThreadPoolExecutor.max_workers argument.  Each thread invokes a detached Docker
    container and manages its lifecycle until completion.

    Args:
        run_directory (str): Run directory.
        image_tag_a (str): Image name for version A of transmogrifier.
        image_tag_b (str): Image name for version B of transmogrifier.
        input_files (list[str]): Input files for transform. Currently, only
            URIs for input files on S3 are accepted.
        docker_client (docker.client.DockerClient | None, optional): Docker client.
            Defaults to None.

    Returns:
        tuple[list[str], ...]: A tuple containing two lists, where each list contains
            the filepaths to transformed files (relative to the run directory) generated
            by each version (A and B) of transmogrifier.

    Examples:
        (
            [
                "transformed/a/source-2024-01-01-daily-transformed-records-to-index.json",
                "transformed/a/source-2024-01-02-daily-transformed-records-to-index.json"
            ],
            [
                "transformed/b/source-2024-01-01-daily-transformed-records-to-index.json",
                "transformed/b/source-2024-01-02-daily-transformed-records-to-index.json"
            ]
        )
    """
    start_time = perf_counter()

    # initialize environment
    if not docker_client:
        docker_client = docker.from_env()
    transformed_directory_a, transformed_directory_b = create_subdirectories(
        base_directory=run_directory, subdirectories=["transformed/a", "transformed/b"]
    )
    logger.info(
        "Transformed directories created: "
        f"{[transformed_directory_a, transformed_directory_b]}"
    )
    run_configs = [
        (image_tag_a, transformed_directory_a),
        (image_tag_b, transformed_directory_b),
    ]

    # run containers and collect results
    futures = run_all_docker_containers(docker_client, input_files, run_configs)
    containers, exceptions = collect_container_results(futures)
    logger.info(
        f"Successful containers: {len(containers)}, failed containers: {len(exceptions)}"
    )

    # process results
    log_file = aggregate_logs(run_directory, containers)
    logger.info(f"Log file created: {log_file}")
    if exceptions:
        raise RuntimeError(  # noqa: TRY003
            f"{len(exceptions)} / {len(containers)} containers failed "
            "to complete successfully."
        )
    ab_transformed_file_lists = get_transformed_files(run_directory)
    validate_output(ab_transformed_file_lists, len(input_files))

    # write and return results
    run_data = {
        "input_files": input_files,
        "transformed_files": ab_transformed_file_lists,
    }
    update_or_create_run_json(run_directory, run_data)
    elapsed_time = perf_counter() - start_time
    logger.info(
        "Total time to complete process: %s", str(timedelta(seconds=elapsed_time))
    )
    return ab_transformed_file_lists


def run_all_docker_containers(
    docker_client: docker.client.DockerClient,
    input_files: list[str],
    run_configs: list[tuple],
) -> list[Future]:
    """Invoke Docker containers to run in parallel via threads.

    By default, when ThreadPoolExecutor is invoked via a context manager it will wait for
    all tasks to complete before exiting the context manager.  While each container is run
    in a detached mode, the function run_docker_container() waits for the container to
    exit making it effectively blocking.  The net result: this function will run all
    containers to completion, returning the completed tasks (Future objects) as a list.
    """
    tasks = []

    with ThreadPoolExecutor(max_workers=CONFIG.transmogrifier_max_workers) as executor:
        for input_file in input_files:
            source, output_file = parse_transform_details_from_extract_filename(
                input_file
            )
            for docker_image, transformed_directory in run_configs:
                args = (
                    docker_image,
                    transformed_directory,
                    source,
                    input_file,
                    output_file,
                    docker_client,
                )
                tasks.append(executor.submit(run_docker_container, *args))

    logger.info(f"All {len(tasks)} containers have exited.")
    return tasks


def run_docker_container(
    docker_image: str,
    transformed_directory: str,
    source: str,
    input_file: str,
    output_file: str,
    docker_client: docker.client.DockerClient,
    timeout: int = CONFIG.transmogrifier_timeout,
) -> tuple[Container, Exception | None]:
    """Run Transmogrifier via Docker container to transform input file.

    The container is run in a detached state to capture a container handle for later use
    but this function waits for the container to exit before returning.
    """
    container = docker_client.containers.run(
        docker_image,
        command=[
            f"--input-file={input_file}",
            f"--output-file=/tmp/{output_file}",
            f"--source={source}",
        ],
        detach=True,
        environment={
            "AWS_ACCESS_KEY_ID": CONFIG.AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": CONFIG.AWS_SECRET_ACCESS_KEY,
            "AWS_SESSION_TOKEN": CONFIG.AWS_SESSION_TOKEN,
        },
        labels={
            "docker_image": docker_image,
            "source": source,
            "input_file": input_file,
        },
        volumes=[f"{os.path.abspath(transformed_directory)}:/tmp"],
    )
    logger.info(
        f"Container '{container.id}' (Docker image: {docker_image}) "
        f"RUNNING transform for '{source}' input_file: {input_file}."
    )

    exception = None
    try:
        start_time = perf_counter()
        while True:
            time.sleep(0.5)
            container.reload()
            if container.status == "exited":
                logger.info(f"Container {container.id} exited.")
                break

            if time.perf_counter() - start_time > timeout:
                logger.error(
                    f"Container {container.id} timed out after {timeout} seconds"
                )
                container.stop()
                exception = DockerContainerTimeoutError(
                    container_id=container.id, timeout=timeout
                )
                break

    except Exception as e:
        exception = e  # type: ignore[assignment]
        logger.exception("Unhandled exception while waiting for container to complete.")

    return container, exception


def collect_container_results(
    futures: list[Future],
) -> tuple[list[Container], list[Exception]]:
    """Collect results of container executions.

    Each future will contain a tuple of (Container, Exception) where the exception may
    be None.  A success is considered when the container exited cleanly and no exceptions
    are present.

    Returns a tuple of (Containers (success), Exceptions (failure)) from all executions.
    """
    containers = []
    exceptions = []
    for future in futures:
        container, exception = future.result()
        containers.append(container)

        if exception:
            exceptions.append(exception)
        if container.attrs["State"]["ExitCode"] != 0:
            exceptions.append(DockerContainerRuntimeError(container.id))

    logger.info(
        f"Container results collected: {len(containers) - len(exceptions)} successes, "
        f"{len(exceptions)} failures"
    )
    return containers, exceptions


def aggregate_logs(run_directory: str, containers: list[Container]) -> str:
    """Retrieve logs for containers in a list, aggregating to a single log file."""
    log_file = str(Path(run_directory) / "transformed/logs.txt")
    with open(log_file, "w") as file:
        for container in containers:
            file.write(f"container: {container.id}\n")
            descriptors = (
                f"docker_image: {container.labels['docker_image']} | "
                f"source: {container.labels['source']} | "
                f"input_file: {container.labels['input_file']}\n"
            )
            file.write(descriptors)
            file.write(container.logs().decode())
            file.write("\n\n")
    return log_file


def get_transformed_files(run_directory: str) -> tuple[list[str], ...]:
    """Get list of filepaths to transformed JSON files.

    Args:
        run_directory (str): Run directory.

    Returns:
        tuple[list[str]]: Tuple containing lists of paths to transformed
            JSON files for each image, relative to 'run_directory'.
    """
    ordered_files = []
    for version in ["a", "b"]:
        absolute_filepaths = glob.glob(f"{run_directory}/transformed/{version}/*.json")
        relative_filepaths = [
            os.path.relpath(file, run_directory) for file in absolute_filepaths
        ]
        ordered_files.append(relative_filepaths)
    return tuple(ordered_files)


def validate_output(
    ab_transformed_file_lists: tuple[list[str], ...], input_files_count: int
) -> None:
    """Validate the output of run_ab_transforms.

    This function checks that the number of files in each of the A/B
    transformed file directories matches the number of input files
    provided to run_ab_transforms (i.e., the expected number of
    files that are transformed).
    """
    if any(
        len(transformed_files) != input_files_count
        for transformed_files in ab_transformed_file_lists
    ):
        raise OutputValidationError(  # noqa: TRY003
            "At least one or more transformed JSON file(s) are missing. "
            f"Expecting {input_files_count} transformed JSON file(s) per A/B version. "
            "Check the transformed file directories."
        )


def parse_transform_details_from_extract_filename(input_file: str) -> tuple[str, ...]:
    """Parse transform details from extract filename.

    Namely, the source and the output filename are parsed from the extract
    filename. These variables are required by the transform command.
    """
    extract_filename = input_file.split("/")[-1]
    match_result = re.match(
        r"^([\w\-]+?)-(\d{4}-\d{2}-\d{2})-(\w+)-extracted-records-to-index(?:_(\d+))?\.\w+$",
        extract_filename,
    )
    if not match_result:
        raise ValueError(  # noqa: TRY003
            f"Extract filename is invalid: {extract_filename}."
        )
    source, date, cadence, sequence = match_result.groups()
    sequence_suffix = f"_{sequence}" if sequence else ""
    output_filename = f"{source}-{date}-{cadence}-transformed-records-to-index{sequence_suffix}.json"  # noqa: E501
    return source, output_filename
