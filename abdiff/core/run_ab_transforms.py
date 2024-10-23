"""abdiff.core.run_ab_transforms"""

import glob
import logging
import os
import re
import time
from datetime import timedelta
from pathlib import Path
from time import perf_counter

import docker
from docker.models.containers import Container

from abdiff.config import Config
from abdiff.core.exceptions import (
    DockerContainerRunFailedError,
    DockerContainerRuntimeExceededTimeoutError,
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
    timeout: int = 600,
) -> tuple[list[str], ...]:
    """Run Docker containers with versioned images of Transmogrifier.

    The following steps are perfomed in sequential order:
        1. Directories are created to capture the transformed files.
        2. For all input files, an A and B version of Transmogrifier is run
           via detached Docker containers, resulting in all containers
           running in parallel.
        3. Wait for all containers to complete.
        4. Aggregate logs from all containers.
        5. If any containers exit, raise an error listing the IDs for failed
           containers.
        6. Validate the number of files in the A/B transformed file directories.
        7. Update run.json with lists describing input and transformed files.
        8. Return a tuple containing two lists representing all A and B transformed files.

    Args:
        run_directory (str): Run directory.
        image_tag_a (str): Image name for version A of transmogrifier.
        image_tag_b (str): Image name for version B of transmogrifier.
        input_files (list[str]): Input files for transform. Currently, only
            URIs for input files on S3 are accepted.
        docker_client (docker.client.DockerClient | None, optional): Docker client.
            Defaults to None.
        timeout (int, optional): Timeout for Docker container runs. An error is raised
            if any Docker container runs exceed the timeout. Defaults to 180 secondss.

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

    containers = []
    for input_file in input_files:
        source, output_file = parse_transform_details_from_extract_filename(input_file)
        for docker_image, transformed_directory in run_configs:
            container = run_docker_container(
                docker_image,
                transformed_directory,
                source,
                input_file,
                output_file,
                docker_client,
            )
            containers.append(container)

    failed_containers = wait_for_containers(containers, timeout)
    logger.info(f"All {len(containers)} containers have exited.")

    log_file = aggregate_logs(run_directory, containers)
    logger.info(f"Log file created: {log_file}")

    # errors from failed containers are accessible via aggregated logs
    if failed_containers:
        raise DockerContainerRunFailedError(failed_containers)

    ab_transformed_file_lists = get_transformed_files(run_directory)

    validate_output(ab_transformed_file_lists, len(input_files))

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


def run_docker_container(
    docker_image: str,
    transformed_directory: str,
    source: str,
    input_file: str,
    output_file: str,
    docker_client: docker.client.DockerClient,
) -> Container:
    """Run transmogrifier via Docker container to transform input file."""
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
    return container


def wait_for_containers(
    containers: list[Container], timeout: int = 180
) -> None | list[str]:
    """Given a list of running containers, wait until all containers exit.

    If there are any containers that exited with an error, a list of IDs
    for failed containers is returned. For more information regarding the
    specific cause of the error, consult the logs.
    """
    exited_containers: list[str] = []
    failed_containers: list[str] = []
    start_time = time.time()
    while len(exited_containers) < len(containers):
        running_containers = [
            container for container in containers if container.id not in exited_containers
        ]
        if time.time() - start_time >= timeout:
            raise DockerContainerRuntimeExceededTimeoutError(
                containers=running_containers, timeout=timeout
            )
        for container in running_containers:
            container.reload()
            if container.status == "exited":
                logger.info(f"Container {container.id} exited.")
                exited_containers.append(container.id)  # type: ignore[arg-type]
                if (exit_code := container.attrs["State"]["ExitCode"]) == 0:
                    logger.info(f"Container {container.id} ran successfully.")
                else:
                    logger.error(
                        f"Container {container.id} exited with an error: {exit_code}. "
                        "Check logs for details."
                    )
                    failed_containers.append(container.id)  # type: ignore[arg-type]
        time.sleep(0.25)

    if any(failed_containers):
        return failed_containers
    return None


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
            f"Expecting {input_files_count} transformed JSON file(s)."
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
