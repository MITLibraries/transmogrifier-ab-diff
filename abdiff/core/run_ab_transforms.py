"""abdiff.core.run_ab_transforms"""

import glob
import logging
import os
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
from abdiff.core.utils import (
    create_subdirectories,
    parse_timdex_filename,
    update_or_create_run_json,
)

CONFIG = Config()

logger = logging.getLogger(__name__)


def run_ab_transforms(
    run_directory: str,
    image_tag_a: str,
    image_tag_b: str,
    input_files: list[str],
    docker_client: docker.client.DockerClient | None = None,
    *,
    use_local_s3: bool = False,
) -> tuple[list[str], ...]:
    """Run Docker containers with versioned images of Transmogrifier.

    Parallelization is handled by invoking the Docker containers via threads, limited by
    the ThreadPoolExecutor.max_workers argument.  Each thread invokes a detached Docker
    container and manages its lifecycle until completion. As each container exits, logs
    are written to a text file in the run directory; log filenames follow the format:
    "{source}-{run-date}-{run-type}-{container-short-id}-logs.txt".

    Args:
        run_directory (str): Run directory.
        image_tag_a (str): Image name for version A of transmogrifier.
        image_tag_b (str): Image name for version B of transmogrifier.
        input_files (list[str]): Input files for transform. Currently, only
            URIs for input files on S3 are accepted.
        docker_client (docker.client.DockerClient | None, optional): Docker client.
            Defaults to None.
        use_local_s3 (bool): Boolean indicating whether the container should
            access input files from a local MinIO server (i.e., "local S3 bucket")
            or from AWS S3. This flag determines the appropriate environment variables
            to set for the Docker containers. Default is False.

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
        base_directory=run_directory,
        subdirectories=["transformed/a", "transformed/b"],
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
    futures = run_all_docker_containers(
        docker_client, input_files, run_configs, run_directory, use_local_s3=use_local_s3
    )
    exceptions = collect_container_results(futures)
    logger.info(
        f"Successful containers: {len(futures)}, failed containers: {len(exceptions)}"
    )

    # process results
    if not CONFIG.allow_failed_transmogrifier_containers and exceptions:
        raise RuntimeError(  # noqa: TRY003
            f"{len(exceptions)} / {len(futures)} containers failed "
            "to complete successfully."
        )
    ab_transformed_file_lists = get_transformed_files(run_directory)
    validate_output(ab_transformed_file_lists, input_files)

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
    run_directory: str,
    *,
    use_local_s3: bool = False,
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
            filename_details = parse_timdex_filename(input_file)
            output_file = get_transformed_filename(filename_details)
            for docker_image, transformed_directory in run_configs:
                args = (
                    docker_image,
                    run_directory,
                    transformed_directory,
                    str(filename_details["source"]),
                    input_file,
                    output_file,
                    docker_client,
                )
                tasks.append(
                    executor.submit(
                        run_docker_container, *args, use_local_s3=use_local_s3
                    )
                )

    logger.info(f"All {len(tasks)} containers have exited.")
    return tasks


def run_docker_container(
    docker_image: str,
    run_directory: str,
    transformed_directory: str,
    source: str,
    input_file: str,
    output_file: str,
    docker_client: docker.client.DockerClient,
    timeout: int = CONFIG.transmogrifier_timeout,
    *,
    use_local_s3: bool = False,
) -> tuple[Container, Exception | None]:
    """Run Transmogrifier via Docker container to transform input file.

    The container is run in a detached state to capture a container handle for later use
    but this function waits for the container to exit before returning.
    """
    if use_local_s3:
        environment_variables = {
            "AWS_ENDPOINT_URL": CONFIG.minio_s3_container_url,
            "AWS_ACCESS_KEY_ID": CONFIG.minio_root_user,
            "AWS_SECRET_ACCESS_KEY": CONFIG.minio_root_password,
        }
    else:
        environment_variables = {
            "AWS_ACCESS_KEY_ID": CONFIG.AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": CONFIG.AWS_SECRET_ACCESS_KEY,
            "AWS_SESSION_TOKEN": CONFIG.AWS_SESSION_TOKEN,
        }

    container = docker_client.containers.run(
        docker_image,
        command=[
            f"--input-file={input_file}",
            f"--output-file=/tmp/{output_file}",
            f"--source={source}",
        ],
        detach=True,
        environment=environment_variables,
        labels={
            "docker_image": docker_image,
            "source": source,
            "input_file": input_file,
        },
        volumes=[f"{os.path.abspath(transformed_directory)}:/tmp"],
    )
    logger.info(f"Transmogrifier container ({container.short_id}) STARTED: {input_file}")

    exception = None
    try:
        start_time = perf_counter()
        while True:
            time.sleep(0.5)
            container.reload()
            elapsed_time = time.perf_counter() - start_time
            if container.status == "exited":
                logger.info(
                    f"Transmogrifier container ({container.short_id}) EXITED, "
                    f"elapsed {timedelta(seconds=elapsed_time)}: {input_file}"
                )
                write_log_file(run_directory, input_file, container)
                break

            if time.perf_counter() - start_time > timeout:
                logger.error(
                    f"Transmogrifier container ({container.short_id}) TIMED OUT, "
                    f"elapsed {timedelta(seconds=elapsed_time)}: {input_file}"
                )
                container.stop()
                exception = DockerContainerTimeoutError(
                    container_id=container.id, timeout=timeout
                )
                write_log_file(
                    run_directory,
                    input_file,
                    container,
                    extra_messages=[
                        f"Transmogrifier container ({container.short_id}) TIMED OUT"
                    ],
                )
                break

    except Exception as e:
        exception = e  # type: ignore[assignment]
        logger.exception(
            f"Transmogrifier container ({container.short_id}) UNHANDLED EXCEPTION: {input_file}"  # noqa: E501
        )

    return container, exception


def collect_container_results(futures: list[Future]) -> list[Exception]:
    """Collect results of container executions.

    Each future will contain a tuple of (Container, Exception) where the exception may
    be None.  A success is considered when the container exited cleanly and no exceptions
    are present.

    Returns a tuple of (Containers (success), Exceptions (failure)) from all executions.
    """
    exceptions = []
    for future in futures:
        container, exception = future.result()

        if exception:
            exceptions.append(exception)
        if container.attrs["State"]["ExitCode"] != 0:
            exceptions.append(DockerContainerRuntimeError(container.id))

    logger.info(
        f"Container results collected: {len(futures) - len(exceptions)} successes, "
        f"{len(exceptions)} failures"
    )
    return exceptions


def write_log_file(
    run_directory: str,
    input_file: str,
    container: Container,
    extra_messages: list[str] | None = None,
) -> None:
    """Write logs for a given container to a text file."""
    filename_details = parse_timdex_filename(input_file)
    log_filename = "{source}-{run_date}-{run_type}-{container_id}-logs.txt".format(
        source=filename_details["source"],
        run_date=filename_details["run-date"],
        run_type=filename_details["run-type"],
        container_id=container.short_id,
    )
    log_filepath = str(Path(run_directory) / "logs" / log_filename)
    header = (
        f"docker_image: {container.labels['docker_image']} | "
        f"source: {container.labels['source']} | "
        f"input_file: {container.labels['input_file']}"
    )
    container_desc = f"container: {container.id}"
    with open(log_filepath, "w") as file:
        file.write(header + "\n")
        file.write(container_desc + "\n")
        for log in container.logs(stream=True):
            file.write(log.decode())

        if extra_messages:
            file.write("\n".join(extra_messages))


def get_transformed_files(run_directory: str) -> tuple[list[str], ...]:
    """Get list of filepaths to transformed JSON files.

    Args:
        run_directory (str): Run directory.

    Returns:
        tuple[list[str]]: Tuple containing lists of paths to transformed
            JSON and TXT (deletions) files for each image, relative to 'run_directory'.
    """
    ordered_files = []
    for version in ["a", "b"]:
        absolute_filepaths = glob.glob(f"{run_directory}/transformed/{version}/*")
        relative_filepaths = [
            os.path.relpath(file, run_directory) for file in absolute_filepaths
        ]
        ordered_files.append(relative_filepaths)
    return tuple(ordered_files)


def validate_output(
    ab_transformed_file_lists: tuple[list[str], ...], input_files: list[str]
) -> None:
    """Validate the output of run_ab_transforms.

    Transmogrifier produces JSON files for records that need indexing, and TXT files for
    records that need deletion.  Every run of Transmogrifier should produce one OR both of
    these.  Some TIMDEX sources provide one file to Transmogrifier that contains both
    records to index and delete, and others provide separate files for each.

    The net effect for validation is that, given an input file, we should expect to see
    1+ files in the A and B output for that input file, ignoring if it's records to index
    or delete.
    """
    for input_file in input_files:
        file_parts = parse_timdex_filename(input_file)
        logger.debug(f"Validating output for input file root: {file_parts}")

        file_found = False
        for version_files in ab_transformed_file_lists:
            for version_file in version_files:
                if (
                    file_parts["source"] in version_file  # type: ignore[operator]
                    and file_parts["run-date"] in version_file  # type: ignore[operator]
                    and file_parts["run-type"] in version_file  # type: ignore[operator]
                    and (not file_parts["index"] or file_parts["index"] in version_file)
                ):
                    file_found = True
                    break

        if not file_found:
            raise OutputValidationError(  # noqa: TRY003
                f"Transmogrifier output was not found for input file '{input_file}'"
            )


def get_transformed_filename(filename_details: dict) -> str:
    """Get transformed filename using extract filename details."""
    return (
        "{source}-{run_date}-{run_type}-{stage}-records-to-{action}{index}.json".format(
            source=filename_details["source"],
            run_date=filename_details["run-date"],
            run_type=filename_details["run-type"],
            stage="transformed",
            index=f"_{sequence}" if (sequence := filename_details["index"]) else "",
            action=filename_details["action"],
        )
    )
