"""abdiff.core.run_ab_transforms"""

import logging
import os
import time
import uuid
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
)
from abdiff.core.utils import (
    create_subdirectories,
    parse_timdex_filename,
    read_run_json,
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
) -> list[str]:
    """Generate transformed records from A and B versions of Transmogrifier.

    Transformed records are written to a TIMDEX dataset, which are collated into a single
    dataset in later steps.
    """
    start_time = perf_counter()

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
        docker_client,
        input_files,
        run_configs,
        run_directory,
        use_local_s3=use_local_s3,
    )
    exceptions = collect_container_results(futures)

    # process results
    if not CONFIG.allow_failed_transmogrifier_containers and exceptions:
        raise RuntimeError(  # noqa: TRY003
            f"{len(exceptions)} / {len(futures)} containers failed "
            "to complete successfully."
        )

    # write and return results
    run_data = {
        "input_files": input_files,
        "transformed_datasets": [transformed_directory_a, transformed_directory_b],
    }
    update_or_create_run_json(run_directory, run_data)
    elapsed_time = perf_counter() - start_time
    logger.info(
        "Total time to complete process: %s", str(timedelta(seconds=elapsed_time))
    )
    return [transformed_directory_a, transformed_directory_b]


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
            for docker_image, transformed_directory in run_configs:
                args = (
                    docker_image,
                    run_directory,
                    transformed_directory,
                    str(filename_details["source"]),
                    input_file,
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
    docker_client: docker.client.DockerClient,
    timeout: int = CONFIG.transmogrifier_timeout,
    *,
    use_local_s3: bool = False,
) -> tuple[Container, Exception | None]:
    """Run Transmogrifier via Docker container to transform input file.

    The container is run in a detached state to capture a container handle for later use
    but this function waits for the container to exit before returning.
    """
    # ensures all records written share the same run_id
    try:
        run_id = read_run_json(run_directory)["run_timestamp"]
    except FileNotFoundError:
        logger.warning("Run JSON not found, probably testing, minting unique run UUID.")
        run_id = str(uuid.uuid4())

    environment_variables = {
        # NOTE: FEATURE FLAG: ETL_VERSION not required once v2 is fully implemented
        "ETL_VERSION": "2",
    }

    if use_local_s3:
        environment_variables.update(
            {
                "AWS_ENDPOINT_URL": CONFIG.minio_s3_container_url,
                "AWS_ACCESS_KEY_ID": CONFIG.minio_root_user,
                "AWS_SECRET_ACCESS_KEY": CONFIG.minio_root_password,
            }
        )
    else:
        environment_variables.update(
            {
                "AWS_ACCESS_KEY_ID": CONFIG.AWS_ACCESS_KEY_ID,
                "AWS_SECRET_ACCESS_KEY": CONFIG.AWS_SECRET_ACCESS_KEY,
                "AWS_SESSION_TOKEN": CONFIG.AWS_SESSION_TOKEN,
            }
        )

    container = docker_client.containers.run(
        docker_image,
        command=[
            f"--input-file={input_file}",
            f"--output-location=/tmp/{transformed_directory}/dataset",
            f"--source={source}",
            f"--run-id={run_id}",
        ],
        detach=True,
        environment=environment_variables,
        labels={
            "docker_image": docker_image,
            "source": source,
            "input_file": input_file,
        },
        volumes=[
            f"{os.path.abspath(transformed_directory)}:/tmp/{transformed_directory}"
        ],
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
