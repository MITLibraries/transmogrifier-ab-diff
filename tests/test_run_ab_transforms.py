# ruff: noqa: ARG005, PLR2004, S108
import os
from pathlib import Path
from unittest import mock

import pytest

from abdiff.core.exceptions import (
    DockerContainerTimeoutError,
)
from abdiff.core.run_ab_transforms import (
    collect_container_results,
    run_ab_transforms,
    run_docker_container,
    write_log_file,
)
from abdiff.core.utils import parse_timdex_filename
from tests.conftest import MockedContainerRun, MockedFutureSuccess


def test_run_ab_transforms_success(
    run_directory,
    transformed_directories,
    mocked_docker_client,
    mocked_container_runs_iter,
):
    mocked_docker_client.containers.run.side_effect = (
        lambda *args, **kwargs: mocked_container_runs_iter.yield_mocked_run(
            transformed_directories
        )
    )

    input_files = [
        "s3://timdex-extract-dev/source/source-2024-01-01-daily-extracted-records-to-index.xml"
    ]
    run_ab_transforms(
        run_directory=run_directory,
        image_tag_a="transmogrifier-example-job-1-abc123:latest",
        image_tag_b="transmogrifier-example-job-1-def456:latest",
        input_files=input_files,
        docker_client=mocked_docker_client,
    )

    assert os.path.exists(run_directory)
    assert len(os.listdir(Path(run_directory) / "logs")) == len(input_files) * 2

    for transformed_directory in ["a", "b"]:
        assert os.path.exists(Path(run_directory) / "transformed" / transformed_directory)
        assert os.path.exists(
            Path(run_directory) / "transformed" / transformed_directory / "dataset"
        )


def test_run_ab_transforms_raise_error_if_containers_failed(
    run_directory,
    transformed_directories,
    mocked_docker_client,
    mocked_container_failed_runs_iter,
    caplog,
):
    caplog.set_level("DEBUG")
    mocked_docker_client.containers.run.side_effect = (
        lambda *args, **kwargs: mocked_container_failed_runs_iter.yield_mocked_run(
            transformed_directories
        )
    )

    with pytest.raises(
        RuntimeError,
        match="2 / 2 containers failed to complete successfully.",
    ):
        run_ab_transforms(
            run_directory=run_directory,
            image_tag_a="transmogrifier-example-job-1-abc123:latest",
            image_tag_b="transmogrifier-example-job-1-def456:latest",
            input_files=[
                "s3://timdex-extract-dev/source/source-2024-01-01-daily-extracted-records-to-index.xml"
            ],
            docker_client=mocked_docker_client,
        )


def test_run_docker_container_success(
    run_directory,
    mocked_docker_client,
    mocked_docker_container_and_image,
    create_transformed_directories,
):
    """Test will run for each set of params in parametrized fixtures.

    This test depends on the 'mocked_docker_container_and_image' fixture,
    which will return a mocked docker container and the corresponding
    image name that would've been used by the container.
    """
    mocked_docker_container, image_name = mocked_docker_container_and_image
    transformed_directory_a, transformed_directory_b = create_transformed_directories

    if mocked_docker_container.id == "abc123":
        transformed_directory = transformed_directory_a
    elif mocked_docker_container.id == "def456":
        transformed_directory = transformed_directory_b

    mocked_docker_client.containers.run.side_effect = lambda *args, **kwargs: (
        MockedContainerRun().create_transformed_dataset(
            transformed_directory=transformed_directory,
            container_id=mocked_docker_container.id,
            image_name=image_name,
        )
    )

    input_file = "s3://timdex-extract-dev/source/source-2024-01-01-daily-extracted-records-to-index.xml"
    filename_details = parse_timdex_filename(input_file)
    container, _ = run_docker_container(
        docker_image=image_name,
        run_directory=run_directory,
        transformed_directory=transformed_directory,
        source=filename_details["source"],
        input_file=input_file,
        docker_client=mocked_docker_client,
    )
    assert container.id == mocked_docker_container.id
    assert os.path.exists(
        Path(run_directory) / "transformed" / transformed_directory / "dataset"
    )


def test_run_docker_container_timeout_triggered(
    run_directory, mocked_docker_client, mocked_docker_container_a
):
    mocked_docker_client.containers.run.return_value = mocked_docker_container_a
    mocked_docker_container_a.run_duration = 2
    timeout = 1
    _, exception = run_docker_container(
        "abc123",
        run_directory,
        "abc",
        "alma",
        "/tmp/source-2024-01-01-full-extracted-records-to-index.xml",
        mocked_docker_client,
        timeout=timeout,
    )
    assert isinstance(exception, DockerContainerTimeoutError)


def test_run_docker_container_timeout_not_triggered(
    mocked_docker_client, mocked_docker_container_a
):
    mocked_docker_client.containers.run.return_value = mocked_docker_container_a
    mocked_docker_container_a.run_duration = 2
    timeout = 3
    container, _ = run_docker_container(
        "abc123",
        "run",
        "abc",
        "alma",
        "/tmp/source-2024-01-01-full-extracted-records-to-index.xml",
        mocked_docker_client,
        timeout=timeout,
    )
    assert container.status == "exited"


def test_run_docker_container_unhandled_error_still_returns_container_and_exception(
    mocked_docker_client, mocked_docker_container_a
):
    mocked_docker_client.containers.run.return_value = mocked_docker_container_a

    mocked_exception = Exception("Error checking container status!")
    with mock.patch.object(mocked_docker_container_a, "reload") as mocked_reload:
        mocked_reload.side_effect = mocked_exception
        container, exception = run_docker_container(
            "abc123",
            "run",
            "abc",
            "alma",
            "/tmp/input.xml",
            mocked_docker_client,
        )

    assert container == mocked_docker_container_a
    assert exception == mocked_exception


def test_collect_containers_two_success_return_zero_exceptions(
    mocked_docker_container_a, mocked_docker_container_b
):
    futures = [
        MockedFutureSuccess(container=mocked_docker_container_a, exception=None),
        MockedFutureSuccess(container=mocked_docker_container_b, exception=None),
    ]

    exceptions = collect_container_results(futures)
    assert len(exceptions) == 0


def test_collect_containers_return_exception(
    mocked_docker_container_a, mocked_docker_container_b
):
    mocked_exception = Exception("Container failure!")
    futures = [
        MockedFutureSuccess(container=mocked_docker_container_a, exception=None),
        MockedFutureSuccess(
            container=mocked_docker_container_b,
            exception=mocked_exception,
        ),
    ]
    exceptions = collect_container_results(futures)

    assert len(exceptions) == 1
    assert exceptions[0] == mocked_exception


def test_write_log_file_success(run_directory, mocked_docker_container_a):
    write_log_file(
        run_directory=run_directory,
        input_file="s3://timdex-extract-dev/source/source-2024-01-01-daily-extracted-records-to-index.xml",
        container=mocked_docker_container_a,
    )
    assert os.path.exists(
        Path(run_directory) / "logs/source-2024-01-01-daily-abc-logs.txt"
    )
