# ruff: noqa: ARG005, PLR2004, S108
import os
from pathlib import Path
from unittest import mock

import pytest

from abdiff.core.exceptions import (
    DockerContainerTimeoutError,
    OutputValidationError,
)
from abdiff.core.run_ab_transforms import (
    aggregate_logs,
    collect_container_results,
    get_transformed_filename,
    get_transformed_files,
    run_ab_transforms,
    run_docker_container,
    validate_output,
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

    run_ab_transforms(
        run_directory=run_directory,
        image_tag_a="transmogrifier-example-job-1-abc123:latest",
        image_tag_b="transmogrifier-example-job-1-def456:latest",
        input_files=[
            "s3://timdex-extract-dev/source/source-2024-01-01-daily-extracted-records-to-index.xml"
        ],
        docker_client=mocked_docker_client,
    )

    assert os.path.exists(run_directory)
    assert os.path.isfile(Path(run_directory) / "transformed/logs.txt")

    for transformed_directory in ["a", "b"]:
        assert os.path.exists(Path(run_directory) / "transformed" / transformed_directory)
        assert os.path.isfile(
            Path(run_directory)
            / "transformed"
            / transformed_directory
            / "source-2024-01-01-daily-transformed-records-to-index.json"
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
        MockedContainerRun().create_transformed_files(
            transformed_directory=transformed_directory,
            container_id=mocked_docker_container.id,
            image_name=image_name,
        )
    )

    input_file = "s3://timdex-extract-dev/source/source-2024-01-01-daily-extracted-records-to-index.xml"
    filename_details = parse_timdex_filename(input_file)
    container, _ = run_docker_container(
        docker_image=image_name,
        transformed_directory=transformed_directory,
        source=filename_details["source"],
        input_file=input_file,
        output_file=get_transformed_filename(filename_details),
        docker_client=mocked_docker_client,
    )
    assert container.id == mocked_docker_container.id
    assert os.path.isfile(
        Path(transformed_directory)
        / "source-2024-01-01-daily-transformed-records-to-index.json"
    )


def test_aggregate_logs_success(
    run_directory,
    create_transformed_directories,
    mocked_docker_container_a,
    mocked_docker_container_b,
):
    log_file = aggregate_logs(
        run_directory, containers=[mocked_docker_container_a, mocked_docker_container_b]
    )
    assert os.path.exists(log_file)


def test_get_transformed_files_success(
    run_directory, create_transformed_directories, output_filename
):
    transformed_directory_a, transformed_directory_b = create_transformed_directories
    with open(
        Path(transformed_directory_a) / output_filename,
        "w",
    ) as tmp_file_a, open(
        Path(transformed_directory_b) / output_filename,
        "w",
    ) as tmp_file_b:
        tmp_file_a.write("This is the transformed JSON file created by version A")
        tmp_file_b.write("This is the transformed JSON file created by version B.")

    assert get_transformed_files(run_directory) == (
        [
            "transformed/a/source-2024-01-01-full-transformed-records-to-index.json",
        ],
        [
            "transformed/b/source-2024-01-01-full-transformed-records-to-index.json",
        ],
    )


@pytest.mark.parametrize(
    ("ab_files", "input_files"),
    [
        # single JSON from single file
        (
            (
                ["dspace-2024-04-10-daily-extracted-records-to-index.json"],
                ["dspace-2024-04-10-daily-extracted-records-to-index.json"],
            ),
            ["s3://X/dspace-2024-04-10-daily-extracted-records-to-index.xml"],
        ),
        # JSON and TXT from single file
        (
            (
                [
                    "dspace-2024-04-10-daily-extracted-records-to-index.json",
                    "dspace-2024-04-10-daily-extracted-records-to-delete.txt",
                ],
                [
                    "dspace-2024-04-10-daily-extracted-records-to-index.json",
                    "dspace-2024-04-10-daily-extracted-records-to-delete.txt",
                ],
            ),
            ["s3://X/dspace-2024-04-10-daily-extracted-records-to-index.xml"],
        ),
        # handles indexed files when multiple
        (
            (
                ["alma-2024-04-10-daily-extracted-records-to-index_09.json"],
                ["alma-2024-04-10-daily-extracted-records-to-index_09.json"],
            ),
            ["s3://X/alma-2024-04-10-daily-extracted-records-to-index_09.xml"],
        ),
        # handles deletes only for alma deletes
        (
            (
                ["alma-2024-04-10-daily-extracted-records-to-delete.txt"],
                ["alma-2024-04-10-daily-extracted-records-to-delete.txt"],
            ),
            ["s3://X/alma-2024-04-10-daily-extracted-records-to-delete.xml"],
        ),
    ],
)
def test_validate_output_success(ab_files, input_files):
    assert (
        validate_output(
            ab_transformed_file_lists=ab_files,
            input_files=input_files,
        )
        is None
    )


@pytest.mark.parametrize(
    ("ab_files", "input_files"),
    [
        # nothing returned
        (
            ([], []),
            ["s3://X/dspace-2024-04-10-daily-extracted-records-to-index.xml"],
        ),
        # output files don't have index, or wrong index, so not direct match
        (
            (
                [
                    "alma-2024-04-10-daily-extracted-records-to-index.json",
                    "alma-2024-04-10-daily-extracted-records-to-index_04.json",
                ],
                [
                    "alma-2024-04-10-daily-extracted-records-to-index.json",
                    "alma-2024-04-10-daily-extracted-records-to-index_04.json",
                ],
            ),
            ["s3://X/alma-2024-04-10-daily-extracted-records-to-index_09.xml"],
        ),
    ],
)
def test_validate_output_error(ab_files, input_files):
    with pytest.raises(OutputValidationError):
        validate_output(ab_transformed_file_lists=ab_files, input_files=input_files)


def test_get_output_filename_success():
    assert (
        get_transformed_filename(
            {
                "source": "source",
                "run-date": "2024-01-01",
                "run-type": "full",
                "stage": "extracted",
                "action": "index",
                "index": None,
                "file_type": "json",
            }
        )
        == "source-2024-01-01-full-transformed-records-to-index.json"
    )


def test_run_docker_container_timeout_triggered(
    mocked_docker_client, mocked_docker_container_a
):
    mocked_docker_client.containers.run.return_value = mocked_docker_container_a
    mocked_docker_container_a.run_duration = 2
    timeout = 1
    _, exception = run_docker_container(
        "abc123",
        "abc",
        "alma",
        "/tmp/input.xml",
        "/tmp/output.json",
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
        "abc",
        "alma",
        "/tmp/input.xml",
        "/tmp/output.json",
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
            "abc",
            "alma",
            "/tmp/input.xml",
            "/tmp/output.json",
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

    containers, exceptions = collect_container_results(futures)
    assert len(containers) == 2
    assert containers == [mocked_docker_container_a, mocked_docker_container_b]
    assert len(exceptions) == 0


def test_collect_containers_return_containers_and_exceptions(
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
    containers, exceptions = collect_container_results(futures)

    assert len(containers) == 2
    assert len(exceptions) == 1
    assert exceptions[0] == mocked_exception


def test_get_output_filename_indexed_success():
    assert (
        get_transformed_filename(
            {
                "source": "source",
                "run-date": "2024-01-01",
                "run-type": "full",
                "stage": "extracted",
                "action": "index",
                "index": "01",
                "file_type": "json",
            }
        )
        == "source-2024-01-01-full-transformed-records-to-index_01.json"
    )
