# ruff: noqa: ARG005
import os
from pathlib import Path

import pytest

from abdiff.core.exceptions import (
    DockerContainerRunFailedError,
    DockerContainerRuntimeExceededTimeoutError,
    OutputValidationError,
)
from abdiff.core.run_ab_transforms import (
    aggregate_logs,
    get_transformed_files,
    parse_transform_details_from_extract_filename,
    run_ab_transforms,
    run_docker_container,
    validate_output,
    wait_for_containers,
)
from tests.conftest import MockedContainerRun


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

    with pytest.raises(DockerContainerRunFailedError):
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
    source, output_file = parse_transform_details_from_extract_filename(input_file)
    container = run_docker_container(
        docker_image=image_name,
        transformed_directory=transformed_directory,
        source=source,
        input_file=input_file,
        output_file=output_file,
        docker_client=mocked_docker_client,
    )
    assert container.id == mocked_docker_container.id
    assert os.path.isfile(
        Path(transformed_directory)
        / "source-2024-01-01-daily-transformed-records-to-index.json"
    )


def test_wait_for_containers_success(
    caplog, mocked_docker_container_a, mocked_docker_container_b
):
    wait_for_containers(containers=[mocked_docker_container_a, mocked_docker_container_b])
    assert "Container abc123 exited." in caplog.text
    assert "Container def456 exited." in caplog.text
    assert mocked_docker_container_a.status == "exited"
    assert mocked_docker_container_b.status == "exited"


def test_wait_for_containers_raise_error(
    mocked_docker_container_a, mocked_docker_container_b
):
    with pytest.raises(DockerContainerRuntimeExceededTimeoutError):
        wait_for_containers(
            containers=[mocked_docker_container_a, mocked_docker_container_b],
            timeout=0,  # force timeout
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


def test_validate_output_success():
    assert (
        validate_output(
            ab_transformed_file_lists=(["transformed/a/file1"], ["transformed/b/file2"]),
            input_files_count=1,
        )
        is None
    )


def test_validate_output_error():
    with pytest.raises(OutputValidationError):
        validate_output(ab_transformed_file_lists=([], []), input_files_count=1)


def test_parse_transform_details_from_extract_filename_success(input_file):
    source, output_file = parse_transform_details_from_extract_filename(input_file)
    assert source == "source"
    assert output_file == "source-2024-01-01-full-transformed-records-to-index.json"


def test_parse_transform_details_from_extract_filename_if_sequenced_success():
    input_file = "s3://timdex-extract-dev/source/source-2024-01-01-full-extracted-records-to-index_01.xml"
    source, output_filename = parse_transform_details_from_extract_filename(input_file)
    assert source == "source"
    assert (
        output_filename == "source-2024-01-01-full-transformed-records-to-index_01.json"
    )


def test_parse_transform_details_from_extract_filename_raise_error():
    input_file = "s3://timdex-extract-dev/source/-2024-01-01-full-extracted-records-to-index_01.xml"
    with pytest.raises(ValueError, match="Extract filename is invalid"):
        parse_transform_details_from_extract_filename(input_file)
