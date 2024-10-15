import random
import time
import warnings
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner
from freezegun import freeze_time

from abdiff.core import init_job, init_run
from abdiff.core.collate_ab_transforms import (
    TRANSFORMED_DATASET_SCHEMA,
    get_transformed_batches_iter,
)
from abdiff.core.utils import create_subdirectories, write_to_dataset


class Container:
    """Stub for docker.models.container.Container object."""

    def __init__(self, id, labels):  # noqa: A002
        self.id = id
        self.labels = labels
        self.status = "created"

    def reload(self):
        time.sleep(0.1)
        if random.randint(0, 100) > 50:  # noqa: PLR2004, S311
            self.status = "exited"

    def logs(self):
        with open("tests/fixtures/transmogrifier-logs.txt", "rb") as file:
            return file.read()


class MockedContainerRun:
    """Class for mocking container runs of Transmogrifier."""

    def __init__(self) -> None:
        self.count = 0

    def yield_mocked_run(self, transformed_directories: tuple[str]) -> None:
        """Perform a mocked run of transmogrifier.

        This function yields a different outcome based on the value of
        MockedContainerRun.count each time it is called. The function will
        create a placeholder transformed file in the A/B transformed
        directories in the correct order (first in 'transformed/a' then
        in 'transformed/b').
        """
        transformed_directory_a, transformed_directory_b = transformed_directories
        self.count += 1
        if self.count == 1:
            return self.create_transformed_files(
                transformed_directory=transformed_directory_a,
                container_id="abc123",
                image_name="transmogrifier-example-job-1-abc123:latest",
            )
        if self.count == 2:  # noqa: PLR2004
            return self.create_transformed_files(
                transformed_directory=transformed_directory_b,
                container_id="def456",
                image_name="transmogrifier-example-job-1-def456:latest",
            )
        warnings.warn("All side effects are exhausted.", UserWarning, stacklevel=2)
        return None

    @classmethod
    def create_transformed_files(
        cls, transformed_directory: str, container_id: str, image_name: str
    ) -> Container:
        with open(
            Path(transformed_directory)
            / "source-2024-01-01-daily-transformed-records-to-index.json",
            "w",
        ) as tmp_file:
            tmp_file.write("Hello world!")
        return Container(
            id=container_id,
            labels={
                "docker_image": image_name,
                "source": "source",
                "input_file": "s3://timdex-extract-dev/source/source-2024-01-01-daily-extracted-records-to-index.xml",
            },
        )


@pytest.fixture(autouse=True)
def _test_env(monkeypatch, tmp_path):
    monkeypatch.setenv("WORKSPACE", "test")
    monkeypatch.setenv("JOB_DIRECTORY", "tests/fixtures/jobs/example-job-1")


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def job_directory(tmp_path):
    return str(tmp_path / "example-job-1")


@pytest.fixture
def example_job_directory():
    return "tests/fixtures/jobs/example-job-1"


@pytest.fixture
def example_run_directory(example_job_directory):
    return str(Path(example_job_directory) / "runs/2024-01-01_12-00-00")


@pytest.fixture
def example_transformed_directory(example_run_directory):
    return str(Path(example_run_directory) / "transformed")


@pytest.fixture
def example_ab_transformed_file_lists():
    transformed_directory_a = Path("transformed/a")
    transformed_directory_b = Path("transformed/b")
    return (
        [
            transformed_directory_a
            / "alma-2024-08-29-daily-transformed-records-to-index.json",
            transformed_directory_a
            / "dspace-2024-10-14-daily-transformed-records-to-index.json",
        ],
        [
            transformed_directory_b
            / "alma-2024-08-29-daily-transformed-records-to-index.json",
            transformed_directory_b
            / "dspace-2024-10-14-daily-transformed-records-to-index.json",
        ],
    )


@pytest.fixture
def job(job_directory):
    return init_job(job_directory)


@pytest.fixture
@freeze_time("2024-01-01T12:00:00")
def run_directory(job, job_directory):
    return init_run(job_directory)


@pytest.fixture
def input_file():
    return "s3://timdex-extract-dev/source/source-2024-01-01-full-extracted-records-to-index.xml"


@pytest.fixture
def output_filename():
    return "source-2024-01-01-full-transformed-records-to-index.json"


@pytest.fixture
def transformed_directories(run_directory):
    transformed_directory_a = str(Path(run_directory) / "transformed/a")
    transformed_directory_b = str(Path(run_directory) / "transformed/b")
    return transformed_directory_a, transformed_directory_b


@pytest.fixture
def create_transformed_directories(run_directory):
    return create_subdirectories(
        base_directory=run_directory, subdirectories=["transformed/a", "transformed/b"]
    )


@pytest.fixture
def transformed_parquet_dataset(
    tmp_path, example_run_directory, example_ab_transformed_file_lists
):
    write_to_dataset(
        get_transformed_batches_iter(
            example_run_directory, example_ab_transformed_file_lists
        ),
        schema=TRANSFORMED_DATASET_SCHEMA,
        base_dir=tmp_path,
        partition_columns=["transformed_file_name"],
    )
    return tmp_path


@pytest.fixture
def mocked_docker_client():
    docker_client = MagicMock()
    docker_images = []
    for image_tag in [
        "transmogrifier-example-job-1-abc123:latest",
        "transmogrifier-example-job-1-def456:latest",
    ]:
        docker_image = MagicMock()
        docker_image.tags = [image_tag]
        docker_images.append((docker_image, ""))
    docker_client.images.build.side_effect = docker_images
    docker_client.images.list.return_value = [docker_image]
    return docker_client


@pytest.fixture(
    params=[
        "transmogrifier-example-job-1-abc123:latest",
        "transmogrifier-example-job-1-def456:latest",
    ]
)
def mocked_docker_container_and_image(
    request, mocked_docker_container_a, mocked_docker_container_b
):
    if "abc123" in request.param:
        yield mocked_docker_container_a, request.param
    elif "def456" in request.param:
        yield mocked_docker_container_b, request.param


@pytest.fixture
def mocked_docker_container_a():
    return Container(
        id="abc123",
        labels={
            "docker_image": "transmogrifier-example-job-1-abc123:latest",
            "source": "source",
            "input_file": "s3://timdex-extract-dev/source/source-2024-01-01-daily-extracted-records-to-index.xml",
        },
    )


@pytest.fixture
def mocked_docker_container_b():
    return Container(
        id="def456",
        labels={
            "docker_image": "transmogrifier-example-job-1-def456:latest",
            "source": "source",
            "input_file": "s3://timdex-extract-dev/source/source-2024-01-01-daily-extracted-records-to-index.xml",
        },
    )


@pytest.fixture
def mocked_container_runs_iter():
    return MockedContainerRun()


@pytest.fixture
def webapp_job_directory():
    return "tests/fixtures/jobs/example-job-1"


@pytest.fixture
def webapp_run_timestamp():
    return "2024-01-01_12-00-00"


@pytest.fixture
def webapp_client():
    from abdiff.webapp.app import create_app

    app = create_app()
    with app.test_client() as client:
        yield client
