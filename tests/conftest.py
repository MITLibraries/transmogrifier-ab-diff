# ruff: noqa: PD901
import datetime
import glob
import json
import os
import shutil
import time
import warnings
from concurrent.futures import Future
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pandas as pd
import pyarrow as pa
import pytest
from click.testing import CliRunner
from freezegun import freeze_time

from abdiff.core import calc_ab_diffs, create_final_records, init_job, init_run
from abdiff.core.calc_ab_metrics import (
    _prepare_duckdb_context,
    calc_ab_metrics,
)
from abdiff.core.collate_ab_transforms import (
    TRANSFORMED_DATASET_SCHEMA,
    get_transformed_batches_iter,
)
from abdiff.core.utils import create_subdirectories, load_dataset, write_to_dataset


class Container:
    """Stub for docker.models.container.Container object."""

    def __init__(self, id, short_id, labels, attrs: dict | None = None):  # noqa: A002
        self.id = id
        self.short_id = short_id
        self.labels = labels
        self.status = "created"
        self.run_duration = 0.5
        self.start_time = time.time()

        if attrs:
            self.attrs = attrs
        else:
            self.attrs = {"State": {"ExitCode": 0}}

    def reload(self):
        """Simulate reload the status of a Docker container.

        Checking if a detached Container has completed is performed by running its
        reload method.  This waits 0.1s, then only resolves to 'exited' if the container
        instance has existed longer than self.run_duration, thereby simulating a total
        running time of X seconds.
        """
        time.sleep(0.1)
        if time.time() - self.start_time >= self.run_duration:
            self.status = "exited"

    def logs(self, *, stream: bool = True):
        with open("tests/fixtures/transmogrifier-logs.txt", "rb") as file:
            if stream:
                yield from file
            return file.read()

    def stop(self):
        pass


class MockedContainerRun:
    """Class for mocking container runs of Transmogrifier."""

    def __init__(self, *, errors: bool = False) -> None:
        self.count = 0
        self.errors = errors

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

    def create_transformed_files(
        self, transformed_directory: str, container_id: str, image_name: str
    ) -> Container:
        with open(
            Path(transformed_directory)
            / "source-2024-01-01-daily-transformed-records-to-index.json",
            "w",
        ) as tmp_file:
            tmp_file.write("Hello world!")

        if self.errors:
            return Container(
                id=container_id,
                short_id=container_id[:3],
                labels={
                    "docker_image": image_name,
                    "source": "source",
                    "input_file": "s3://timdex-extract-dev/source/source-2024-01-01-daily-extracted-records-to-index.xml",
                },
                attrs={"State": {"ExitCode": 1}},
            )
        return Container(
            id=container_id,
            short_id=container_id[:3],
            labels={
                "docker_image": image_name,
                "source": "source",
                "input_file": "s3://timdex-extract-dev/source/source-2024-01-01-daily-extracted-records-to-index.xml",
            },
        )


class MockedFutureSuccess(Future):

    def __init__(self, container: Container, exception: Exception | None = None):
        self.container = container
        self.exception = exception

    def result(self, timeout=None):  # noqa: ARG002
        return self.container, self.exception


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
def example_job_directory(tmp_path):
    """Copy example job from fixtures to tmp path where it will be modified during test"""
    source_dir = Path("tests/fixtures/jobs/example-job-1")
    dest_dir = tmp_path / "example-job-1"
    shutil.copytree(source_dir, dest_dir)
    return dest_dir


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
        short_id="abc",
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
        short_id="def",
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
def mocked_container_failed_runs_iter():
    return MockedContainerRun(errors=True)


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


@pytest.fixture
def collated_dataset_directory(run_directory):
    """Simulate the outputs of core function collate_ab_transforms."""
    dataset_directory = str(Path(run_directory) / "collated")
    df = pd.DataFrame(
        [
            {
                "timdex_record_id": "abc123",
                "source": "alma",
                "run_date": "2024-10-01",
                "run_type": "full",
                "action": "index",
                "record_a": json.dumps(
                    {"material": "concrete", "color": "green", "number": 42}
                ).encode(),
                "record_b": json.dumps(
                    {"material": "concrete", "color": "red", "number": 42}
                ).encode(),
            },
            {
                "timdex_record_id": "def456",
                "source": "dspace",
                "run_date": "2024-10-01",
                "run_type": "full",
                "action": "index",
                "record_a": json.dumps(
                    {"material": "concrete", "color": "blue", "number": 101}
                ).encode(),
                "record_b": json.dumps(
                    {"material": "concrete", "color": "blue", "number": 101}
                ).encode(),
            },
            {
                "timdex_record_id": "ghi789",
                "source": "libguides",
                "run_date": "2024-10-01",
                "run_type": "full",
                "action": "index",
                "record_a": json.dumps(
                    {
                        "material": "concrete",
                        "color": "purple",
                        "number": 13,
                        "fruit": "apple",
                    }
                ).encode(),
                "record_b": json.dumps(
                    {"material": "concrete", "color": "brown", "number": 99}
                ).encode(),
            },
        ]
    )
    write_to_dataset(
        pa.Table.from_pandas(df),
        base_dir=dataset_directory,
    )
    return dataset_directory


@pytest.fixture
def collated_with_dupe_dataset_directory(run_directory):
    """Simulate the outputs of core function collate_ab_transforms."""
    dataset_directory = str(Path(run_directory) / "collated")
    df = pd.DataFrame(
        [
            {
                "abdiff_record_id": "ece5ee65-20af-4410-89d9-1afe58531be8",
                "timdex_record_id": "abc123",
                "source": "alma",
                "run_date": "2024-10-01",
                "run_type": "full",
                "action": "index",
                "record_a": json.dumps(
                    {"material": "concrete", "color": "green", "number": 42}
                ).encode(),
                "record_b": json.dumps(
                    {"material": "concrete", "color": "red", "number": 42}
                ).encode(),
            },
            {
                "abdiff_record_id": "f1b4670c-cbfe-4246-8cb9-be9eabd78756",
                "timdex_record_id": "def456",
                "source": "dspace",
                "run_date": "2024-10-01",
                "run_type": "full",
                "action": "index",
                "record_a": json.dumps(
                    {"material": "concrete", "color": "blue", "number": 101}
                ).encode(),
                "record_b": json.dumps(
                    {"material": "concrete", "color": "blue", "number": 101}
                ).encode(),
            },
            {
                "abdiff_record_id": "49e9aa7f-11f2-41d3-aab4-4c76efab51aa",
                "timdex_record_id": "def456",
                "source": "dspace",
                "run_date": "2024-10-02",
                "run_type": "full",
                "action": "delete",
                "record_a": json.dumps(
                    {"material": "concrete", "color": "blue", "number": 101}
                ).encode(),
                "record_b": json.dumps(
                    {"material": "concrete", "color": "blue", "number": 101}
                ).encode(),
            },
            {
                "abdiff_record_id": "7fafe666-a435-4939-b50a-ee059b36fb9f",
                "timdex_record_id": "ghi789",
                "source": "libguides",
                "run_date": "2024-10-01",
                "run_type": "full",
                "action": "index",
                "record_a": json.dumps(
                    {
                        "material": "concrete",
                        "color": "purple",
                        "number": 13,
                        "fruit": "apple",
                    }
                ).encode(),
                "record_b": json.dumps(
                    {"material": "concrete", "color": "brown", "number": 99}
                ).encode(),
            },
            {
                "abdiff_record_id": "47d038ec-f231-4634-b20e-97e26c7829ea",
                "timdex_record_id": "ghi789",
                "source": "libguides",
                "run_date": "2024-10-02",
                "run_type": "daily",
                "action": "index",
                "record_a": json.dumps(
                    {
                        "material": "stucco",
                        "color": "green",
                        "number": 42,
                        "fruit": "banana",
                    }
                ).encode(),
                "record_b": json.dumps(
                    {
                        "material": "stucco",
                        "color": "green",
                        "number": 42,
                        "fruit": "banana",
                    }
                ).encode(),
            },
        ]
    )
    write_to_dataset(
        pa.Table.from_pandas(df),
        base_dir=dataset_directory,
    )
    return dataset_directory


@pytest.fixture
def collated_dataset(collated_dataset_directory):
    return load_dataset(collated_dataset_directory)


@pytest.fixture
def metrics_directory(run_directory):
    directory = Path(run_directory) / "metrics"
    os.makedirs(directory, exist_ok=True)
    return str(directory)


@pytest.fixture
def diffs_dataset_directory(
    run_directory, metrics_directory, collated_dataset_directory
) -> str:
    return calc_ab_diffs(run_directory, collated_dataset_directory)


@pytest.fixture
def diff_matrix_dataset_filepath(run_directory, diffs_dataset_directory) -> str:
    return calc_ab_metrics(run_directory, diffs_dataset_directory)


@pytest.fixture
def diff_matrix_df(diff_matrix_dataset_filepath) -> pd.DataFrame:
    diff_matrix_ds = load_dataset(diff_matrix_dataset_filepath)
    return diff_matrix_ds.to_table().to_pandas()


@pytest.fixture
def function_duckdb_connection():
    with duckdb.connect(":memory:") as conn:
        yield conn


@pytest.fixture
def duckdb_context_with_diff_matrix(
    function_duckdb_connection, diff_matrix_dataset_filepath
):
    fields, sources = _prepare_duckdb_context(
        function_duckdb_connection, diff_matrix_dataset_filepath
    )
    return function_duckdb_connection, fields, sources


@pytest.fixture
def final_records_dataset_path(
    run_directory, diffs_dataset_directory, diff_matrix_dataset_filepath
):
    return create_final_records(
        run_directory=run_directory,
        diffs_dataset_path=diffs_dataset_directory,
        metrics_dataset_path=diff_matrix_dataset_filepath,
    )


@pytest.fixture
def collating_intermediate_transformed_dataset(run_directory, tmp_path):
    shutil.copytree(
        "tests/fixtures/collating/sample_scenarios/transformed",
        Path(run_directory) / "transformed",
    )
    ab_transformed_file_lists = [
        [
            file.removeprefix(run_directory + "/")
            for file in glob.glob(f"{run_directory}/transformed/{version}/*.*")
        ]
        for version in ["a", "b"]
    ]
    transformed_dataset_filepath = tmp_path / "transformed_dataset"
    write_to_dataset(
        get_transformed_batches_iter(run_directory, tuple(ab_transformed_file_lists)),
        schema=TRANSFORMED_DATASET_SCHEMA,
        base_dir=transformed_dataset_filepath,
        partition_columns=["transformed_file_name"],
    )
    return transformed_dataset_filepath


@pytest.fixture
def mocked_transformed_files_500(run_directory):
    """Generate 500 A and B transformed files (1k total)."""
    transformed_dir = Path(run_directory) / "transformed"
    for version in ["a", "b"]:
        version_path = transformed_dir / version
        os.makedirs(version_path)
        start_date = datetime.datetime(2020, 1, 1)  # noqa: DTZ001
        for x in range(500):
            current_date = start_date + datetime.timedelta(days=x)
            transformed_filepath = (
                f"libguides-{current_date.strftime('%Y-%m-%d')}-"
                "daily-transformed-records-to-index.json"
            )
            with open(version_path / transformed_filepath, "w") as f:
                json.dump(
                    [
                        {
                            "timdex_record_id": f"libguides:{x}",
                            "source": "libguides",
                            "title": "I am title",
                        }
                    ],
                    f,
                )
    return transformed_dir


@pytest.fixture
def mocked_transformed_files_500_ab_list(run_directory, mocked_transformed_files_500):
    return (
        [
            file.removeprefix(f"{run_directory}/")
            for file in glob.glob(f"{mocked_transformed_files_500}/a/*.json")
        ],
        [
            file.removeprefix(f"{run_directory}/")
            for file in glob.glob(f"{mocked_transformed_files_500}/b/*.json")
        ],
    )


@pytest.fixture
def records_duckdb_filepath():
    return "tests/fixtures/jobs/example-job-3/runs/2024-11-07_03-56-22/run.duckdb"
