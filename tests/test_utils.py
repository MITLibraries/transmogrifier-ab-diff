# ruff: noqa: D417, D205

import json
import os.path
import resource
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pytest

from abdiff.core import init_job
from abdiff.core.collate_ab_transforms import (
    TRANSFORMED_DATASET_SCHEMA,
    get_transformed_batches_iter,
)
from abdiff.core.utils import (
    create_subdirectories,
    load_dataset,
    parse_timdex_filename,
    read_job_json,
    update_or_create_job_json,
    write_to_dataset,
)


def test_read_job_json_no_job_raise_error(job_directory):
    with pytest.raises(OSError, match="No such file or directory"):
        read_job_json(job_directory)


def test_read_job_json_success(example_job_directory):
    job_data = read_job_json(example_job_directory)
    with open(Path(example_job_directory) / "job.json") as f:
        assert job_data == json.load(f)


def test_create_job_json_file_success(tmp_path):
    job_json_filepath = tmp_path / "job.json"
    assert not os.path.exists(job_json_filepath)
    update_or_create_job_json(tmp_path, {"msg": "in a bottle"})
    assert os.path.exists(job_json_filepath)
    with open(job_json_filepath) as f:
        assert json.load(f) == {"msg": "in a bottle"}


def test_update_job_json_file_success(job_directory):
    message = "I am an example job."
    init_job(job_directory, message=message)
    update_or_create_job_json(job_directory, {"msg": "in a bottle"})
    job_data = read_job_json(job_directory)
    assert job_data == {
        "job_directory": job_directory,
        "job_message": message,
        "msg": "in a bottle",
    }


def test_create_sub_directories_success(tmp_path):
    base_directory = str(tmp_path / "base")
    subdirectory_a, subdirectory_b = create_subdirectories(
        base_directory,
        subdirectories=["subdirectory/a", "subdirectory/b"],
    )
    assert os.path.exists(subdirectory_a)
    assert os.path.exists(subdirectory_b)


def test_parse_timdex_filename_s3_uri_success():
    assert parse_timdex_filename(
        "s3://timdex-extract-dev/source/source-2024-01-01-full-extracted-records-to-index.xml"
    ) == {
        "source": "source",
        "run-date": "2024-01-01",
        "run-type": "full",
        "stage": "extracted",
        "action": "index",
        "index": None,
        "file_type": "xml",
    }


def test_parse_timdex_filename_indexed_s3_uri_success():
    assert parse_timdex_filename(
        "s3://timdex-extract-dev/source/source-2024-01-01-full-extracted-records-to-index_01.xml"
    ) == {
        "source": "source",
        "run-date": "2024-01-01",
        "run-type": "full",
        "stage": "extracted",
        "action": "index",
        "index": "01",
        "file_type": "xml",
    }


def test_parse_timdex_filename_filename_success():
    assert parse_timdex_filename(
        "source-2024-01-01-full-extracted-records-to-index.xml"
    ) == {
        "source": "source",
        "run-date": "2024-01-01",
        "run-type": "full",
        "stage": "extracted",
        "action": "index",
        "index": None,
        "file_type": "xml",
    }


def test_parse_timdex_filename_raise_error_if_invalid_filename():
    with pytest.raises(ValueError, match="Provided S3 URI and filename is invalid."):
        parse_timdex_filename(s3_uri_or_filename="invalid")


def test_write_to_dataset_success(tmp_path):
    record_batch = pa.RecordBatch.from_pylist(
        [{"fruit": "apple", "color": "red"}, {"fruit": "banana", "color": "yellow"}]
    )
    test_schema = pa.schema(
        [pa.field("fruit", pa.string()), pa.field("color", pa.string())]
    )
    write_to_dataset(
        data=record_batch,
        base_dir=tmp_path / "test_dataset",
        schema=test_schema,
        partition_columns=["fruit"],
    )
    assert set(os.listdir(tmp_path / "test_dataset")) == {"fruit=apple", "fruit=banana"}


@pytest.mark.parametrize(
    "test_params",
    [
        # simulate low OS level open file limit and function correctly aligns with it
        pytest.param(
            {
                "ulimit": 100,
                "max_open_files": None,
                "expected_error": None,
                "expected_log": "OS open file limit of 100 detected",
                "description": "Dynamic file limit succeeds",
            },
            id="dynamic_limit_success",
        ),
        # simulate low OS level open file limit and override exceeds it raising error
        pytest.param(
            {
                "ulimit": 100,
                "max_open_files": 1024,
                "expected_error": OSError,
                "expected_log": None,
                "description": "Exceeding file limit raises error",
            },
            id="exceed_limit_error",
        ),
    ],
)
def test_write_to_dataset_parallel_write_and_open_file_limits(
    test_params: dict,
    caplog,
    tmp_path,
    run_directory,
    mocked_transformed_files_500_ab_list,
) -> None:
    """Test that write_to_dataset() correctly manages open files during parallel writing
    to align with OS level configurations.

    This test mimics the writing of the temporary transformed file dataset which
    originally revealed the need for this limiting.  This operation creates partitions
    based on the transformed file name, where parallel writes were holding these
    directories + files open for writing.  However, this applies generally to the function
    write_to_dataset() when called from anywhere.

    Args:
        test_params: Dictionary containing:
            - ulimit: OS-level file limit to set
            - max_open_files: Value to pass to write_to_dataset
            - expected_error: Expected exception or None
            - expected_log: Expected log message or None
            - description: Human-readable test description
    """
    caplog.set_level("DEBUG")

    # set OS-level open file limit (aka "ulimit")
    resource.setrlimit(
        resource.RLIMIT_NOFILE, (test_params["ulimit"], test_params["ulimit"])
    )

    transformed_dataset_filepath = str(tmp_path / "transformed_dataset")

    # test case expects success
    if test_params["expected_error"] is None:
        write_to_dataset(
            get_transformed_batches_iter(
                run_directory, mocked_transformed_files_500_ab_list
            ),
            schema=TRANSFORMED_DATASET_SCHEMA,
            base_dir=transformed_dataset_filepath,
            partition_columns=["transformed_file_name"],
            max_open_files=test_params["max_open_files"],
        )

        if test_params["expected_log"]:
            assert test_params["expected_log"] in caplog.text

        transformed_dataset = load_dataset(transformed_dataset_filepath)
        assert isinstance(transformed_dataset, ds.Dataset)

    # test case expects an error
    else:
        with pytest.raises(test_params["expected_error"], match="Too many open files"):
            write_to_dataset(
                get_transformed_batches_iter(
                    run_directory, mocked_transformed_files_500_ab_list
                ),
                schema=TRANSFORMED_DATASET_SCHEMA,
                base_dir=transformed_dataset_filepath,
                partition_columns=["transformed_file_name"],
                max_open_files=test_params["max_open_files"],
            )
