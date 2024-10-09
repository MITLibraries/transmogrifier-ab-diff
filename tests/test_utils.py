import json
import os.path
from pathlib import Path

import pyarrow as pa
import pytest

from abdiff.core import init_job
from abdiff.core.utils import (
    create_subdirectories,
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
