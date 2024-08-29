import json
import os.path
from pathlib import Path

import pytest

from abdiff.core import init_job
from abdiff.core.utils import read_job_json, update_or_create_job_json


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
