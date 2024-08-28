import os.path
from pathlib import Path

import pytest

from abdiff.core import init_job, init_run
from abdiff.core.utils import read_job_json, read_run_json


def test_init_job_creates_working_directory_success(job_directory):
    assert not os.path.exists(job_directory)
    init_job(job_directory)
    assert os.path.exists(job_directory)


def test_init_job_creates_job_json_file_success(job_directory):
    message = "I am an Example job."
    init_job(job_directory, message=message)
    job_data = read_job_json(job_directory)
    assert job_data == {"job_directory": job_directory, "job_message": message}


def test_init_job_directory_exists_raise_error(job_directory):
    init_job(job_directory)
    with pytest.raises(OSError, match="File exists"):
        init_job(job_directory)


@pytest.mark.freeze_time("2024-01-01T12:00:00")
def test_init_run_creates_working_directory_success(job):
    expected_run_directory = str(Path(job) / "runs/2024-01-01_12-00-00")
    assert not os.path.exists(expected_run_directory)
    run_directory = init_run(job)
    assert expected_run_directory == run_directory
    assert os.path.exists(run_directory)


def test_init_run_creates_run_json_file_success(job):
    message = "I am an example run."
    run_directory = init_run(job, message=message)
    run_data = read_run_json(run_directory)
    assert run_data["run_directory"] == run_directory
    assert run_data["run_message"] == message
