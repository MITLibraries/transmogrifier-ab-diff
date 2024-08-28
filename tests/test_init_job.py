import os.path

import pytest

from abdiff.core import init_job
from abdiff.core.utils import read_job_json


def test_init_job_creates_working_directory_success(job_directory):
    assert not os.path.exists(job_directory)
    init_job(job_directory)
    assert os.path.exists(job_directory)


def test_init_job_creates_job_json_file_success(job_directory):
    message = "I am an Example job."
    init_job(job_directory, message=message)
    job_data = read_job_json(job_directory)
    assert job_data == {"job_directory": job_directory, "message": message}


def test_init_job_directory_exists_raise_error(job_directory):
    init_job(job_directory)
    with pytest.raises(OSError, match="File exists"):
        init_job(job_directory)
