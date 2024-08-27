import os

import pytest
from click.testing import CliRunner
from slugify import slugify

from abdiff.core.utils import (
    get_job_slug_and_working_directory,
)


@pytest.fixture(autouse=True)
def _test_env(monkeypatch, tmp_path):
    monkeypatch.setenv("WORKSPACE", "test")
    monkeypatch.setenv("ROOT_WORKING_DIRECTORY", str(tmp_path / "output"))


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def job_name():
    return "Large Refactor Project"


@pytest.fixture
def job_slug(job_name):
    return slugify(job_name)


@pytest.fixture
def job_working_directory(job_name):
    job_slug, job_dir = get_job_slug_and_working_directory(job_name)
    os.makedirs(job_dir)
    return job_dir
