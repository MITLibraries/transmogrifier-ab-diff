import pytest
from click.testing import CliRunner


@pytest.fixture(autouse=True)
def _test_env(monkeypatch, tmp_path):
    monkeypatch.setenv("WORKSPACE", "test")
    monkeypatch.setenv("ROOT_WORKING_DIRECTORY", str(tmp_path / "output"))


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def job_directory(tmp_path):
    return str(tmp_path / "example-job-1")


@pytest.fixture
def example_job_directory():
    return "tests/fixtures/jobs/example-job-1"
