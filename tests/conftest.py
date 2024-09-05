from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from abdiff.core import init_job


@pytest.fixture(autouse=True)
def _test_env(monkeypatch, tmp_path):
    monkeypatch.setenv("WORKSPACE", "test")


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
def job(job_directory):
    return init_job(job_directory)


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
