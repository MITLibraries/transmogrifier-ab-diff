import os
from unittest.mock import patch

from abdiff.core.build_ab_images import (
    build_ab_images,
    clone_repo_and_build_image,
    clone_repo_and_reset_to_commit,
    docker_image_exists,
)
from abdiff.core.utils import read_job_json


@patch("abdiff.core.build_ab_images.clone_repository")
def test_build_ab_images_success(mocked_clone, job_directory, mocked_docker_client):
    side_effect = os.makedirs(job_directory + "/clone")
    mocked_clone.side_effect = side_effect

    images = build_ab_images(
        job_directory,
        "abc123",
        "def456",
        mocked_docker_client,
    )
    assert images[0] == "transmogrifier-example-job-1-abc123:latest"
    assert images[1] == "transmogrifier-example-job-1-def456:latest"
    assert read_job_json(job_directory) == {
        "image_name_a": "transmogrifier-example-job-1-abc123:latest",
        "image_name_b": "transmogrifier-example-job-1-def456:latest",
    }


def test_docker_image_exists_returns_true(mocked_docker_client):
    assert (
        docker_image_exists(
            mocked_docker_client, "transmogrifier-example-job-1-def456:latest"
        )
        is True
    )


def test_docker_image_exists_returns_false(mocked_docker_client):
    assert (
        docker_image_exists(
            mocked_docker_client, "transmogrifier-example-job-1-abc123:latest"
        )
        is False
    )


@patch("abdiff.core.build_ab_images.clone_repository")
def test_clone_repo_and_build_image_success(
    mocked_clone, job_directory, mocked_docker_client
):
    image = clone_repo_and_build_image(
        job_directory,
        "abc123",
        mocked_docker_client,
    )
    assert image.tags[0] == "transmogrifier-example-job-1-abc123:latest"


@patch("abdiff.core.build_ab_images.clone_repository")
def test_clone_repo_and_reset_to_commit_success(mocked_clone, job_directory):
    clone_directory = job_directory + "/clone"
    assert not os.path.exists(clone_directory)
    side_effect = os.makedirs(clone_directory)
    mocked_clone.side_effect = side_effect

    clone_repo_and_reset_to_commit(clone_directory, "abc123")
    assert os.path.exists(clone_directory)
