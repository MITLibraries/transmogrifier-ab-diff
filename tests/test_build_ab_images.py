import os
from unittest.mock import patch

import pytest

from abdiff.core.build_ab_images import (
    build_ab_images,
    build_image,
    clone_repo_and_reset_to_commit,
    docker_image_exists,
)
from abdiff.core.exceptions import InvalidRepositoryCommitSHAError
from abdiff.core.utils import read_job_json


@patch("abdiff.core.build_ab_images.clone_repository")
def test_build_ab_images_success(
    mocked_clone, job_directory, mocked_docker_client, default_transmogrifier_location
):
    side_effect = os.makedirs(job_directory + "/clone")
    mocked_clone.side_effect = side_effect

    images = build_ab_images(
        job_directory,
        default_transmogrifier_location,
        "abc123",
        default_transmogrifier_location,
        "def456",
        mocked_docker_client,
    )
    assert images[0] == "transmogrifier-example-job-1-abc123:latest"
    assert images[1] == "transmogrifier-example-job-1-def456:latest"
    assert read_job_json(job_directory) == {
        "image_tag_a": "transmogrifier-example-job-1-abc123:latest",
        "image_tag_b": "transmogrifier-example-job-1-def456:latest",
    }


@patch("abdiff.core.build_ab_images.clone_repository")
def test_build_ab_images_invalid_commit_sha_raise_error(
    mocked_clone,
    job_directory,
    mocked_docker_client,
    caplog,
    default_transmogrifier_location,
):
    caplog.set_level("DEBUG")
    side_effect = os.makedirs(job_directory + "/clone")
    mocked_clone.side_effect = side_effect
    mocked_clone.return_value.reset.side_effect = KeyError

    with pytest.raises(InvalidRepositoryCommitSHAError):
        build_ab_images(
            job_directory,
            default_transmogrifier_location,
            "invalid",
            default_transmogrifier_location,
            "def456",
            mocked_docker_client,
        )


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
def test_build_image_success(
    mocked_clone, mocked_docker_client, default_transmogrifier_location
):
    image = build_image(
        default_transmogrifier_location,
        "abc123",
        mocked_docker_client,
    )
    assert image.tags[0] == "transmogrifier-example-job-1-abc123:latest"


@patch("abdiff.core.build_ab_images.clone_repository")
def test_clone_repo_and_reset_to_commit_success(
    mocked_clone, job_directory, default_transmogrifier_location
):
    clone_directory = job_directory + "/clone"
    assert not os.path.exists(clone_directory)
    side_effect = os.makedirs(clone_directory)
    mocked_clone.side_effect = side_effect

    clone_repo_and_reset_to_commit(
        default_transmogrifier_location, clone_directory, "abc123"
    )
    assert os.path.exists(clone_directory)
