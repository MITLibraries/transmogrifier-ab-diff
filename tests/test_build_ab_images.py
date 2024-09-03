import os
from unittest.mock import patch

from abdiff.core.build_ab_images import (
    build_ab_images,
    clone_repo_and_reset_to_commit,
    delete_repo_clone,
)
from abdiff.core.utils import read_job_json


@patch("abdiff.core.build_ab_images.clone_repository")
def test_build_ab_images_success(mocked_clone, job_directory, mocked_docker_client):
    side_effect = os.makedirs(job_directory + "/clone")
    mocked_clone.side_effect = side_effect

    images = build_ab_images(job_directory, mocked_docker_client, "abc123", "def456")
    assert images[0] == "987abc"
    assert images[1] == "654def"
    assert read_job_json(job_directory) == {
        "image_name_a": "987abc",
        "image_name_b": "654def",
    }


@patch("abdiff.core.build_ab_images.clone_repository")
def test_clone_repo_from_commit_success(mocked_clone, job_directory):
    clone_directory = job_directory + "/clone"
    assert not os.path.exists(clone_directory)
    side_effect = os.makedirs(clone_directory)
    mocked_clone.side_effect = side_effect

    clone_repo_and_reset_to_commit(clone_directory, "abc123")
    assert os.path.exists(clone_directory)


def test_delete_repo_clone_success(job_directory):
    clone_directory = job_directory + "/clone"
    os.makedirs(clone_directory)

    assert os.path.exists(clone_directory)
    delete_repo_clone(clone_directory)
    assert not os.path.exists(clone_directory)
