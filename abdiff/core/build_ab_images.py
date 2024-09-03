import logging
import os
import shutil

import docker
from pygit2 import clone_repository
from pygit2.enums import ResetMode

from abdiff.core.utils import update_or_create_job_json

logger = logging.getLogger(__name__)


def build_ab_images(
    job_directory: str,
    docker_client: docker.client.DockerClient,
    commit_sha_a: str,
    commit_sha_b: str,
) -> tuple[str, str]:
    """Build Docker images based on 2 commit SHAs.

    Args:
        job_directory: The directory containing all files related to a job.
        docker_client: A configured Docker client.
        commit_sha_a: The SHA of the first commit for comparison.
        commit_sha_b: The SHA of the second commit for comparison.
    """
    images = []
    for commit_sha in [commit_sha_a, commit_sha_b]:
        clone_directory = f"{job_directory}/{commit_sha}"
        image_name = f"{job_directory.split("/")[-1]}-{commit_sha}"
        clone_repo_and_reset_to_commit(clone_directory, commit_sha)
        image, _ = docker_client.images.build(path=clone_directory, tag=image_name)
        images.append(image.tags[0])
        delete_repo_clone(clone_directory)
    images_update = {"image_name_a": images[0], "image_name_b": images[1]}
    update_or_create_job_json(job_directory, images_update)
    return (images[0], images[1])


def clone_repo_and_reset_to_commit(clone_directory: str, commit_sha: str) -> None:
    """Clone GitHub repo and reset to a specified commit.

    Args:
        clone_directory: The directory for the cloned repo.
        commit_sha: The SHA of a repo commit.
    """
    logger.debug(f"Cloning repo to: {clone_directory}")
    repository = clone_repository(
        "https://github.com/MITLibraries/transmogrifier.git",
        clone_directory,
    )
    logger.debug(f"Cloned repo to: {clone_directory}")
    repository.reset(commit_sha, ResetMode.HARD)
    logger.debug(f"Cloned repo reset to commit: {commit_sha}")


def delete_repo_clone(clone_directory: str) -> None:
    """Remove cloned repository.

    Args:
        clone_directory: The directory for the cloned repo.
    """
    logger.debug(f"Removing repo clone: {clone_directory}")
    if os.path.exists(clone_directory):
        shutil.rmtree(clone_directory)
    logger.debug(f"Removed repo clone: {clone_directory}")
