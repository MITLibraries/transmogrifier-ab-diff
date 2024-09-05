import logging
import tempfile

import docker
import docker.models
import docker.models.images
from pygit2 import clone_repository
from pygit2.enums import ResetMode

from abdiff.core.utils import update_or_create_job_json

logger = logging.getLogger(__name__)


def build_ab_images(
    job_directory: str,
    commit_sha_a: str,
    commit_sha_b: str,
    docker_client: docker.client.DockerClient | None = None,
) -> tuple[str, str]:
    """Build Docker images based on 2 commit SHAs.

    Args:
        job_directory: The directory containing all files related to a job.
        commit_sha_a: The SHA of the first commit for comparison.
        commit_sha_b: The SHA of the second commit for comparison.
        docker_client: A configured Docker client.
    """
    if not docker_client:
        docker_client = docker.from_env()
    image_names = []
    for commit_sha in [commit_sha_a, commit_sha_b]:
        logger.debug(f"Processing commit: {commit_sha}")
        image_name = f"transmogrifier-{job_directory.split("/")[-1]}-{commit_sha}:latest"
        if docker_image_exists(docker_client, image_name):
            image_names.append(image_name)
        else:
            image = clone_repo_and_build_image(job_directory, commit_sha, docker_client)
            image_names.append(image.tags[0])
        logger.debug(f"Finished processing commit: {commit_sha}")
    images_data = {"image_name_a": image_names[0], "image_name_b": image_names[1]}
    update_or_create_job_json(job_directory, images_data)
    return (image_names[0], image_names[1])


def docker_image_exists(
    docker_client: docker.client.DockerClient, image_name: str
) -> bool:
    """Check if Docker image already exists with a certain name.

    Args:
        docker_client: A configured Docker client.
        image_name: The name of the Docker image to be created.
    """
    return image_name in docker_client.images.list()


def clone_repo_and_build_image(
    job_directory: str,
    commit_sha: str,
    docker_client: docker.client.DockerClient,
) -> docker.models.images.Image:
    """Clone repo and build Docker image.

    Args:
        job_directory: The directory containing all files related to a job.
        commit_sha: The SHA of the commit.
        docker_client: A configured Docker client.
    """
    with tempfile.TemporaryDirectory() as clone_directory:
        image_name = f"transmogrifier-{job_directory.split("/")[-1]}-{commit_sha}"
        clone_repo_and_reset_to_commit(clone_directory, commit_sha)
        image, _ = docker_client.images.build(path=clone_directory, tag=image_name)
        logger.debug(f"Docker image created with tag: {image}")
        return image


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
