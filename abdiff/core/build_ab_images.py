import logging
import tempfile

import docker
import docker.models
import docker.models.images
from pygit2 import clone_repository
from pygit2.enums import ResetMode

from abdiff.core.exceptions import InvalidRepositoryCommitSHAError
from abdiff.core.utils import update_or_create_job_json

logger = logging.getLogger(__name__)


def build_ab_images(
    job_directory: str,
    location_a: str,
    commit_sha_a: str,
    location_b: str,
    commit_sha_b: str,
    docker_client: docker.client.DockerClient | None = None,
) -> tuple[str, str]:
    """Build Docker images based on 2 commit SHAs.

    Args:
        job_directory: The directory containing all files related to a job.
        location_a: Location of Transmogrifier version 'A' to clone from.
        commit_sha_a: The SHA of the first commit for comparison.
        location_b: Location of Transmogrifier version 'B' to clone from.
        commit_sha_b: The SHA of the second commit for comparison.
        docker_client: A configured Docker client.
    """
    if not docker_client:
        docker_client = docker.from_env()

    image_tags = []
    for location, commit_sha in [(location_a, commit_sha_a), (location_b, commit_sha_b)]:
        logger.debug(f"Processing commit: {commit_sha}")
        image_tag = generate_image_name(commit_sha)
        if docker_image_exists(docker_client, image_tag):
            logger.debug(f"Docker image already exists with tag: {image_tag}")
            image_tags.append(image_tag)
        else:
            image = build_image(location, commit_sha, docker_client)
            image_tags.append(image.tags[0])
        logger.debug(f"Finished processing commit: {commit_sha}")

    images_data = {"image_tag_a": image_tags[0], "image_tag_b": image_tags[1]}
    update_or_create_job_json(job_directory, images_data)
    return (image_tags[0], image_tags[1])


def generate_image_name(commit_sha: str) -> str:
    """Standardize docker image naming via this function."""
    return f"transmogrifier-abdiff-{commit_sha}:latest"


def docker_image_exists(
    docker_client: docker.client.DockerClient, image_tag: str
) -> bool:
    """Check if Docker image already exists with a certain name.

    Args:
        docker_client: A configured Docker client.
        image_tag: The tag of the Docker image to be created.
    """
    return image_tag in [
        image_tag for image in docker_client.images.list() for image_tag in image.tags
    ]


def build_image(
    location: str,
    commit_sha: str,
    docker_client: docker.client.DockerClient,
) -> docker.models.images.Image:
    """Clone repo and build Docker image.

    Args:
        location: Location of Transmogrifier to clone from.
        commit_sha: The SHA of the commit.
        docker_client: A configured Docker client.
    """
    with tempfile.TemporaryDirectory() as clone_directory:
        image_tag = generate_image_name(commit_sha)
        clone_repo_and_reset_to_commit(location, clone_directory, commit_sha)
        image, _ = docker_client.images.build(path=clone_directory, tag=image_tag)
        logger.debug(f"Docker image created with tag: {image}")
        return image


def clone_repo_and_reset_to_commit(
    location: str,
    clone_directory: str,
    commit_sha: str,
) -> None:
    """Clone GitHub repo and reset to a specified commit.

    Args:
        location: Location of Transmogrifier to clone from.
        clone_directory: The directory for the cloned repo.
        commit_sha: The SHA of a repo commit.
    """
    logger.debug(f"Cloning repo from: {location}, to: {clone_directory}")
    repository = clone_repository(
        location,
        clone_directory,
    )
    try:
        repository.reset(commit_sha, ResetMode.HARD)
        logger.debug(f"Cloned repo reset to commit: {commit_sha}")
    except KeyError as exception:
        raise InvalidRepositoryCommitSHAError(location, commit_sha) from exception
