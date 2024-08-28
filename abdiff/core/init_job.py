"""abdiff.core.init_job"""

import logging
import os

from abdiff.config import Config
from abdiff.core.utils import update_or_create_job_json

CONFIG = Config()

logger = logging.getLogger(__name__)


def init_job(
    job_directory: str,
    message: str | None = None,
) -> str:
    """Function to initialize a new Job."""
    os.makedirs(job_directory)
    logger.info(f"Job working directory created: {job_directory}")

    job_data = {"job_directory": job_directory, "message": message}
    update_or_create_job_json(job_directory, job_data)

    return job_directory
