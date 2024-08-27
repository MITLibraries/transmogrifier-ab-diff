"""abdiff.core.init_job"""

import logging
import os

from abdiff.config import Config
from abdiff.core.utils import (
    get_job_slug_and_working_directory,
    update_or_create_job_json,
)

CONFIG = Config()

logger = logging.getLogger(__name__)


def init_job(job_name: str) -> dict:
    """Function to initialize a new Job.

    1. create a working directory for job
    2. initialize a job.json file
    """
    job_slug, job_working_directory = get_job_slug_and_working_directory(job_name)
    os.makedirs(job_working_directory)
    logger.info(
        f"Job '{job_slug}' initialized.  Job working directory: {job_working_directory}"
    )

    job_data = {
        "job_name": job_name,
        "job_slug": job_slug,
        "working_directory": str(job_working_directory),
    }
    update_or_create_job_json(job_name, job_data)

    return job_data
