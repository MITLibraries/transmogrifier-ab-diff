"""abdiff.core.utils"""

import json
import os
from pathlib import Path

from slugify import slugify

from abdiff.config import Config

CONFIG = Config()


def get_job_slug_and_working_directory(job_name: str) -> tuple[str, Path]:
    """Create working directory for new job by slugifying job name."""
    job_slug = slugify(job_name)
    return job_slug, Path(CONFIG.data_directory) / job_slug


def update_or_create_job_json(job_name: str, new_job_data: dict) -> dict:
    """Create or update a job's JSON file.

    This is helpful as a utility method, as multiple steps in the process may update the
    Job JSON file, with this as a standard interface.
    """
    job_slug, working_directory = get_job_slug_and_working_directory(job_name)
    job_json_filepath = working_directory / "job.json"

    job_data = {}
    if os.path.exists(job_json_filepath):
        with open(job_json_filepath) as f:
            job_data = json.load(f)
    job_data.update(new_job_data)

    with open(job_json_filepath, "w") as f:
        json.dump(job_data, f, indent=2)

    return job_data
