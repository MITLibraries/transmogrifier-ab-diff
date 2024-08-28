"""abdiff.core.utils"""

import json
import os
from pathlib import Path

from abdiff.config import Config

CONFIG = Config()


def read_job_json(job_directory: str) -> dict:
    """Read job JSON file."""
    job_json_filepath = Path(job_directory) / "job.json"
    with open(job_json_filepath) as f:
        return json.load(f)


def update_or_create_job_json(job_directory: str | Path, new_job_data: dict) -> dict:
    """Create or update a job's JSON file.

    This is helpful as a utility method, as multiple steps in the process may update the
    Job JSON file, with this as a standard interface.
    """
    job_json_filepath = Path(job_directory) / "job.json"

    job_data = {}
    if os.path.exists(job_json_filepath):
        with open(job_json_filepath) as f:
            job_data = json.load(f)
    job_data.update(new_job_data)

    with open(job_json_filepath, "w") as f:
        json.dump(job_data, f, indent=2)

    return job_data
