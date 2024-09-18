"""abdiff.core.utils"""

import json
import os
from pathlib import Path

from abdiff.config import Config

CONFIG = Config()


def read_job_json(job_directory: str) -> dict:
    """Read job JSON file."""
    with open(Path(job_directory) / "job.json") as f:
        return json.load(f)


def read_run_json(run_directory: str) -> dict:
    """Read run JSON file."""
    with open(Path(run_directory) / "run.json") as f:
        return json.load(f)


def update_or_create_json(
    directory: str | Path,
    filename: str,
    new_data: dict,
) -> dict:
    filepath = Path(directory) / filename
    data = {}

    if os.path.exists(filepath):
        with open(filepath) as f:
            data = json.load(f)
    data.update(new_data)

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    return data


def update_or_create_job_json(job_directory: str, new_data: dict) -> dict:
    """Create or update a job's JSON file."""
    return update_or_create_json(job_directory, "job.json", new_data)


def update_or_create_run_json(run_directory: str, new_data: dict) -> dict:
    """Create or update a run's JSON file."""
    return update_or_create_json(run_directory, "run.json", new_data)


def create_subdirectories(
    base_directory: str, subdirectories: list[str]
) -> tuple[str, ...]:
    """Create subdirectories nested within a base directory.

    This util is preferred for commands that require creating
    nested subdirectories to organize outputs (e.g., run_ab_transforms).

    Returns:
        tuple[str, ...]: A tuple of absolute paths to created subdirectories.
    """
    directories = []
    for subdirectory in subdirectories:
        directory = Path(base_directory) / subdirectory
        os.makedirs(directory)
        directories.append(str(directory))
    return tuple(directories)
