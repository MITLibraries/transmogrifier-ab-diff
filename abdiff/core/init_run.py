"""abdiff.core.init_run"""

import datetime
import logging
import os
from pathlib import Path

from abdiff.config import Config
from abdiff.core.utils import read_job_json, update_or_create_run_json

CONFIG = Config()

logger = logging.getLogger(__name__)


def init_run(
    job_directory: str,
    message: str | None = None,
) -> str:
    """Function to initialize a new Run as part of a parent Job."""
    run_timestamp = datetime.datetime.now(tz=datetime.UTC).strftime("%Y-%m-%d_%H-%M-%S")
    run_directory = str(Path(job_directory) / "runs" / run_timestamp)
    logs_directory = str(Path(run_directory) / "logs")
    os.makedirs(run_directory)
    os.makedirs(logs_directory)
    logger.info(f"Run directory created: {run_directory}")
    logger.info(f"Logs directory created: {logs_directory}")

    # clone job data and update with run information
    run_data = read_job_json(job_directory)
    run_data.update(
        {
            "run_directory": run_directory,
            "run_message": message,
            "run_timestamp": run_timestamp,
        }
    )
    update_or_create_run_json(run_directory, run_data)

    return run_directory
