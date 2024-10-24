"""abdiff.core.utils"""

import json
import os
import re
from collections.abc import Iterable
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds

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


def load_dataset(base_dir: str | Path) -> ds.Dataset:
    """Standardized way of reading of parquet datasets in application."""
    return ds.dataset(base_dir, partitioning="hive")


def parse_timdex_filename(s3_uri_or_filename: str) -> dict[str, str | None]:
    """Parse details from filename."""
    filename = s3_uri_or_filename.split("/")[-1]

    match_result = re.match(
        r"^([\w\-]+?)-(\d{4}-\d{2}-\d{2})-(\w+)-(\w+)-records-to-(.+?)(?:_(\d+))?\.(\w+)$",
        filename,
    )

    keys = ["source", "date", "cadence", "stage", "action", "index", "file_type"]
    if not match_result:
        raise ValueError(  # noqa: TRY003
            f"Provided S3 URI and filename is invalid: {filename}."
        )

    try:
        return dict(zip(keys, match_result.groups(), strict=True))
    except ValueError as exception:
        raise ValueError(  # noqa: TRY003
            f"Provided S3 URI and filename is invalid: {filename}."
        ) from exception


def write_to_dataset(
    data: (
        ds.Dataset
        | pa.Table
        | pa.RecordBatch
        | pa.RecordBatchReader
        | list[pa.Table]
        | Iterable[pa.RecordBatch]
    ),
    base_dir: str | Path,
    schema: pa.Schema | None = None,
    partition_columns: list[str] | None = None,
) -> list[ds.WrittenFile]:
    """Standardized way of writing to parquet datasets in application.

    This ensures that all datasets written by core functions are writing them with a
    similar structure and attention to row group and file size.

    If an iterable of RecordBatches is passed as the data to write, a schema object must
    also be provided, because a schema cannot be extracted or inferred from the lazily
    evaluated generator.  All other types passed do not explicitly require a schema,
    as it can be inferred.
    """
    written_files = []

    ds.write_dataset(
        data,
        schema=schema,
        base_dir=str(base_dir),
        partitioning=partition_columns,
        partitioning_flavor="hive",
        format="parquet",
        basename_template="records-{i}.parquet",
        existing_data_behavior="delete_matching",
        use_threads=True,
        max_rows_per_group=1_000,
        max_rows_per_file=100_000,
        file_visitor=lambda written_file: written_files.append(written_file),
    )

    return written_files  # type: ignore[return-value] # mypy incorrect here
