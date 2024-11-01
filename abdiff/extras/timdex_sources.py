"""abdiff.helpers.timdex_sources"""

import datetime
import logging
import re

import boto3  # type: ignore[import-untyped]

from abdiff.config import Config

logger = logging.getLogger(__name__)

CONFIG = Config()


def get_ordered_extracted_files_all_sources(
    sources: list[str] | None = None,
) -> dict[str, list[str]]:
    """Get ordered extract files for all TIMDEX sources."""
    if not sources:
        sources = CONFIG.active_timdex_sources
    return {
        source: get_ordered_extracted_files_since_last_full_run(source=source)
        for source in sources
    }


def get_ordered_extracted_files_since_last_full_run(source: str) -> list[str]:
    """Get extract files, from last full run, through all daily runs, for a source."""
    logger.info(f"Retrieving ordered extracted files for source: '{source}'")
    all_files = get_extracted_files_for_source(source)

    # Find all full extract files and extract their dates
    full_extracts = [f for f in all_files if _is_full_extract(f)]
    if not full_extracts:
        logger.warning("No full extracts found.")
        return []

    # Extract dates from full extract files and find the most recent date
    full_extract_dates = [_extract_date(f) for f in full_extracts]
    most_recent_full_date = max(full_extract_dates)

    # Collect all full extract files with the most recent date
    most_recent_full_files = sorted(
        f for f in full_extracts if _extract_date(f) == most_recent_full_date
    )

    # Collect all daily extracts from the cutoff date onwards
    recent_daily_extracts = sorted(
        f
        for f in all_files
        if _is_daily_extract(f) and _extract_date(f) >= most_recent_full_date
    )

    # Combine full extracts and daily extracts
    ordered_files = most_recent_full_files + recent_daily_extracts
    logger.info(f"Total files retrieved: {len(ordered_files)}")
    return ordered_files


def _is_full_extract(filename: str) -> bool:
    return "-full-" in filename


def _is_daily_extract(filename: str) -> bool:
    return "-daily-" in filename


def _extract_date(filename: str) -> datetime.datetime:
    date_string = re.findall(r".+?(\d{4}-\d{2}-\d{2})", filename)[0]
    return datetime.datetime.strptime(date_string, "%Y-%m-%d").astimezone(datetime.UTC)


def get_extracted_files_for_source(
    source: str,
    bucket: str = CONFIG.TIMDEX_BUCKET,
) -> list[str]:
    """List S3 URIs for extract files in TIMDEX S3 bucket for a given source."""
    s3_client = boto3.client("s3")
    files = []

    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=source)

    for page in page_iterator:
        if "Contents" in page:
            for obj in page["Contents"]:
                if not obj["Key"].endswith("/"):  # skip folders
                    s3_uri = f"s3://{bucket}/{obj['Key']}"
                    files.append(s3_uri)

    # filter where "extracted" in filename
    return [file for file in files if "extracted" in file]
