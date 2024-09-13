# ruff: noqa: PLR2004

from pathlib import Path

import pyarrow as pa
import pytest

from abdiff.core.collate_ab_transforms import (
    JOINED_DATASET_SCHEMA,
    READ_BATCH_SIZE,
    TRANSFORMED_DATASET_SCHEMA,
    collate_ab_transforms,
    get_joined_batches_iter,
    get_transformed_batches_iter,
    parse_parquet_details_from_transformed_file,
    yield_records,
)


def test_collate_ab_transforms_success(example_run_directory, example_transformed_files):
    collated_dataset_path = collate_ab_transforms(
        run_directory=example_run_directory, transformed_files=example_transformed_files
    )
    assert collated_dataset_path == str(Path(example_run_directory) / "collated")


def test_yield_records_success(example_transformed_directory):
    """Validates the structure of the yielded TIMDEX record dictionaries."""
    records_iter = yield_records(
        transformed_file=Path(example_transformed_directory)
        / "a/alma-2024-08-29-daily-transformed-records-to-index.json"
    )
    timdex_record_dict = next(records_iter)

    assert list(timdex_record_dict.keys()) == [
        "timdex_record_id",
        "source",
        "record",
        "version",
        "transformed_file_name",
    ]
    assert isinstance(timdex_record_dict["record"], bytes)
    assert timdex_record_dict["version"] == "a"
    assert (
        timdex_record_dict["transformed_file_name"]
        == "alma-2024-08-29-daily-transformed-records-to-index"
    )


def test_get_transformed_batches_iter_success(
    example_run_directory, example_transformed_files
):
    transformed_batches_iter = get_transformed_batches_iter(
        run_directory=example_run_directory, transformed_files=example_transformed_files
    )
    transformed_batch = next(transformed_batches_iter)

    assert isinstance(transformed_batch, pa.RecordBatch)
    assert transformed_batch.num_rows <= READ_BATCH_SIZE
    assert transformed_batch.schema == TRANSFORMED_DATASET_SCHEMA


def test_get_joined_batches_iter_success(transformed_parquet_dataset):
    """Validates the structure of the joined A/B TIMDEX record dictionaries.

    For purposes of testing, the transformed JSON files used to create the
    'transformed_parquet_dataset' fixture were purposefully edited such that
    one (1) record of the five (5) original set of records is omitted in
    version B. To verify that the full outer join is working as expected,
    the single batch in joined_batches_iter must still contain five (5) records,
    where the unmatched record contains a bytestring for the 'record_a' column
    and is None for the 'record_b' column.
    """
    joined_batches_iter = get_joined_batches_iter(transformed_parquet_dataset)
    joined_batch = next(joined_batches_iter)
    max_rows_per_file = 100_000

    assert isinstance(joined_batch, pa.RecordBatch)
    assert joined_batch.num_rows <= max_rows_per_file
    assert joined_batch.schema == JOINED_DATASET_SCHEMA

    # assert result of full outer join
    missing_in_b = joined_batch.filter(pa.array(joined_batch["record_b"].is_null()))
    assert joined_batch.num_rows == 5
    assert missing_in_b.num_rows == 1


def test_parse_parquet_details_from_transformed_file_success(
    transformed_directories, output_filename
):
    transformed_directory_a, transformed_directory_b = transformed_directories
    transformed_file_a = str(Path(transformed_directory_a) / output_filename)
    transformed_file_b = str(Path(transformed_directory_b) / output_filename)
    transformed_filename = output_filename.replace(".json", "")  # remove file .ext

    assert parse_parquet_details_from_transformed_file(transformed_file_a) == (
        "a",
        transformed_filename,
    )
    assert parse_parquet_details_from_transformed_file(transformed_file_b) == (
        "b",
        transformed_filename,
    )


def test_parse_parquet_details_from_transformed_file_raise_error(output_filename):
    with pytest.raises(ValueError, match="Transformed filename is invalid"):
        parse_parquet_details_from_transformed_file(output_filename)
