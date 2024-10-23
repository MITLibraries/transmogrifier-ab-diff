# ruff: noqa: PLR2004
import os
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from abdiff.core.collate_ab_transforms import (
    JOINED_DATASET_SCHEMA,
    READ_BATCH_SIZE,
    TRANSFORMED_DATASET_SCHEMA,
    collate_ab_transforms,
    get_joined_batches_iter,
    get_transformed_batches_iter,
    get_transformed_records_iter,
    parse_parquet_details_from_transformed_file,
    validate_output,
)
from abdiff.core.exceptions import OutputValidationError
from abdiff.core.utils import write_to_dataset


def test_collate_ab_transforms_success(
    example_run_directory, example_ab_transformed_file_lists
):
    """Validates the output of collate_ab_transforms.

    This test performs assertions on the returned output of the function
    and the resulting collated parquet dataset.

    For purposes of testing, the function is called on two (2) transformed
    JSON files (used to create the 'transformed_parquet_dataset' fixture).
    These JSON files were manually edited such that:
        * The example 'DSpace@MIT' transformed JSON file only contains
          five (5) records.
        * The example 'MIT Alma' transformed JSON file contains five records
          in version A but only four (4) records in version B. The reason for
          omitting one record is to verify that the full outer join is working
          as expected, as evidenced by the final parquet dataset containing
          10 records.
    """
    collated_dataset_path = collate_ab_transforms(
        run_directory=example_run_directory,
        ab_transformed_file_lists=example_ab_transformed_file_lists,
    )
    assert collated_dataset_path == str(Path(example_run_directory) / "collated")

    collated_dataset = ds.dataset(collated_dataset_path, format="parquet")
    collated_df = collated_dataset.to_table().to_pandas()

    assert collated_dataset.files == [
        str(Path(collated_dataset_path) / "records-0.parquet")
    ]
    assert len(collated_df) == 10
    assert set(collated_df["source"].unique()) == {"MIT Alma", "DSpace@MIT"}

    # assert result of full outer join
    missing_in_b = collated_df[collated_df["record_b"].isna()]
    assert len(missing_in_b) == 1
    assert missing_in_b["source"].to_list() == ["MIT Alma"]


def test_get_transformed_records_iter_success(example_transformed_directory):
    """Validates the structure of the yielded TIMDEX record dictionaries."""
    records_iter = get_transformed_records_iter(
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
    example_run_directory, example_ab_transformed_file_lists
):
    transformed_batches_iter = get_transformed_batches_iter(
        run_directory=example_run_directory,
        ab_transformed_file_lists=example_ab_transformed_file_lists,
    )
    transformed_batch = next(transformed_batches_iter)

    assert isinstance(transformed_batch, pa.RecordBatch)
    assert transformed_batch.num_rows <= READ_BATCH_SIZE
    assert transformed_batch.schema == TRANSFORMED_DATASET_SCHEMA


def test_get_joined_batches_iter_success(transformed_parquet_dataset):
    """Validates the structure of the joined A/B TIMDEX record dictionaries.

    The function yields pyarrow.RecordBatch objects per transformed file.
    Given that the fixture 'transformed_parquet_dataset' collates
    two (2) transformed JSON files, this test asserts that two
    batches are yielded by the function. This test also performs
    assertions on the structure of a single pyarrow.RecordBatch.
    """
    joined_batches_iter = get_joined_batches_iter(transformed_parquet_dataset)
    joined_batches = list(joined_batches_iter)
    max_rows_per_file = 100_000

    assert len(joined_batches) == 2

    joined_batch = joined_batches[0]
    assert isinstance(joined_batch, pa.RecordBatch)
    assert joined_batch.num_rows <= max_rows_per_file
    assert joined_batch.schema == JOINED_DATASET_SCHEMA


def test_validate_output_success(collated_dataset_directory):
    validate_output(dataset_path=collated_dataset_directory)


def test_validate_output_raises_error_if_dataset_is_empty(run_directory):
    empty_table = pa.Table.from_batches(batches=[], schema=JOINED_DATASET_SCHEMA)
    empty_dataset_path = Path(run_directory) / "empty_dataset"

    os.makedirs(empty_dataset_path)
    pq.write_table(empty_table, empty_dataset_path / "empty.parquet")
    write_to_dataset(empty_table, base_dir=empty_dataset_path)

    with pytest.raises(
        OutputValidationError, match="The collated dataset does not contain any records."
    ):
        validate_output(dataset_path=empty_dataset_path)


def test_validate_output_raises_error_if_missing_record_column(run_directory):
    missing_record_cols_table = pa.Table.from_pylist(
        [
            {
                "timdex_record_id": "abc",
                "source": "source",
                "record_a": b"{timdex_record_id: 'abc'}",
                "record_b": None,
            }
        ],
        schema=JOINED_DATASET_SCHEMA,
    )
    missing_record_cols_dataset_path = Path(run_directory) / "missing_record_cols_dataset"

    os.makedirs(missing_record_cols_dataset_path)
    pq.write_table(
        missing_record_cols_table,
        missing_record_cols_dataset_path / "missing_record_cols.parquet",
    )
    write_to_dataset(missing_record_cols_table, base_dir=missing_record_cols_dataset_path)

    with pytest.raises(
        OutputValidationError,
        match=re.escape(
            "At least one or both record column(s) ['record_a', 'record_b'] in the collated dataset are empty."  # noqa: E501
        ),
    ):
        validate_output(dataset_path=missing_record_cols_dataset_path)


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
