# ruff: noqa: D205, D209, PD901, PLR2004

import json
import os
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from abdiff.core.collate_ab_transforms import (
    COLLATED_DATASET_SCHEMA,
    READ_BATCH_SIZE,
    TRANSFORMED_DATASET_SCHEMA,
    collate_ab_transforms,
    get_deduped_batches_iter,
    get_joined_batches_iter,
    get_transform_version,
    get_transformed_batches_iter,
    get_transformed_records_iter,
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
    assert set(collated_df["source"].unique()) == {"alma", "dspace"}

    # assert result of full outer join
    missing_in_b = collated_df[collated_df["record_b"].isna()]
    assert len(missing_in_b) == 1
    assert missing_in_b["source"].to_list() == ["alma"]


def test_get_transformed_records_iter_success(example_transformed_directory):
    """Validates the structure of the yielded TIMDEX record dictionaries."""
    records_iter = get_transformed_records_iter(
        transformed_file=str(
            Path(example_transformed_directory)
            / "a/alma-2024-08-29-daily-transformed-records-to-index.json"
        )
    )
    timdex_record_dict = next(records_iter)

    assert set(timdex_record_dict.keys()) == {
        "timdex_record_id",
        "source",
        "run_date",
        "run_type",
        "action",
        "record",
        "version",
        "transformed_file_name",
    }
    assert isinstance(timdex_record_dict["record"], bytes)
    assert timdex_record_dict["version"] == "a"
    assert (
        timdex_record_dict["transformed_file_name"]
        == "alma-2024-08-29-daily-transformed-records-to-index.json"
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
    assert set(transformed_batch.schema.names) == set(TRANSFORMED_DATASET_SCHEMA.names)


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
    assert joined_batch.schema.names == COLLATED_DATASET_SCHEMA.names


def test_get_deduped_batches_iter_success(collated_with_dupe_dataset_directory):
    deduped_batches_iter = get_deduped_batches_iter(collated_with_dupe_dataset_directory)
    deduped_df = next(deduped_batches_iter).to_pandas()

    # assert record 'def456' was dropped because most recent is action=delete
    assert len(deduped_df) == 2
    assert set(deduped_df.timdex_record_id) == {"abc123", "ghi789"}

    # assert record 'ghi789' has most recent 2024-10-02 version
    deduped_record = deduped_df.set_index("timdex_record_id").loc["ghi789"]
    assert json.loads(deduped_record.record_a)["material"] == "stucco"


def test_validate_output_success(collated_dataset_directory):
    validate_output(dataset_path=collated_dataset_directory)


def test_validate_output_raises_error_if_dataset_is_empty(run_directory):
    empty_table = pa.Table.from_batches(batches=[], schema=COLLATED_DATASET_SCHEMA)
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
        schema=COLLATED_DATASET_SCHEMA,
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


def test_validate_output_raises_error_if_duplicate_records(
    collated_with_dupe_dataset_directory,
):
    with pytest.raises(
        OutputValidationError,
        match="The collated dataset contains duplicate 'timdex_record_id' records.",
    ):
        validate_output(dataset_path=collated_with_dupe_dataset_directory)


def test_get_transform_version_success(transformed_directories, output_filename):
    transformed_directory_a, transformed_directory_b = transformed_directories
    transformed_file_a = str(Path(transformed_directory_a) / output_filename)
    transformed_file_b = str(Path(transformed_directory_b) / output_filename)

    assert get_transform_version(transformed_file_a) == "a"
    assert get_transform_version(transformed_file_b) == "b"


def test_get_transform_version_raise_error():
    with pytest.raises(ValueError, match="Transformed filepath is invalid."):
        get_transform_version("invalid")


@pytest.mark.parametrize(
    ("timdex_record_id", "action", "record_a_type", "record_b_type"),
    [
        ("libguides:1", "index", type(None), bytes),  # missing from A
        ("libguides:3", "index", bytes, type(None)),  # missing from B
        ("libguides:99", "delete", type(None), type(None)),  # missing from A
        ("libguides:4", "delete", type(None), type(None)),  # missing from B
    ],
)
def test_joining_dataset_handles_missing_records_success(
    collating_intermediate_transformed_dataset,
    timdex_record_id,
    action,
    record_a_type,
    record_b_type,
):
    """This test asserts that, for transformed or delete files, if a timdex_record_id
    exists in A or B, but is missing from the other, a value of 'None' is correctly
    found after the join."""
    batches_iter = get_joined_batches_iter(collating_intermediate_transformed_dataset)

    while True:
        df = next(batches_iter).to_pandas()
        if timdex_record_id in list(df.timdex_record_id):
            break

    row = df.set_index("timdex_record_id").loc[timdex_record_id]

    if record_a_type is type(None):
        assert row.record_a is None
    else:
        assert isinstance(row.record_a, record_a_type)
    if record_b_type is type(None):
        assert row.record_b is None
    else:
        assert isinstance(row.record_b, record_b_type)
