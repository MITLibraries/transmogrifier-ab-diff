# ruff: noqa: PLR2004

import json
from itertools import islice
from pathlib import Path

import pyarrow as pa

from abdiff.core.calc_ab_diffs import (
    calc_ab_diffs,
    calc_record_diff,
    get_diffed_batches_iter,
)
from abdiff.core.utils import load_dataset


def test_calc_record_diff_has_diff():
    a = {"color": "green"}
    b = {"color": "red"}
    ab_diff, modified_timdex_fields, has_diff = calc_record_diff(a, b)
    assert ab_diff == json.dumps(
        {"values_changed": {"root['color']": {"new_value": "red", "old_value": "green"}}}
    )
    assert modified_timdex_fields == {"color"}
    assert has_diff


def test_calc_record_diff_no_diff():
    a = {"color": "green"}
    b = a
    ab_diff, modified_timdex_fields, has_diff = calc_record_diff(a, b)
    assert ab_diff == json.dumps({})  # no diff
    assert not modified_timdex_fields
    assert not has_diff


def test_calc_record_diff_one_input_is_none():
    a = {"color": "green"}
    assert calc_record_diff(a, None) == (None, None, False)


def test_calc_record_diff_array_by_default_order_not_a_diff():
    """Arrays with the same values, but differently ordered, not considered a diff."""
    a = {"colors": ["green", "red"]}
    b = {"colors": ["red", "green"]}
    ab_diff, modified_timdex_fields, has_diff = calc_record_diff(a, b)
    assert ab_diff == json.dumps({})  # no diff
    assert not modified_timdex_fields
    assert not has_diff


def test_calc_record_diff_array_set_flag_order_is_a_diff():
    """Arrays with the same values, but differently ordered, not considered a diff."""
    a = {"colors": ["green", "red"]}
    b = {"colors": ["red", "green"]}
    _, _, has_diff = calc_record_diff(a, b, ignore_order=False)
    assert has_diff


def test_calc_record_diff_array_repetition_is_reported_when_diff():
    """Same array values, but different in repetition, is considered a diff."""
    a = {"colors": ["red", "green"]}
    b = {"colors": ["red", "green", "green"]}
    ab_diff, modified_timdex_fields, has_diff = calc_record_diff(a, b)
    assert ab_diff == json.dumps(
        {
            "repetition_change": {
                "root['colors'][1]": {
                    "old_repeat": 1,
                    "new_repeat": 2,
                    "old_indexes": [1],
                    "new_indexes": [1, 2],
                    "value": "green",
                }
            }
        }
    )
    assert modified_timdex_fields == {"colors"}
    assert has_diff


def test_diffed_batches_yields_pyarrow_record_batch(collated_dataset):
    batch_iter = get_diffed_batches_iter(collated_dataset, batch_size=1)
    batch = next(batch_iter)
    assert isinstance(batch, pa.RecordBatch)


def test_diffed_batches_first_batch_has_diff(collated_dataset):
    batch_iter = get_diffed_batches_iter(collated_dataset, batch_size=1)
    batch_one = next(batch_iter).to_pandas()
    row = batch_one.iloc[0]

    assert row.ab_diff == json.dumps(
        {"values_changed": {"root['color']": {"new_value": "red", "old_value": "green"}}}
    )
    assert row.has_diff


def test_diffed_batches_second_batch_has_no_diff(collated_dataset):
    batch_iter = get_diffed_batches_iter(collated_dataset, batch_size=1)
    batch_two = next(islice(batch_iter, 1, None)).to_pandas()
    row = batch_two.iloc[0]

    assert row.ab_diff == json.dumps({})
    assert not row.has_diff


def test_calc_ab_diffs_writes_dataset(caplog, run_directory, collated_dataset_directory):
    caplog.set_level("INFO")
    calc_ab_diffs(run_directory, collated_dataset_directory)
    diffs_dataset = load_dataset(Path(run_directory) / "diffs")

    assert "wrote 2 parquet files to diffs dataset" in caplog.text
    assert diffs_dataset.count_rows() == 3
    assert diffs_dataset.schema == pa.schema(
        (
            pa.field("timdex_record_id", pa.string()),
            pa.field("source", pa.string()),
            pa.field("record_a", pa.binary()),
            pa.field("record_b", pa.binary()),
            pa.field("ab_diff", pa.string()),
            pa.field("modified_timdex_fields", pa.list_(pa.string())),
            pa.field("has_diff", pa.string()),
        )
    )
