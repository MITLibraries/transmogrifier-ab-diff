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
    assert calc_record_diff(json.dumps(a).encode(), json.dumps(b).encode()) == json.dumps(
        {"color": ["green", "red"]}
    )


def test_calc_record_diff_no_diff():
    a = {"color": "green"}
    b = a
    assert calc_record_diff(json.dumps(a).encode(), json.dumps(b).encode()) == json.dumps(
        {}  # no diff
    )


def test_calc_record_diff_one_input_is_none():
    a = {"color": "green"}
    assert calc_record_diff(json.dumps(a).encode(), None) is None


def test_diffed_batches_yields_pyarrow_record_batch(collated_dataset):
    batch_iter = get_diffed_batches_iter(collated_dataset, batch_size=1)
    batch = next(batch_iter)
    assert isinstance(batch, pa.RecordBatch)


def test_diffed_batches_first_batch_has_diff(collated_dataset):
    batch_iter = get_diffed_batches_iter(collated_dataset, batch_size=1)
    batch_one = next(batch_iter).to_pandas()
    row = batch_one.iloc[0]

    assert row.ab_diff == json.dumps({"color": ["green", "red"]})
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
    assert diffs_dataset.count_rows() == 2
    assert diffs_dataset.schema == pa.schema(
        (
            pa.field("timdex_record_id", pa.string()),
            pa.field("source", pa.string()),
            pa.field("record_a", pa.binary()),
            pa.field("record_b", pa.binary()),
            pa.field("ab_diff", pa.string()),
            pa.field("has_diff", pa.string()),
        )
    )
