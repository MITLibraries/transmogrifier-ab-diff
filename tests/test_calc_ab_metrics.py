# ruff: noqa: PLR2004

import os.path

import pyarrow.dataset as ds

from abdiff.core.calc_ab_metrics import (
    _get_field_counts,
    _get_global_counts,
    _get_source_counts,
    _prepare_duckdb_context,
    calc_ab_metrics,
    calculate_metrics_data,
    create_record_diff_matrix_dataset,
)
from abdiff.core.utils import load_dataset, read_run_json


def test_sparse_matrix_dataset_created_success(run_directory, diffs_dataset_directory):
    diff_matrix_dataset_filepath = create_record_diff_matrix_dataset(
        run_directory, diffs_dataset_directory
    )
    assert os.path.exists(diff_matrix_dataset_filepath)
    diff_matrix_dataset = load_dataset(diff_matrix_dataset_filepath)
    assert isinstance(diff_matrix_dataset, ds.Dataset)


def test_sparse_matrix_has_expected_structure(diff_matrix_df):

    diff_matrix_df = diff_matrix_df.set_index("timdex_record_id")

    # assert matrix columns are driven by fields where at least 1 record has a diff for it
    assert set(diff_matrix_df.columns).issuperset(
        {
            "color",
            "fruit",
            "number",
        }
    )
    # assert that field "concrete" is not present as zero records had diff for this field
    assert "concrete" not in diff_matrix_df.columns

    # assert records have diff true/false as expected
    row = diff_matrix_df.loc["abc123"]
    assert row.color == 1
    assert row.number == 0
    assert row.fruit == 0

    row = diff_matrix_df.loc["def456"]
    assert row.color == 0
    assert row.number == 0
    assert row.fruit == 0

    row = diff_matrix_df.loc["ghi789"]
    assert row.color == 1
    assert row.number == 1
    assert row.fruit == 1


def test_duckdb_context_extracts_fields_and_sources(
    function_duckdb_connection, diff_matrix_dataset_filepath
):
    fields, sources = _prepare_duckdb_context(
        function_duckdb_connection, diff_matrix_dataset_filepath
    )
    assert set(fields) == {
        "color",
        "fruit",
        "number",
    }
    assert set(sources) == {"alma", "dspace", "libguides"}


def test_duckdb_context_creates_record_diff_matrix_view(
    function_duckdb_connection, diff_matrix_dataset_filepath
):
    _prepare_duckdb_context(function_duckdb_connection, diff_matrix_dataset_filepath)

    function_duckdb_connection.execute(
        """
    select * from record_diff_matrix
    order by timdex_record_id;
    """
    )
    record_diff_df = function_duckdb_connection.fetchdf()
    assert len(record_diff_df) == 3
    assert set(record_diff_df.columns) == {
        "timdex_record_id",
        "source",
        "has_diff",
        "color",
        "fruit",
        "number",
    }
    assert record_diff_df.iloc[0].to_dict() == {
        "timdex_record_id": "abc123",
        "source": "alma",
        "has_diff": 1.0,
        "color": 1.0,
        "fruit": 0.0,
        "number": 0.0,
    }


def test_global_counts_metrics(duckdb_context_with_diff_matrix):
    conn, fields, sources = duckdb_context_with_diff_matrix
    total_records, total_records_with_diff = _get_global_counts(conn)
    assert total_records == 3
    assert total_records_with_diff == 2


def test_source_counts_metrics(duckdb_context_with_diff_matrix):
    conn, fields, sources = duckdb_context_with_diff_matrix
    analysis: dict = {
        "by_source": {},
    }
    analysis = _get_source_counts(conn, fields, sources, analysis)
    assert analysis["by_source"] == {
        "alma": {
            "count": 1,
            "field_counts": {
                "color": 1,
                "number": 0,
                "fruit": 0,
            },
        },
        "libguides": {
            "count": 1,
            "field_counts": {
                "color": 1,
                "number": 1,
                "fruit": 1,
            },
        },
        "dspace": {
            "count": 0,
            "field_counts": {
                "color": 0,
                "number": 0,
                "fruit": 0,
            },
        },
    }


def test_fields_counts_metrics(duckdb_context_with_diff_matrix):
    conn, fields, sources = duckdb_context_with_diff_matrix
    analysis: dict = {
        "by_field": {},
    }
    analysis = _get_field_counts(conn, fields, sources, analysis)
    assert analysis["by_field"] == {
        "color": {"count": 2, "source_counts": {"alma": 1, "dspace": 0, "libguides": 1}},
        "fruit": {"count": 1, "source_counts": {"alma": 0, "dspace": 0, "libguides": 1}},
        "number": {"count": 1, "source_counts": {"alma": 0, "dspace": 0, "libguides": 1}},
    }


def test_full_metrics_data_has_expected_structure(diff_matrix_dataset_filepath):
    metrics = calculate_metrics_data(diff_matrix_dataset_filepath)
    assert set(metrics.keys()) == {"summary", "analysis"}
    assert set(metrics["analysis"].keys()) == {"by_source", "by_field"}


def test_core_function_updates_run_data(run_directory, diffs_dataset_directory):
    metrics_dataset_filepath = calc_ab_metrics(run_directory, diffs_dataset_directory)

    metrics_dataset = load_dataset(metrics_dataset_filepath)
    assert isinstance(metrics_dataset, ds.Dataset)

    run_data = read_run_json(run_directory)
    assert isinstance(run_data["metrics"], dict)
