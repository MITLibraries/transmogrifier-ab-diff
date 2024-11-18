# ruff: noqa: PLR2004

import json
import os
import signal
from pathlib import Path
from unittest.mock import patch

import pytest

from abdiff.webapp.utils import query_duckdb_for_records_datatable


def test_webapp_without_job_directory_raise_error(monkeypatch, webapp_client):
    monkeypatch.delenv("JOB_DIRECTORY", raising=False)
    with pytest.raises(RuntimeError, match="A job directory is required for flask app."):
        webapp_client.get("/ping")


def test_ping_route_success(webapp_client):
    response = webapp_client.get("/ping")
    assert response.status_code == 200
    assert response.text == "pong"


def test_job_route_load_job_and_runs_data_success(
    webapp_job_directory, webapp_run_timestamp, webapp_client
):
    with patch("abdiff.webapp.app.render_template") as mock_render:
        _response = webapp_client.get("/")
    args, kwargs = mock_render.call_args

    assert json.loads(kwargs["job_json"])["job_directory"] == webapp_job_directory

    assert len(kwargs["runs"]) == 1
    assert webapp_run_timestamp in kwargs["runs"]
    assert kwargs["runs"][webapp_run_timestamp]["run_directory"] == str(
        Path(webapp_job_directory) / "runs" / webapp_run_timestamp
    )


def test_run_route_load_run_data_success(
    webapp_job_directory, webapp_run_timestamp, webapp_client
):
    with patch("abdiff.webapp.app.render_template") as mock_render:
        _response = webapp_client.get(f"/run/{webapp_run_timestamp}")
    args, kwargs = mock_render.call_args

    assert kwargs["run_data"]["run_timestamp"] == webapp_run_timestamp


def test_run_route_load_transformation_logs(
    webapp_job_directory, webapp_run_timestamp, webapp_client
):
    with patch("abdiff.webapp.app.render_template") as mock_render:
        _response = webapp_client.get(f"/run/{webapp_run_timestamp}")
    args, kwargs = mock_render.call_args

    assert set(kwargs["transform_logs"]) == {
        "alma-2024-08-29-daily-aaaaaa123456-logs.txt",
        "alma-2024-08-29-daily-bbbbbb123456-logs.txt",
        "dspace-2024-10-14-daily-cccccc123456-logs.txt",
        "dspace-2024-10-14-daily-dddddd123456-logs.txt",
    }


def test_shutdown_route_success(caplog, webapp_client):
    with patch("os.kill") as mock_pid_kill:
        _response = webapp_client.get("/shutdown")

    assert "Shutting down flask webapp..." in caplog.text
    mock_pid_kill.assert_called_once_with(os.getpid(), signal.SIGINT)


def test_record_page_get_a_b_records_passed_as_valid_json_strings(
    monkeypatch, caplog, webapp_client
):
    monkeypatch.setenv("JOB_DIRECTORY", "tests/fixtures/jobs/example-job-2")

    with patch("abdiff.webapp.app.render_template") as mock_render:
        _response = webapp_client.get(
            "/run/2024-10-17_14-01-18/record/dspace:1721.1-157217"
        )
    args, kwargs = mock_render.call_args

    a, b = kwargs["a_json"], kwargs["b_json"]
    assert isinstance(a, str)
    assert isinstance(b, str)
    assert isinstance(json.loads(a), dict)
    assert isinstance(json.loads(b), dict)


def test_datatables_duckdb_query_expected_structure(records_duckdb_filepath):
    data = query_duckdb_for_records_datatable(records_duckdb_filepath)
    assert set(data.keys()) == {"draw", "recordsTotal", "recordsFiltered", "data"}
    assert isinstance(data["data"], list)


@pytest.mark.parametrize(
    ("source_filter", "expected_count"),
    [
        (None, 1278),
        (["libguides"], 386),
        (["researchdatabases"], 892),
        (["libguides", "researchdatabases"], 1278),
        (["bad_source_name"], 0),
    ],
)
def test_datatables_duckdb_query_source_filtering(
    records_duckdb_filepath, source_filter, expected_count
):
    """Tests filtering response data by TIMDEX source."""
    data = query_duckdb_for_records_datatable(
        records_duckdb_filepath, source_filter=source_filter
    )
    assert data["recordsFiltered"] == expected_count


@pytest.mark.parametrize(
    ("modified_fields_filter", "expected_count"),
    [
        (None, 1278),
        (["publishers"], 386),
        (["bad_field_name"], 0),
    ],
)
def test_datatables_duckdb_query_modified_field_filtering(
    records_duckdb_filepath, modified_fields_filter, expected_count
):
    """Tests filtering response data by TIMDEX source."""
    data = query_duckdb_for_records_datatable(
        records_duckdb_filepath, modified_fields_filter=modified_fields_filter
    )
    assert data["recordsFiltered"] == expected_count


@pytest.mark.parametrize(
    ("fulltext_search", "expected_count"),
    [
        (None, 1278),
        ("Engineering", 164),
        ("MIT", 433),
    ],
)
def test_datatables_duckdb_query_record_fulltext_search(
    records_duckdb_filepath, fulltext_search, expected_count
):
    """Tests filtering response data by TIMDEX source."""
    data = query_duckdb_for_records_datatable(
        records_duckdb_filepath, search_value=fulltext_search
    )
    assert data["recordsFiltered"] == expected_count
