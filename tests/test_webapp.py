# ruff: noqa: PLR2004

import json
import os
import signal
from pathlib import Path
from unittest.mock import patch

import pytest


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

    assert (
        "docker_image: transmogrifier-best-job-008e20c:latest | source: alma | "
        "input_file: s3://timdex-extract-prod-300442551476/alma/alma-2024-08-29-daily"
        "-extracted-records-to-index.xml" in kwargs["transform_logs"]
    )
    assert (
        "docker_image: transmogrifier-best-job-395e612:latest | source: alma | "
        "input_file: s3://timdex-extract-prod-300442551476/alma/alma-2024-08-29-daily"
        "-extracted-records-to-index.xml" in kwargs["transform_logs"]
    )


def test_shutdown_route_success(caplog, webapp_client):
    with patch("os.kill") as mock_pid_kill:
        _response = webapp_client.get("/shutdown")

    assert "Shutting down flask webapp..." in caplog.text
    mock_pid_kill.assert_called_once_with(os.getpid(), signal.SIGINT)
