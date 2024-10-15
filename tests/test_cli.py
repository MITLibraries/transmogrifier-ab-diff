import os
from pathlib import Path
from unittest.mock import patch

from abdiff.cli import app, main
from abdiff.core.utils import read_job_json


def test_cli_default_log_level_info(caplog, runner):
    result = runner.invoke(main, ["ping"])
    assert result.exit_code == 0
    assert "Logger 'root' configured with level=INFO" in caplog.text
    assert "pong" in result.output


def test_cli_verbose_sets_debug_log_level(caplog, runner):
    caplog.set_level("DEBUG")
    result = runner.invoke(main, ["--verbose", "ping"])
    assert result.exit_code == 0
    assert "Logger 'root' configured with level=DEBUG" in caplog.text
    assert "got ping, preparing to pong" in caplog.text


def test_cli_main_group_callback_called(caplog, runner):
    result = runner.invoke(main, ["--verbose", "ping"])
    assert result.exit_code == 0
    assert "Total elapsed" in caplog.text


@patch("abdiff.core.build_ab_images.docker_image_exists")
def test_init_job_all_arguments_success(
    mocked_image_exists,
    caplog,
    runner,
    job_directory,
):
    mocked_image_exists.return_value = True
    caplog.set_level("DEBUG")

    message = "This is a Super Job."
    _result = runner.invoke(
        main,
        [
            "--verbose",
            "init-job",
            f"--job-directory={job_directory}",
            f"--message={message}",
            "--commit-sha-a=abc123",
            "--commit-sha-b=def456",
        ],
    )

    assert os.path.exists(job_directory)

    job_data = read_job_json(job_directory)
    assert job_data == {
        "job_directory": job_directory,
        "job_message": message,
        "image_tag_a": "transmogrifier-example-job-1-abc123:latest",
        "image_tag_b": "transmogrifier-example-job-1-def456:latest",
    }


def test_init_job_pre_existing_job_directory_raise_error(
    caplog,
    runner,
    job_directory,
):
    caplog.set_level("DEBUG")
    os.makedirs(job_directory)
    result = runner.invoke(
        main,
        [
            "--verbose",
            "init-job",
            f"--job-directory={job_directory}",
            "--message=This is a Super Job.",
            "--commit-sha-a=abc123",
            "--commit-sha-b=def456",
        ],
    )
    assert result.exit_code == 1
    assert "Job directory already exists" in result.output


def test_view_job_webapp_configured_and_run_success(
    caplog,
    runner,
):
    job_directory = "tests/fixtures/jobs/example-job-1"

    with patch.object(app, "run") as mocked_run:
        mocked_run.run.return_value = None
        _result = runner.invoke(
            main,
            [
                "--verbose",
                "view-job",
                f"--job-directory={job_directory}",
            ],
        )

    assert app.config["JOB_DIRECTORY"] == job_directory
    assert f"Starting flask webapp for job directory: {job_directory}" in caplog.text
    mocked_run.assert_called()


@patch("abdiff.cli.init_run")
@patch("abdiff.cli.run_ab_transforms")
@patch("abdiff.cli.collate_ab_transforms")
@patch("abdiff.cli.calc_ab_diffs")
@patch("abdiff.cli.calc_ab_metrics")
def test_run_diff_success(
    mock_init_run,
    mock_run_ab_transforms,
    mock_collate_ab_transforms,
    mock_calc_ab_diffs,
    mock_calc_ab_metrics,
    caplog,
    runner,
    example_job_directory,
):
    # mock initialization of run
    mock_init_run.return_value = str(
        Path(example_job_directory) / "runs" / "2024-10-15_12-00-00"
    )

    # mock transformation
    mock_run_ab_transforms.return_value = (
        [
            "transformed/a/alma-2024-01-01-daily-transformed-records-to-index.json",
            "transformed/a/dspace-2024-01-02-daily-transformed-records-to-index.json",
        ],
        [
            "transformed/b/alma-2024-01-01-daily-transformed-records-to-index.json",
            "transformed/b/dspace-2024-01-02-daily-transformed-records-to-index.json",
        ],
    )

    # mock collated dataset
    mock_collate_ab_transforms.return_value = "path/to/run/collated"

    # mock diffs dataset
    mock_calc_ab_diffs.return_value = "path/to/run/diffs"

    # mock metrics generation
    mock_calc_ab_metrics.return_value = {"msg": "these are the from the diffs metrics"}

    caplog.set_level("DEBUG")
    result = runner.invoke(
        main,
        [
            "--verbose",
            "run-diff",
            f"--job-directory={example_job_directory}",
            "--input-files="
            "s3://path/to/files/alma-2024-01-01-daily-extracted-records-to-index.xml,"
            "s3://path/to/files/dspace-2024-01-01-daily-extracted-records-to-index.xml",
        ],
    )
    assert result.exit_code == 0

    mock_init_run.assert_called()
    mock_run_ab_transforms.assert_called()
    mock_collate_ab_transforms.assert_called()
    mock_calc_ab_diffs.assert_called()
    mock_calc_ab_metrics.assert_called()
