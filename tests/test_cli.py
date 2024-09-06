import os
from unittest.mock import patch

from abdiff.cli import main
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
