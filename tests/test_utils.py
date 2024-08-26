import json
import os.path
from pathlib import Path

from abdiff.config import Config
from abdiff.core.utils import (
    get_job_slug_and_working_directory,
    update_or_create_job_json,
)

CONFIG = Config()


def test_job_slug_success(job_name, job_slug, tmp_path):
    _job_slug, _ = get_job_slug_and_working_directory(job_name)
    assert job_slug == _job_slug


def test_job_slug_remove_special_characters():
    job_name = "abc 123 $#$#( // :: !! def $## 456"
    job_slug, _ = get_job_slug_and_working_directory(job_name)
    assert job_slug == "abc-123-def-456"


def test_job_working_directory_success(job_name, job_slug, tmp_path):
    _, job_dir = get_job_slug_and_working_directory(job_name)
    assert job_dir == Path(CONFIG.data_directory) / job_slug


def test_create_job_json_returns_initial_data(job_name, job_working_directory):
    initial_job_data = {"msg": "in a bottle"}
    set_job_data = update_or_create_job_json(job_name, initial_job_data)
    assert set_job_data == initial_job_data


def test_create_job_json_creates_file(job_name, job_working_directory):
    initial_job_data = {"msg": "in a bottle"}
    update_or_create_job_json(job_name, initial_job_data)
    _, job_dir = get_job_slug_and_working_directory(job_name)
    assert os.path.exists(job_dir / "job.json")


def test_update_job_json_success(job_name, job_working_directory):
    # simulate pre-existing job JSON file + data
    with open(job_working_directory / "job.json", "w") as f:
        json.dump({"msg": "in a bottle"}, f)

    job_data = update_or_create_job_json(job_name, {"msg2": "still in bottle"})
    assert job_data == {
        "msg": "in a bottle",
        "msg2": "still in bottle",
    }
