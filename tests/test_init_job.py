import os.path

from abdiff.core import init_job


def test_init_job_returns_initialized_job_data(tmp_path, job_name):
    job_data = init_job(job_name)
    assert job_data == {
        "job_name": "Large Refactor Project",
        "job_slug": "large-refactor-project",  # NOTE: the slug form varies slightly
        "working_directory": str(tmp_path / "output/large-refactor-project"),
    }


def test_init_job_creates_working_directory_and_job_json(tmp_path, job_name):
    init_job(job_name)
    assert os.path.exists(tmp_path / "output/large-refactor-project")
    assert os.path.exists(tmp_path / "output/large-refactor-project/job.json")
