import glob
import json
import logging
import os
import signal
from pathlib import Path

from flask import Flask, g, render_template

from abdiff.core.utils import read_run_json
from abdiff.webapp.utils import (
    get_field_sample_records,
    get_record_a_b_versions,
    get_record_field_diff_summary,
    get_record_unified_diff_string,
    get_run_directory,
    get_source_sample_records,
)

logger = logging.getLogger(__name__)


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["DEBUG"] = True

    @app.before_request
    def set_job_directory() -> None:
        """Set job directory for flask webapp.

        This before request method will first look for a job directory set as part of the
        app's config via a CLI invocation, and second for env var JOB_DIRECTORY which is
        useful for testing and development.
        """
        app.config["JOB_DIRECTORY"] = app.config.get(
            "JOB_DIRECTORY", os.environ.get("JOB_DIRECTORY")
        )
        if not app.config.get("JOB_DIRECTORY"):
            raise RuntimeError(  # noqa: TRY003
                "A job directory is required for flask app."
            )
        g.job_directory = app.config["JOB_DIRECTORY"]
        g.job_name_pretty = ".../" + g.job_directory.split("/")[-1]

    @app.route("/ping", methods=["GET"])
    def ping() -> str:
        return "pong"

    @app.route("/", methods=["GET"])
    def job() -> str:
        """Primary route for a Job."""
        # load job JSON data
        job_json_filepath = Path(g.job_directory) / "job.json"
        with open(job_json_filepath) as f:
            job_json = f.read()

        # crawl runs
        runs = {}
        for run_json_filepath in glob.glob(f"{g.job_directory}/runs/**/run.json"):
            with open(run_json_filepath) as f:
                run_data = json.load(f)
            runs[run_data["run_timestamp"]] = run_data

        return render_template(
            "job.html",
            job_json=job_json,
            runs=runs,
        )

    @app.route("/run/<run_timestamp>", methods=["GET"])
    def run(run_timestamp: str) -> str:
        """Primary route for a Run."""
        run_directory = get_run_directory(run_timestamp)
        run_data = read_run_json(run_directory)

        # load transform logs
        try:
            with open(Path(run_directory) / "transformed/logs.txt") as f:
                transform_logs = f.read()
        except FileNotFoundError:
            transform_logs = "'logs.txt' not found for transform logs"

        # parse run metrics
        metrics = run_data.get(
            "metrics", {"warning": "'metrics' section not found in run data"}
        )

        # generate links for field and source samples
        field_samples = {
            field: f"http://localhost:5000/run/{run_timestamp}/sample/field/{field}"
            for field in metrics["summary"]["fields_with_diffs"]
        }
        source_samples = {
            source: f"http://localhost:5000/run/{run_timestamp}/sample/source/{source}"
            for source in metrics["summary"]["sources"]
        }
        sample_links = {
            "field_samples": field_samples,
            "source_samples": source_samples,
        }

        return render_template(
            "run.html",
            run_data=run_data,
            run_json=json.dumps(run_data),
            transform_logs=transform_logs,
            metrics_json=json.dumps(metrics),
            sample_links=sample_links,
        )

    @app.route(
        "/run/<run_timestamp>/sample/<sample_type>/<sample_value>", methods=["GET"]
    )
    def run_sample(run_timestamp: str, sample_type: str, sample_value: str) -> str:
        """Route to provide links to record views based on a subset of detected diffs."""
        run_directory = get_run_directory(run_timestamp)

        # get sample records
        if sample_type == "field":
            sample_df = get_field_sample_records(run_directory, sample_value)
        elif sample_type == "source":
            sample_df = get_source_sample_records(run_directory, sample_value)
        else:
            raise ValueError(  # noqa: TRY003
                f"Sample type: '{sample_type}' not recognized"
            )
        sample_df["record_link"] = sample_df.timdex_record_id.apply(
            lambda timdex_record_id: (
                f"http://localhost:5000/run/{run_timestamp}/record/{timdex_record_id}"
            )
        )
        sample_df = sample_df.sort_values(by=["source", "timdex_record_id"])

        return render_template(
            "sample.html",
            sample_type=sample_type,
            sample_value=sample_value,
            sample_df=sample_df,
        )

    @app.route("/run/<run_timestamp>/record/<timdex_record_id>", methods=["GET"])
    def record(run_timestamp: str, timdex_record_id: str) -> str:
        """Record view."""
        run_directory = get_run_directory(run_timestamp)

        # get record A and B versions
        a, b = get_record_a_b_versions(run_directory, timdex_record_id)

        # build summary of record
        summary = get_record_field_diff_summary(run_directory, timdex_record_id)
        summary["fields_only_in_a"] = list(set(a.keys()).difference(set(b.keys())))
        summary["fields_only_in_b"] = list(set(b.keys()).difference(set(a.keys())))

        # get unified diff string of record
        diff_str = get_record_unified_diff_string(a, b)

        return render_template(
            "record.html",
            timdex_record_id=timdex_record_id,
            summary=json.dumps(summary),
            a_json=json.dumps(a),
            b_json=json.dumps(b),
            diff_str=diff_str,
        )

    @app.route("/shutdown", methods=["GET"])
    def shutdown() -> str:
        """Route to shutdown the flask webapp from within the webapp itself.

        This is a bit unusual, but a nice-to-have given that the flask webapp will often
        be run via a CLI command.  This issues a signal to shutdown the OS process that
        the CLI command started.
        """
        logger.info("Shutting down flask webapp...")
        os.kill(os.getpid(), signal.SIGINT)
        return "Flask webapp shutdown success."

    return app


app = create_app()
