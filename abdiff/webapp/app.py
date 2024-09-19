import glob
import json
import logging
import os
import signal
from pathlib import Path

from flask import Flask, g, render_template

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
        run_directory = Path(g.job_directory) / "runs" / run_timestamp
        # load run JSON data
        with open(run_directory / "run.json") as f:
            run_data = json.load(f)

        # load transform logs
        try:
            with open(run_directory / "transformed/logs.txt") as f:
                transform_logs = f.read()
        except FileNotFoundError:
            transform_logs = "'logs.txt' not found for transform logs"

        # load run metrics
        try:
            with open(run_directory / "metrics.json") as f:
                metrics = json.load(f)
        except FileNotFoundError:
            metrics = {"note": "'metrics.json' not found in run directory"}

        return render_template(
            "run.html",
            run_data=run_data,
            run_json=json.dumps(run_data),
            transform_logs=transform_logs,
            metrics_json=json.dumps(metrics),
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
