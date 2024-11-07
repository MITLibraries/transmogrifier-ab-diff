import glob
import json
import logging
import os
import signal
from datetime import datetime
from pathlib import Path
from time import perf_counter

from flask import Flask, Response, g, jsonify, render_template, request

from abdiff.core.utils import read_run_json
from abdiff.webapp.utils import (
    get_record_a_b_versions,
    get_record_field_diff_summary,
    get_record_unified_diff_string,
    get_run_directory,
    query_duckdb_for_records_datatable,
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
        runs = dict(
            sorted(
                runs.items(),
                key=lambda x: datetime.strptime(  # noqa: DTZ007
                    x[0], "%Y-%m-%d_%H-%M-%S"
                ),
            )
        )

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

        return render_template(
            "run.html",
            run_data=run_data,
            run_json=json.dumps(run_data),
            transform_logs=transform_logs,
            metrics_json=json.dumps(run_data["metrics"]),
            sources=sorted(run_data["metrics"]["summary"]["sources"]),
            modified_fields=sorted(run_data["metrics"]["summary"]["fields_with_diffs"]),
        )

    @app.route("/run/<run_timestamp>/records/data", methods=["POST"])
    def records_data(run_timestamp: str) -> Response:
        """Endpoint to provide data for Records table in Run view.

        The Javascript library DataTables (https://datatables.net/) is used to create the
        Records table in the Run view.  This table is configured to make HTTP POST
        requests to an endpoint for filtered, paginated data that supplies the table. This
        endpoint provides that data.

        The POST request payload conforms to the request signature here:
        https://datatables.net/manual/server-side.  This endpoint receives the parameters
        from the table (e.g. page, ordering, filtering, etc.), parses the query parameters
        from the request payload, and passes to a utility function which performs the
        DuckDB query, returning a dataframe of results suitable for the table.
        """
        start_time = perf_counter()
        run_directory = get_run_directory(run_timestamp)
        run_data = read_run_json(run_directory)

        datatables_data = query_duckdb_for_records_datatable(
            run_data["duckdb_filepath"],
            draw=int(request.form.get("draw", "1")),
            start=int(request.form.get("start", "0")),
            length=int(request.form.get("length", "10")),
            search_value=request.form.get("search[value]", ""),
            order_column_index=int(request.form.get("order[0][column]", "0")),
            order_direction=request.form.get("order[0][dir]", "asc"),
            source_filter=request.form.getlist("sourceFilter[]"),
            modified_fields_filter=request.form.getlist("modifiedFieldsFilter[]"),
        )

        logger.info(f"records data elapsed: {perf_counter()-start_time}")
        return jsonify(datatables_data)

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
