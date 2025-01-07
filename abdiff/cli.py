import json
import logging
import shutil
from datetime import timedelta
from itertools import chain
from time import perf_counter

import click
import pandas as pd
from click.exceptions import ClickException

from abdiff.config import Config, configure_logger
from abdiff.core import (
    build_ab_images,
    calc_ab_diffs,
    calc_ab_metrics,
    collate_ab_transforms,
    create_final_records,
    download_input_files,
    init_run,
    run_ab_transforms,
)
from abdiff.core import init_job as core_init_job
from abdiff.core.utils import read_job_json
from abdiff.extras.timdex_sources import get_ordered_extracted_files_all_sources
from abdiff.webapp.app import app

logger = logging.getLogger(__name__)

CONFIG = Config()


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Pass to log at debug level instead of info.",
)
@click.pass_context
def main(
    ctx: click.Context,
    verbose: bool,  # noqa: FBT001
) -> None:
    ctx.ensure_object(dict)
    ctx.obj["START_TIME"] = perf_counter()
    root_logger = logging.getLogger()
    logger.info(configure_logger(root_logger, verbose=verbose))
    logger.info("Running process")


@main.result_callback()
@click.pass_context
def post_main_group_subcommand(
    ctx: click.Context,
    *_args: tuple,
    **_kwargs: dict,
) -> None:
    """Callback for any work to perform after a main sub-command completes."""
    logger.info(
        "Total elapsed: %s",
        str(
            timedelta(seconds=perf_counter() - ctx.obj["START_TIME"]),
        ),
    )


@main.command()
def ping() -> None:
    """Debug ping/pong command."""
    logger.debug("got ping, preparing to pong")
    click.echo("pong")


@main.command()
@click.option(
    "-d",
    "--job-directory",
    type=str,
    required=True,
    help="Job directory to create.",
)
@click.option(
    "-m",
    "--message",
    type=str,
    required=False,
    help="Message to describe Job.",
    default="Not provided.",
)
@click.option(
    "-a",
    "--commit-sha-a",
    type=str,
    required=True,
    help="Transmogrifier commit SHA for version 'A'",
)
@click.option(
    "-b",
    "--commit-sha-b",
    type=str,
    required=True,
    help="Transmogrifier commit SHA for version 'B'",
)
def init_job(
    job_directory: str,
    message: str,
    commit_sha_a: str,
    commit_sha_b: str,
) -> None:
    """Initialize a new Job."""
    try:
        core_init_job(job_directory, message)
    except FileExistsError as exc:
        message = (
            f"Job directory already exists: '{job_directory}', cannot create new job."
        )
        raise ClickException(message) from exc

    build_ab_images(
        job_directory,
        commit_sha_a,
        commit_sha_b,
    )

    job_json = json.dumps(read_job_json(job_directory), indent=2)
    logger.info(f"Job initialized: {job_json}")


@main.command()
@click.option(
    "-d",
    "--job-directory",
    type=str,
    required=True,
    help="Job directory to create.",
)
@click.option(
    "-i",
    "--input-files",
    type=str,
    required=True,
    help=(
        "Input files to transform. This may be a comma separated list of input files, "
        "or a local CSV file that provides a list of files."
    ),
)
@click.option(
    "-m",
    "--message",
    type=str,
    required=False,
    help="Message to describe Run.",
    default="Not provided.",
)
@click.option(
    "--download-files",
    is_flag=True,
    help=(
        "Pass to download input files (or use previously downloaded input files) "
        "from AWS S3. The downloaded files are stored in a local MinIO S3 server "
        "and made available for Transmogrifier to use."
    ),
)
def run_diff(
    job_directory: str, input_files: str, message: str, *, download_files: bool
) -> None:

    job_data = read_job_json(job_directory)
    run_directory = init_run(job_directory, message=message)

    # handle CSV file containing input files
    if input_files.endswith(".csv"):
        input_files_list = pd.read_csv(input_files, header=None)[0].tolist()
    # else, assume comma separated list of extract files
    else:
        input_files_list = [filepath.strip() for filepath in input_files.split(",")]

    if download_files:
        download_input_files(input_files_list)

    ab_transformed_datasets = run_ab_transforms(
        run_directory=run_directory,
        image_tag_a=job_data["image_tag_a"],
        image_tag_b=job_data["image_tag_b"],
        input_files=input_files_list,
        use_local_s3=download_files,
    )

    collated_dataset_path = collate_ab_transforms(
        run_directory=run_directory,
        ab_transformed_datasets=ab_transformed_datasets,
    )

    diffs_dataset_path = calc_ab_diffs(
        run_directory=run_directory,
        collated_dataset_path=collated_dataset_path,
    )

    if not CONFIG.preserve_artifacts:
        shutil.rmtree(collated_dataset_path)

    metrics_dataset_path = calc_ab_metrics(
        run_directory=run_directory,
        diffs_dataset_path=diffs_dataset_path,
    )

    create_final_records(run_directory, diffs_dataset_path, metrics_dataset_path)

    if not CONFIG.preserve_artifacts:
        shutil.rmtree(diffs_dataset_path)
        shutil.rmtree(metrics_dataset_path)


@main.command()
@click.option(
    "-d",
    "--job-directory",
    type=str,
    required=True,
    help="Job directory to view in webapp.",
)
def view_job(
    job_directory: str,
) -> None:
    """Start flask app to view Job and Runs."""
    config = Config()
    logger.info(
        f"Starting flask webapp for job directory: '{job_directory}', "
        f"available at: http://{config.webapp_host}:{config.webapp_port}"
    )
    app.config.update(JOB_DIRECTORY=job_directory)
    logger.info("")
    app.run(host=config.webapp_host, port=config.webapp_port)


@main.command()
@click.option(
    "-o",
    "--output-file",
    type=str,
    required=True,
    help="Output filepath for CSV.",
)
@click.option(
    "-s",
    "--sources",
    type=str,
    required=False,
    help="Optional comma separated list of sources to include.  Default is all.",
)
def timdex_sources_csv(output_file: str, sources: str) -> None:
    """Generate a CSV of ordered extract files for all, or a subset, of TIMDEX sources.

    This CSV may be passed to CLI command 'run-diff' for the '-i / --input-files'
    argument, serving as the list of input files for the run.

    This command requires that env var 'TIMDEX_BUCKET' is set to establish what S3 bucket
    to use for scanning.  The appropriate AWS credentials are also needed to be set.
    """
    sources_list = None
    if sources:
        sources_list = [source.strip() for source in sources.split(",")]

    input_files = get_ordered_extracted_files_all_sources(sources=sources_list)

    input_files_df = pd.DataFrame(
        columns=["input_file"],
        data=[*chain.from_iterable(v for v in input_files.values())],
    )
    input_files_df.to_csv(output_file, index=False, header=False)
    logger.info(f"Created file: {output_file}")
