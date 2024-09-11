import json
import logging
from collections.abc import Callable
from datetime import timedelta
from time import perf_counter

import click
from click.exceptions import ClickException

from abdiff.config import configure_logger
from abdiff.core import build_ab_images
from abdiff.core import init_job as core_init_job
from abdiff.core.utils import read_job_json

logger = logging.getLogger(__name__)


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


def shared_job_options(cli_command: Callable) -> Callable:
    """Decorator to provide shared CLI arguments to Job related commands."""
    cli_command = click.option(
        "-d",
        "--job-directory",
        type=str,
        required=True,
        help="Job working directory to create.",
    )(cli_command)

    cli_command = click.option(
        "-m",
        "--message",
        type=str,
        required=False,
        help="Message to describe Job.",
        default="Not provided.",
    )(cli_command)

    return cli_command  # noqa: RET504


@main.command()
@shared_job_options
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
    commit_sha_a: str,
    commit_sha_b: str,
    message: str,
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
