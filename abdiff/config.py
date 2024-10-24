import logging
import os
from typing import Any


class Config:
    REQUIRED_ENV_VARS = (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "WORKSPACE",
    )
    OPTIONAL_ENV_VARS = (
        "WEBAPP_HOST",
        "WEBAPP_PORT",
        "TRANSMOGRIFIER_CONCURRENCY",
        "TRANSMOGRIFIER_TIMEOUT",
    )

    def __getattr__(self, name: str) -> Any:  # noqa: ANN401
        """Method to raise exception if required env vars not set."""
        if name in self.REQUIRED_ENV_VARS or name in self.OPTIONAL_ENV_VARS:
            return os.getenv(name)
        message = f"'{name}' not a valid configuration variable"
        raise AttributeError(message)

    @property
    def webapp_host(self) -> str:
        return self.WEBAPP_HOST or "localhost"

    @property
    def webapp_port(self) -> int:
        port = self.WEBAPP_PORT or "5000"
        return int(port)

    @property
    def transmogrifier_concurrency(self) -> int:
        """Maximum number of Transmogrifier containers to run in parallel."""
        max_workers = self.TRANSMOGRIFIER_CONCURRENCY or 6
        return int(max_workers)

    @property
    def transmogrifier_timeout(self) -> int:
        """Timeout for a single Transmogrifier container."""
        timeout = self.TRANSMOGRIFIER_TIMEOUT or 60 * 60 * 5  # 5 hours default
        return int(timeout)


def configure_logger(logger: logging.Logger, *, verbose: bool) -> str:
    if verbose:
        logging.basicConfig(
            format="%(asctime)s %(levelname)s %(name)s.%(funcName)s() line %(lineno)d: "
            "%(message)s"
        )
        logger.setLevel(logging.DEBUG)
        for handler in logging.root.handlers:
            handler.addFilter(logging.Filter("abdiff"))
    else:
        logging.basicConfig(
            format="%(asctime)s %(levelname)s %(name)s.%(funcName)s(): %(message)s"
        )
        logger.setLevel(logging.INFO)
    return (
        f"Logger '{logger.name}' configured with level="
        f"{logging.getLevelName(logger.getEffectiveLevel())}"
    )
