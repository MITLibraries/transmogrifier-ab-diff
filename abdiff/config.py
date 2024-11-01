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
        "MINIO_S3_LOCAL_STORAGE",
        "MINIO_S3_URL",
        "MINIO_S3_CONTAINER_URL",
        "MINIO_ROOT_USER",
        "MINIO_ROOT_PASSWORD",
        "WEBAPP_HOST",
        "WEBAPP_PORT",
        "TRANSMOGRIFIER_MAX_WORKERS",
        "TRANSMOGRIFIER_TIMEOUT",
        "TIMDEX_BUCKET",
    )

    def __getattr__(self, name: str) -> Any:  # noqa: ANN401
        """Method to raise exception if required env vars not set."""
        if name in self.REQUIRED_ENV_VARS or name in self.OPTIONAL_ENV_VARS:
            return os.getenv(name)
        message = f"'{name}' not a valid configuration variable"
        raise AttributeError(message)

    @property
    def minio_s3_url(self) -> str:
        return self.MINIO_S3_URL or "http://localhost:9000/"

    @property
    def minio_s3_container_url(self) -> str:
        return self.MINIO_S3_CONTAINER_URL or "http://host.docker.internal:9000/"

    @property
    def minio_root_user(self) -> str:
        return self.MINIO_ROOT_USER or "minioadmin"

    @property
    def minio_root_password(self) -> str:
        return self.MINIO_ROOT_PASSWORD or "minioadmin"

    @property
    def webapp_host(self) -> str:
        return self.WEBAPP_HOST or "localhost"

    @property
    def webapp_port(self) -> int:
        port = self.WEBAPP_PORT or "5000"
        return int(port)

    @property
    def transmogrifier_max_workers(self) -> int:
        """Maximum number of Transmogrifier containers to run in parallel."""
        max_workers = self.TRANSMOGRIFIER_MAX_WORKERS or 6
        return int(max_workers)

    @property
    def transmogrifier_timeout(self) -> int:
        """Timeout for a single Transmogrifier container."""
        timeout = self.TRANSMOGRIFIER_TIMEOUT or 60 * 60 * 5  # 5 hours default
        return int(timeout)

    @property
    def active_timdex_sources(self) -> list[str]:
        return [
            "alma",
            "aspace",
            "dspace",
            "gismit",
            "gisogm",
            "libguides",
            "researchdatabases",
        ]


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
