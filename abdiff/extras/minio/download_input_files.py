import logging
import subprocess

import boto3
from botocore.exceptions import ClientError
from mypy_boto3_s3.client import S3Client

from abdiff.config import Config

logger = logging.getLogger(__name__)

CONFIG = Config()


def download_input_files(input_files: list[str]) -> None:
    """Download extract files from S3 to a local MinIO server.

    For each file download, two AWS CLI commands are run by subprocess.
    The output from the first command is piped to the second command.
    These commands are further explained below:

        1. Copy the contents from the input file and direct to stdout.
           ```
           aws s3 cp <input_file> -
           ```

        2. Given the stdout from the previous command as input, copy the contents
           to a similarly named file on the local MinIO server.
           ```
           aws s3 cp --endpoint-url <minio_s3_url> --profile minio - <input_file>
           ```

    Note: An S3 client connected to the local MinIO server will check whether the
          file exists prior to any download.
    """
    s3_client = boto3.client(
        "s3",
        endpoint_url=CONFIG.minio_s3_url,
        aws_access_key_id=CONFIG.minio_root_user,
        aws_secret_access_key=CONFIG.minio_root_password,
    )

    success_count = 0
    fail_count = 0
    for i, input_file in enumerate(input_files):
        try:
            download_input_file(input_file, s3_client)
            success_count += 1
            logger.info(
                f"Input file: {i + 1} / {len(input_files)}: '{input_file}' "
                "available locally for transformation."
            )
        except subprocess.CalledProcessError:
            fail_count += 1
            logger.info(
                f"Input file: {i + 1} / {len(input_files)}: '{input_file}' "
                "failed to download."
            )
    logger.info(
        f"Available input files: {success_count}, missing input files: {fail_count}."
    )

    if fail_count > 0:
        raise RuntimeError(  # noqa: TRY003
            f"{fail_count} input file(s) failed to download."
        )


def download_input_file(input_file: str, s3_client: S3Client) -> None:
    if check_object_exists(CONFIG.TIMDEX_BUCKET, input_file, s3_client):
        return
    copy_command = ["aws", "s3", "cp", input_file, "-"]
    upload_command = [
        "aws",
        "s3",
        "cp",
        "--endpoint-url",
        CONFIG.minio_s3_url,
        "--profile",
        "minio",
        "-",
        input_file,
    ]
    copy_process = subprocess.run(args=copy_command, check=True, capture_output=True)
    subprocess.run(
        args=upload_command,
        check=True,
        input=copy_process.stdout,
    )


def check_object_exists(bucket: str, input_file: str, s3_client: S3Client) -> bool:
    key = input_file.replace(f"s3://{bucket}/", "")
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as exception:
        if exception.response["Error"]["Code"] == "404":
            return False
        return False
    else:
        return True
