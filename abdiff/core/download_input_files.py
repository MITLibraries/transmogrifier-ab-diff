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

    for input_file in input_files:
        if check_object_exists(CONFIG.TIMDEX_BUCKET, input_file, s3_client):
            logger.info(f"File found for input: {input_file}. Skipping download.")
            continue

        logger.info(f"Downloading input file from {CONFIG.TIMDEX_BUCKET}: {input_file}")
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

        try:
            copy_process = subprocess.run(
                args=copy_command, check=True, capture_output=True
            )
            subprocess.run(
                args=upload_command,
                check=True,
                input=copy_process.stdout,
            )
        except subprocess.CalledProcessError:
            logger.exception(f"Failed to download input file: {input_file}")


def check_object_exists(bucket: str, input_file: str, s3_client: S3Client) -> bool:
    key = input_file.replace(f"s3://{bucket}/", "")
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as exception:
        if exception.response["Error"]["Code"] == "404":
            return False
        logger.exception(f"Cannot determine if object exists for key {key}.")
        return False
    else:
        return True
