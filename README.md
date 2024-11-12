# transmogrifier-ab-diff

Compare transformed TIMDEX records from two versions (A,B) of Transmogrifier.

# abdiff

`abdiff` is the name of the CLI application in this repository that performs an A/B test of Transmogrifier.

## Development

- To preview a list of available Makefile commands: `make help`
- To install with dev dependencies: `make install`
- To update dependencies: `make update`
- To run unit tests: `make test`
- To lint the repo: `make lint`
- To run the app: `pipenv run abdiff --help`

### Running a Local MinIO Server

TIMDEX extract files from S3 (i.e., input files to use in transformations) can be downloaded to a local MinIO server hosted via a Docker container. [MinIO is an object storage solution that provides an Amazon Web Services S3-compatible API and supports all core S3 features](https://min.io/docs/minio/kubernetes/upstream/). The MinIO server acts as a "local S3 file system", allowing the app to access data on disk through an S3 interface. Since the MinIO server runs in a Docker container, it can be easily started when needed and stopped when not in use. Any data stored in the MinIO server will persist as long as the files exist in the directory specified for `MINIO_S3_LOCAL_STORAGE`.

Downloading extract files improves the runtime of a diff by reducing the number of requests sent to S3 and avoids AWS credentials timing out. Once an extract file is stored in the local MinIO server, the app can access the data from MinIO for all future runs that include the extract file, avoiding repeated downloads of data used across multiple runs. 


1. Configure your `.env` file. In addition to the [required environment variables](#required), the following environment variables must also be set:
   
   ```text
   MINIO_S3_LOCAL_STORAGE=# full file system path to the directory where MinIO stores its object data on the local disk
   MINIO_ROOT_USER=# username for root user account for MinIO server
   MINIO_ROOT_PASSWORD=# password for root user account MinIO server
   TIMDEX_BUCKET=# when using CLI command 'timdex-sources-csv', this is required to know what TIMDEX bucket to use
   ```

   Note: There are additional variables required by the Local MinIO server (see vars prefixed with "MINIO" in [optional environment variables](#optional)). For these variables, defaults are provided in [abdiff.config](abdiff/config.py).

2. Create an AWS profile `minio`. When prompted for an "AWS Access Key ID" and "AWS Secret Access Key", pass the values set for the `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` environment variables, respectively.
   ```shell
   aws configure --profile minio
   ```

3. Launch a local MinIO server via Docker container by running the Makefile command: 
   ```shell 
   make start-minio-server
   ```

   The API is accessible at: http://127.0.0.1:9000.
   The WebUI is accessible at: http://127.0.0.1:9001.

4. On your browser, navigate to the WebUI and sign into the local MinIO server. Create a bucket in the local MinIO server named after the S3 bucket containing the TIMDEX extract files that will be used in the A/B Diff.

5. Proceed with A/B Diff CLI commands as needed!

Once a diff run is complete, you can stop the local MinIO server using the Makefile command: `make stop-minio-server`. If you're planning to run another diff using the same files, all you have to do is restart the local MinIO server. Your data will persist as long as the files exist in the directory you specified for `MINIO_S3_LOCAL_STORAGE`.

## Concepts

A **Job** in `abdiff` represents the A/B test for comparing the results from two versions of Transmogrifier.  When a job is first created, a working directory and a JSON file `job.json` with an initial set of configurations is created.

`job.json` follows roughly the following format:

```json
{
	"job_directory": "amazing-job",
	"job_message": "This job is testing all the things.",
	"image_tag_a": "transmogrifier-example-job-1-abc123:latest",
	"image_tag_b": "transmogrifier-example-job-1-def456:latest",
	// potentially other job related data...
}
```

A **Run** is the _execution_ of a job. The outputs from a run are fully encapsulated in a nested sub-directory of the job folder, with each run uniquely identified by the timestamp of execution (formatted as `YYYY-MM-DD_HH-MM-SS`). When a run is executed, the job JSON file is cloned into the run folder as `run.json`, and is then updated with details about the run along the way.

A `run.json` follows roughly the following format, demonstrating fields added by the run:

```json
{
   // all job data...
   "timestamp": "2024-08-23_15-55-00",   
   "input_files": [
      "s3://path/to/extract_file_1.xml",
      "s3://path/to/extract_file_2.xml"
   ]
   // potentially other run related data...
}
```

The following sketches a single job `"mvp"` after a single run `"2024-10-15_19-44-05"`, and the resulting file structure:

```text
output/mvp
├── job.json
└── runs
    └── 2024-10-15_19-44-05
        ├── collated
        │   └── records-0.parquet
        ├── diffs
        │   ├── has_diff=false
        │   │   └── records-0.parquet
        │   └── has_diff=true
        │       └── records-0.parquet
        ├── metrics
        │   └── records-0.parquet
        ├── run.json
        └── transformed
            ├── a
            │   ├── alma-2023-02-19-daily-transformed-records-to-index.json
            │   ├── dspace-2024-10-11-daily-transformed-records-to-index.json
            │   └── libguides-2024-04-03-full-transformed-records-to-index.json
            ├── b
            │   ├── alma-2023-02-19-daily-transformed-records-to-index.json
            │   ├── dspace-2024-10-11-daily-transformed-records-to-index.json
            │   └── libguides-2024-04-03-full-transformed-records-to-index.json
            └── logs.txt
```

## Environment Variables

### Required

```text
WORKSPACE=dev # required by convention, but not actively used
AWS_ACCESS_KEY_ID=# passed to Transmogrifier containers for use
AWS_SECRET_ACCESS_KEY=# passed to Transmogrifier containers for use 
AWS_SESSION_TOKEN=# passed to Transmogrifier containers for use
```

### Optional

```text
MINIO_S3_LOCAL_STORAGE=# full file system path to the directory where MinIO stores its object data on the local disk
MINIO_S3_URL=# endpoint for MinIO server API; default is "http://localhost:9000/"
MINIO_S3_CONTAINER_URL=# endpoint for the MinIO server when acccessed from inside a Docker container; default is "http://host.docker.internal:9000/"
MINIO_ROOT_USER=# username for root user account for MinIO server
MINIO_ROOT_PASSWORD=# password for root user account MinIO server
WEBAPP_HOST=# host for flask webapp
WEBAPP_PORT=# port for flask webapp
TRANSMOGRIFIER_MAX_WORKERS=# max number of Transmogrifier containers to run in parallel; default is 6
TRANSMOGRIFIER_TIMEOUT=# timeout for a single Transmogrifier container; default is 5 hours
TIMDEX_BUCKET=# when using CLI command 'timdex-sources-csv', this is required to know what TIMDEX bucket to use
PRESERVE_ARTIFACTS=# if 'true', intermediate artifacts like transformed files, collated records, etc., will not be automatically removed
ALLOW_FAILED_TRANSMOGRIFIER_CONTAINERS=# if 'true' (default), the run will continue even if some Transmogrifier containers failed to complete successfully
```

## CLI commands

### `abdiff`
```text
Usage: -c [OPTIONS] COMMAND [ARGS]...

Options:
  -v, --verbose  Pass to log at debug level instead of info.
  -h, --help     Show this message and exit.

Commands:
  init-job  Initialize a new Job.
  ping      Debug ping/pong command.
```

### `abdiff ping`
```text
Usage: -c ping [OPTIONS]

  Debug ping/pong command.

Options:
  -h, --help  Show this message and exit.
```

### `abdiff init-job`
```text
Usage: -c init-job [OPTIONS]

  Initialize a new Job.

Options:
  -d, --job-directory TEXT  Job directory to create.  [required]
  -m, --message TEXT        Message to describe Job.
  -a, --commit-sha-a TEXT   Transmogrifier commit SHA for version 'A'
                            [required]
  -b, --commit-sha-b TEXT   Transmogrifier commit SHA for version 'B'
                            [required]
  -h, --help                Show this message and exit.
```

### `abdiff run-diff`
```text
Usage: -c run-diff [OPTIONS]

Options:
  -d, --job-directory TEXT  Job directory to create.  [required]
  -i, --input-files TEXT    Input files to transform.  This may be a comma
                            separated list of input files, or a local CSV file
                            that provides a list of files.  [required]
  -m, --message TEXT        Message to describe Run.
  -h, --help                Show this message and exit.
```

### `abdiff view-job`
```text
Usage: -c view-job [OPTIONS]

  Start flask app to view Job and Runs.

Options:
  -d, --job-directory TEXT  Job directory to view in webapp.  [required]
  -h, --help                Show this message and exit.
```

### `timdex-sources-csv`
```text
Usage: -c timdex-sources-csv [OPTIONS]

  Generate a CSV of ordered extract files for all, or a subset, of TIMDEX
  sources.

  This CSV may be passed to CLI command 'run-diff' for the '-i / --input-
  files' argument, serving as the list of input files for the run.

  This command requires that env var 'TIMDEX_BUCKET' is set to establish what
  S3 bucket to use for scanning.  The appropriate AWS credentials are also
  needed to be set.

Options:
  -o, --output-file TEXT  Output filepath for CSV.  [required]
  -s, --sources TEXT      Optional comma separated list of sources to include.
                          Default is all.
  -h, --help              Show this message and exit.
```