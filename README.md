# transmogrifier-ab-diff

Compare transformed TIMDEX records from two versions (A,B) of Transmogrifier.

# abdiff

`abdiff` is the name of the CLI application in this repository that performs an A/B test of Transmogrifier.

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

The following sketches a single job `"test-refactor"` and two runs `"2024-08-23_12-10-00"` and `"2024-08-23_13-30-00"`, and the resulting file structure:

```text
│   ─── test-refactor
│       ├── job.json
│       └── runs
│           ├── 2024-08-23_12-10-00
│           │   ├── collate_ab.parquet
│           │   ├── diff_ab.parquet
│           │   ├── metrics_ab.json
│           │   ├── run.json
│           │   └── transformed
│           │       ├── a
│           │       │   ├── alma-2024-01-01-transformed-to-index.json
│           │       │   └── dspace-2024-03-15-transformed-to-index.json
│           │       └── b
│           │           ├── alma-2024-01-01-transformed-to-index.json
│           │           └── dspace-2024-03-15-transformed-to-index.json
│           └── 2024-08-23_13-30-00
               └── # and similar structure here for this run...
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
```
Usage: -c init-job [OPTIONS]

  Initialize a new Job.

Options:
  -m, --message TEXT        Message to describe Job.
  -d, --job-directory TEXT  Job working directory to create.  [required]
  -a, --commit-sha-a TEXT   Transmogrifier commit SHA for version 'A'
                            [required]
  -b, --commit-sha-b TEXT   Transmogrifier commit SHA for version 'B'
                            [required]
  -h, --help                Show this message and exit.
```


## Development

- To preview a list of available Makefile commands: `make help`
- To install with dev dependencies: `make install`
- To update dependencies: `make update`
- To run unit tests: `make test`
- To lint the repo: `make lint`
- To run the app: `pipenv run abdiff --help`

## Environment Variables

### Required

```shell
WORKSPACE=### Set to `dev` for local development, this will be set to `stage` and `prod` in those environments by Terraform.
```

### Optional

```shell
```




