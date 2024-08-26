# transmogrifier-ab-diff

Compare transformed TIMDEX records from two versions (A,B) of Transmogrifier.

# abdiff

`abdiff` is the name of the CLI application in this repository that performs an A/B test of Transmogrifier.

## Concepts

A **Job** in `abdiff` represents the A/B test for comparing the results from two versions of Transmogrifier.  When a job is first created, a working directory and a JSON file `job.json` with an initial set of configurations is created.

`job.json` follows roughly the following format:

```json
{
   "job_name": "<slugified version of passed job name>",
   "transmogrifier_version_a": "<git commit SHA or tag name of version 'A' of Transmogrifier>",
   "transmogrifier_version_b": "<git commit SHA or tag name of version 'B' of Transmogrifier>",
   // any other data helpful to store about the job...
}
```

A **Run** is the _execution_ of a job. The outputs from a run are fully encapsulated in a nested sub-directory of the job folder, with each run uniquely identified by the timestamp of execution (formatted as `YYYY-MM-DD_HH-MM-SS`). When a run is executed, the job JSON file is cloned into the run folder as `run.json`, and is then updated with details about the run along the way.

A `run.json` follows roughly the following format, demonstrating fields added by the run:

```json
{
   // all data from job.json included...,
   "timestamp": "2024-08-23_15-55-00",
   "transmogrifier_docker_image_a": "transmogrifier-job-<name>-version-a:latest",
   "transmogrifier_docker_image_b": "transmogrifier-job-<name>-version-b:latest",
   "input_files": [
      "s3://path/to/extract_file_1.xml",
      "s3://path/to/extract_file_2.xml"
   ]
   // any other data helpful to store about the run...
}
```

By default, all job working directories are created in `./output`.  Taken altogether, the following sketches a single job `"test-refactor"` and two runs `"2024-08-23_12-10-00"` and `"2024-08-23_13-30-00"`, and the resulting file structure:

```text
├── output
│   └── test-refactor
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

Coming soon...

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

_Delete this section if it isn't applicable to the PR._

```shell
ROOT_WORKING_DIRECTORY=### Location for Jobs and other working artifacts; defaults to relative `./output`
```




