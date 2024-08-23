# transmogrifier-ab-diff

Compare transformed TIMDEX records from two versions (A,B) of Transmogrifier.

# abdiff

`abdiff` is the name of the CLI application in this repository that performs an A/B test of Transmogrifier by 
performing the following:

1. initialize a new "Job" with two git commit SHAs of Transmogrifier (A and B) to compare the transformed records from
2. locally build A and B Docker images of Transmogrifier based on these commits
3. accept and transform a batch of input files, for A and B versions of Transmogrifier, constituting a "Run"
4. collate all A and B transformed records into a single parquet file
5. for each `timdex_record_id` create a diff of version A and B
6. generate metrics derived from all diffs
7. provide a Flask app to view and explore the metrics

## Job, Run, and File Structure

A "Job" is defined as two versions of Transmogrifier to compare.  A "Run" is a run of this Job, given a set of input files
to transform.  Each Job gets its own directory, and each Run is fully encapsulated in a sub-directory of the Job.  All of this
is written to local `./output` directory.

This results in a file structure similar to the following:

```text
├── output
│   └── test-refactor-1
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
<OPTIONAL_ENV>=### Description for optional environment variable
```




