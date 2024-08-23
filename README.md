# transmogrifier-ab-diff

Compare transformed TIMDEX records from two versions (A,B) of Transmogrifier.

# abdiff

TODO...

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
SENTRY_DSN=### If set to a valid Sentry DSN, enables Sentry exception monitoring. This is not needed for local development.
WORKSPACE=### Set to `dev` for local development, this will be set to `stage` and `prod` in those environments by Terraform.
```

### Optional

_Delete this section if it isn't applicable to the PR._

```shell
<OPTIONAL_ENV>=### Description for optional environment variable
```




