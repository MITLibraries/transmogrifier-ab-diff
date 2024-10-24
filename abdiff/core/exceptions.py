class DockerContainersNotFoundError(Exception):
    def __init__(self, run_id: str) -> None:
        super().__init__(f"No Docker containers were found with label run='{run_id}'.")


class DockerContainerTimeoutError(Exception):
    def __init__(self, container_id: str | None, timeout: int) -> None:
        self.container_id = container_id
        self.timeout = timeout
        super().__init__(self.get_formatted_message())

    def get_formatted_message(self) -> str:
        return f"Container {self.container_id} exceed timeout of {self.timeout} seconds."


# core function errors
class DockerContainerRuntimeError(Exception):
    def __init__(self, container_id: str) -> None:
        self.container_id = container_id
        super().__init__(self.get_formatted_message())

    def get_formatted_message(self) -> str:
        return (
            f"Container {self.container_id} did not exit cleanly. "
            "Check the logs in transformed/logs.txt to identify the error."
        )


class InvalidRepositoryCommitSHAError(Exception):
    def __init__(self, repository: str, commit_sha: str):
        super().__init__(
            f"Cannot reset repository ({repository}) to an invalid commit SHA: {commit_sha}."  # noqa: E501
        )


class OutputValidationError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
