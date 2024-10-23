class DockerContainersNotFoundError(Exception):
    def __init__(self, run_id: str) -> None:
        super().__init__(f"No Docker containers were found with label run='{run_id}'.")


class DockerContainerRuntimeExceededTimeoutError(Exception):
    def __init__(self, containers: list, timeout: int) -> None:
        self.containers = containers
        self.timeout = timeout
        super().__init__(self.get_formatted_message())

    def get_formatted_message(self) -> str:
        container_ids = [container.id for container in self.containers]
        return (
            f"Timeout of {self.timeout} seconds exceeded."
            f"{len(container_ids)} container(s) is/are still running:"
            f"{container_ids}."
        )


# core function errors
class DockerContainerRunFailedError(Exception):
    def __init__(self, containers: list) -> None:
        self.containers = containers
        super().__init__(self.get_formatted_message())

    def get_formatted_message(self) -> str:
        return (
            f"The following Docker containers exited with an error: {self.containers}. "
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
