from src.ingestion.snapshot import process_batches

"""Process and rebuild derived batches from retained inputs."""


from dagster import job, op

from src.ingestion.job import runtime_resource


@op
def empty_dependency() -> str:
    return ""


@job(resource_defs={"runtime": runtime_resource})
def rebuild_job() -> None:
    process_batches(empty_dependency())
