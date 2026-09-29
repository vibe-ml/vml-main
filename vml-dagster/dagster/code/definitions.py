"""Add pipeline definitions here; the smoke job verifies deployed run execution."""

from dagster import Definitions, job, op


@op
def deployment_check():
    return "Dagster deployment is operational"


@job
def deployment_smoke_test():
    deployment_check()


defs = Definitions(jobs=[deployment_smoke_test])
