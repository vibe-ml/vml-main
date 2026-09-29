"""Dagster code location for social collection."""

import os

from dagster import Definitions, ScheduleDefinition

from src.collection.jobs import social_job

defs = Definitions(
    jobs=[social_job],
    schedules=[
        ScheduleDefinition(
            name="daily_social_schedule",
            job=social_job,
            cron_schedule=os.getenv("SOCIAL_CADENCE", "0 3 * * *"),
            execution_timezone="UTC",
        )
    ],
)
