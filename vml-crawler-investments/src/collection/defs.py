"""Dagster code location for funding news collection."""

import os

from dagster import Definitions, ScheduleDefinition

from src.collection.jobs import investments_job

defs = Definitions(
    jobs=[investments_job],
    schedules=[
        ScheduleDefinition(
            name="daily_investments_schedule",
            job=investments_job,
            cron_schedule=os.getenv("INVESTMENTS_CADENCE", "0 4 * * *"),
            execution_timezone="UTC",
        )
    ],
)
