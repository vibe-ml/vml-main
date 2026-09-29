import os

from dagster import Definitions, ScheduleDefinition

from src.ingestion.assets import observable_assets
from src.ingestion.bootstrap import bootstrap_job
from src.ingestion.job import taxonomy_job
from src.ingestion.rebuild import rebuild_job
from src.ingestion.scheduler import daily_refresh_job
from src.ingestion.sensor import batch_claims_sensor, bootstrap_progress_sensor
from src.ingestion.snapshot import snapshot_job

cadence = os.getenv("OPENALEX_INGESTION_CADENCE", "0 0 * * *")

defs = Definitions(
    assets=observable_assets,
    jobs=[taxonomy_job, snapshot_job, bootstrap_job, daily_refresh_job, rebuild_job],
    sensors=[batch_claims_sensor, bootstrap_progress_sensor],
    schedules=[
        ScheduleDefinition(
            name="daily_taxonomy_schedule",
            job=taxonomy_job,
            cron_schedule=os.getenv("OPENALEX_TAXONOMY_CADENCE", cadence),
            execution_timezone="UTC",
        ),
        ScheduleDefinition(
            name="daily_refresh_schedule",
            job=daily_refresh_job,
            cron_schedule=cadence,
            execution_timezone="UTC",
        ),
        ScheduleDefinition(
            name="process_batches_schedule",
            job=rebuild_job,
            cron_schedule=os.getenv("OPENALEX_BATCH_CADENCE", cadence),
            execution_timezone="UTC",
        ),
    ],
)
