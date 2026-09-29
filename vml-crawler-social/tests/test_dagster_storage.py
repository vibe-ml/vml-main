"""Check the PostgreSQL event storage used by deployed Dagster workers."""

import logging
import time
from uuid import uuid4

import sqlalchemy as sa
from dagster._core.events.log import EventLogEntry
from dagster_postgres.event_log import PostgresEventLogStorage

from tests.conftest import Database


def test_dagster_event_storage(database: Database) -> None:
    """Persist an event through Dagster's default PostgreSQL driver."""
    url = sa.make_url(database.url.get_secret_value())
    engine = sa.create_engine(url)
    with engine.begin() as connection:
        connection.execute(sa.schema.CreateSchema(database.schema_social))
    engine.dispose()

    # Dagster's structured postgres_db config generates a driverless URL.
    url = url.set(drivername="postgresql").update_query_dict(
        {"options": f"-csearch_path={database.schema_social}"}
    )
    storage = PostgresEventLogStorage(url.render_as_string(hide_password=False))
    run_id = str(uuid4())
    try:
        storage.store_event(
            EventLogEntry(
                error_info=None,
                level=logging.INFO,
                user_message="Deployment event storage check",
                run_id=run_id,
                timestamp=time.time(),
            )
        )
        events = storage.get_logs_for_run(run_id)
        assert len(events) == 1
        assert events[0].user_message == "Deployment event storage check"
    finally:
        storage.dispose()
