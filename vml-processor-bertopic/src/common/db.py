"""Database engine construction."""

from sqlalchemy import Engine, create_engine

from src.models.upstream import UPSTREAM_SCHEMA


def create_processor_engine(
    database_url: str, *, upstream_schema: str = UPSTREAM_SCHEMA
) -> Engine:
    """Create an engine that resolves crawler tables in the configured upstream schema.

    Args:
        database_url: SQLAlchemy URL of the database holding the crawler tables.
        upstream_schema: Schema that holds the crawler's `raw` tables in this database.
    """
    engine = create_engine(database_url, pool_pre_ping=True)
    return engine.execution_options(
        schema_translate_map={UPSTREAM_SCHEMA: upstream_schema}
    )
