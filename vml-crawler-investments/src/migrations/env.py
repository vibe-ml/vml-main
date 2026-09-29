"""Run investment migrations transactionally in their configured namespace."""

from collections.abc import Iterable

import sqlalchemy as sa
from alembic import context
from alembic.operations.ops import MigrationScript
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import Connection
from sqlalchemy.schema import CreateSchema

from src.common.settings import Settings

config = context.config
settings = config.attributes.get("settings") or Settings()


def sequential_revision(
    migration_context: MigrationContext,
    revision: str | Iterable[str | None] | Iterable[str],
    directives: list[MigrationScript],
) -> None:
    """Assign the next zero-padded revision number for a linear migration history."""
    head = ScriptDirectory.from_config(config).get_current_head()
    if directives:
        directives[0].rev_id = f"{int(head or '0') + 1:04d}"


def migrate(connection: Connection) -> None:
    """Serialize schema setup and apply Alembic operations in one transaction."""
    with connection.begin():
        connection.execute(
            sa.select(
                sa.func.pg_advisory_xact_lock(
                    sa.func.hashtext("investments:migrations")
                )
            )
        )
        connection.execute(CreateSchema("migrations", if_not_exists=True))
        connection.execute(
            CreateSchema(settings.schema_investments, if_not_exists=True)
        )
        context.configure(
            connection=connection,
            version_table_schema="migrations",
            version_table="alembic_" + settings.alembic_name,
            process_revision_directives=sequential_revision,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    raise RuntimeError("Investment migrations require an online PostgreSQL connection")

connection = config.attributes.get("connection")
if connection is not None:
    migrate(connection)
else:
    engine = sa.create_engine(
        settings.database_url.get_secret_value(), hide_parameters=True
    )
    try:
        with engine.connect() as connection:
            migrate(connection)
    finally:
        engine.dispose()
