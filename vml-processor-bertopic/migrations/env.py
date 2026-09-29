"""Processor Alembic environment.

Migration state lives in `migrations.alembic_<settings.alembic_name>`, separate from
the crawler's migration history. Target metadata is only the processor `Base` models.
"""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool, text

from src.models import base as _base  # noqa: F401
from src.models import emergence as _emergence  # noqa: F401
from src.models import labels as _labels  # noqa: F401
from src.models import topics as _topics  # noqa: F401
from src.models.base import Base

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata
version_table = f"alembic_{os.environ.get('PWF_ALEMBIC_NAME', 'bertopic')}"


def run_migrations_offline() -> None:
    """Run migrations without opening a database connection."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        version_table=version_table,
        version_table_schema="migrations",
        include_schemas=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against the configured PostgreSQL database."""
    configuration = config.get_section(config.config_ini_section, {}) or {}
    url = config.get_main_option("sqlalchemy.url")
    if url:
        configuration["sqlalchemy.url"] = url
    else:
        from src.common.settings import Settings

        # engine_from_config passes this string to create_engine. Do not double
        # percent signs; SQLAlchemy decodes %40 in the password itself.
        configuration["sqlalchemy.url"] = Settings().database_url
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS migrations"))
        connection.commit()
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table=version_table,
            version_table_schema="migrations",
            include_schemas=True,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
