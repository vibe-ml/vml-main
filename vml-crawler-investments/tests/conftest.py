"""Isolated PostgreSQL schemas with a fake social profile namespace."""

import os
from collections.abc import Iterator
from dataclasses import dataclass
from uuid import uuid4

import pytest
import sqlalchemy as sa
from pydantic import SecretStr

from src.models import social


@dataclass(frozen=True)
class Database:
    """Connection URL and disposable namespaces for one test."""

    url: SecretStr
    schema_investments: str
    schema_social: str
    alembic_name: str


@pytest.fixture
def database() -> Iterator[Database]:
    """Create disposable investment and social schemas on TEST_DATABASE_URL."""
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is not set")
    uid = uuid4().hex[:12]
    db = Database(
        SecretStr(url), f"investments_{uid}", f"social_src_{uid}", f"test_{uid}"
    )
    engine = sa.create_engine(url)
    with engine.begin() as connection:
        connection.execute(sa.schema.CreateSchema(db.schema_social))
        social.metadata.create_all(
            connection.execution_options(
                schema_translate_map={"social_src": db.schema_social}
            )
        )
    yield db
    with engine.begin() as connection:
        for schema in (db.schema_investments, db.schema_social):
            connection.execute(sa.text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        sa.Table("alembic_" + db.alembic_name, sa.MetaData(), schema="migrations").drop(
            connection, checkfirst=True
        )
    engine.dispose()
