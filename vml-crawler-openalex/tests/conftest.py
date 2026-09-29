"""Isolated PostgreSQL and source fixtures for ingestion jobs."""

import json
import os
import subprocess
from collections.abc import Iterator
from uuid import uuid4

import pytest
from pydantic import SecretStr
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.engine import URL


@pytest.fixture
def database(monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[SecretStr, str]]:
    """Create only a disposable collection schema on the existing server."""
    url: str | URL | None = os.environ.get("TEST_DATABASE_URL")
    if not url:
        info = json.loads(
            subprocess.check_output(["docker", "inspect", "local-db-pg"])
        )[0]
        env = dict(
            value.split("=", 1) for value in info["Config"]["Env"] if "=" in value
        )
        url = URL.create(
            "postgresql+psycopg",
            username=env.get("POSTGRES_USER", "postgres"),
            password=env.get("POSTGRES_PASSWORD"),
            host="localhost",
            port=5432,
            database=env.get("POSTGRES_DB", "local_vml"),
        )
    engine = create_engine(url)
    uid = uuid4().hex[:16]
    schema_raw = f"raw_{uid}"
    schema_tmd = f"tmd_{uid}"
    alembic_name = f"test_{uid}"
    monkeypatch.setenv("OPENALEX_SCHEMA_RAW", schema_raw)
    monkeypatch.setenv("OPENALEX_SCHEMA_TMD", schema_tmd)
    monkeypatch.setenv("OPENALEX_ALEMBIC_NAME", alembic_name)
    yield SecretStr(engine.url.render_as_string(hide_password=False)), schema_raw
    with engine.begin() as connection:
        connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_raw}" CASCADE'))
        connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_tmd}" CASCADE'))
        Table("alembic_" + alembic_name, MetaData(), schema="migrations").drop(
            connection, checkfirst=True
        )
    engine.dispose()
