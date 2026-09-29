"""Shared fixtures: the local `pwf_test` database with crawler-shaped upstream tables."""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import (
    Column,
    Engine,
    ForeignKey,
    Integer,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    insert,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.schema import CreateSchema, DropSchema

from src.common.db import create_processor_engine
from src.models.emergence import EmergenceObservation
from src.models.labels import TopicLabel, WorkSummary
from src.models.topics import (
    DiscoveryTopic,
    TopicRun,
    TopicRunMapping,
    WorkTopicAssignment,
)

_REPO_ROOT = Path(__file__).resolve().parents[1]
_PROCESSOR_TABLES = (
    EmergenceObservation.__table__,
    TopicLabel.__table__,
    WorkSummary.__table__,
    WorkTopicAssignment.__table__,
    TopicRunMapping.__table__,
    DiscoveryTopic.__table__,
    TopicRun.__table__,
)


class TestSettings(BaseSettings):
    """Local test service URLs. Tests never read the production database URL."""

    __test__ = False
    model_config = SettingsConfigDict(
        env_prefix="PWF_TEST_", env_file=".env", extra="ignore"
    )

    database_url: str
    qdrant_url: str


def _crawler_tables(schema: str) -> tuple[MetaData, Table, Table]:
    """Mirror the crawler's DDL for the two tables corpus selection reads.

    Kept independent of `src.models.upstream` so a wrong processor declaration fails tests.
    """
    metadata = MetaData(schema=schema)
    versions = Table(
        "openalex_work_versions",
        metadata,
        Column("id", Text(), primary_key=True),
        Column("entity_id", Text(), nullable=False),
        Column("canonicalization_version", Integer(), nullable=False),
        Column("content_hash", Text(), nullable=False),
        Column("payload", JSONB(), nullable=False),
        UniqueConstraint("entity_id", "canonicalization_version", "content_hash"),
    )
    current = Table(
        "openalex_work_current",
        metadata,
        Column("scope_id", Text(), primary_key=True),
        Column("entity_id", Text(), primary_key=True),
        Column("version_id", Text(), ForeignKey(versions.c.id), nullable=False),
    )
    return metadata, versions, current


class UpstreamFixture:
    """Writes crawler rows into an isolated schema and exposes an engine that reads it."""

    def __init__(self, admin: Engine, database_url: str, schema: str) -> None:
        self.schema = schema
        self._admin = admin
        self.metadata, self._versions, self._current = _crawler_tables(schema)
        self.engine = create_processor_engine(database_url, upstream_schema=schema)

    def add_version(self, work_id: str, payload: dict[str, Any]) -> str:
        """Store one immutable work version and return its version ID."""
        content = json.dumps(payload, sort_keys=True)
        content_hash = hashlib.sha256(content.encode()).hexdigest()
        version_id = hashlib.sha256(f"{work_id}|{content_hash}".encode()).hexdigest()
        with self._admin.begin() as connection:
            connection.execute(
                insert(self._versions).values(
                    id=version_id,
                    entity_id=work_id,
                    canonicalization_version=2,
                    content_hash=content_hash,
                    payload=payload,
                )
            )
        return version_id

    def set_current(self, scope_id: str, work_id: str, version_id: str) -> None:
        """Mark a version as the current one for a work within a scope."""
        with self._admin.begin() as connection:
            connection.execute(
                insert(self._current).values(
                    scope_id=scope_id, entity_id=work_id, version_id=version_id
                )
            )

    def add_work(self, scope_id: str, payload: dict[str, Any]) -> str:
        """Store a version of `payload` and make it current in `scope_id`."""
        version_id = self.add_version(payload["id"], payload)
        self.set_current(scope_id, payload["id"], version_id)
        return version_id

    def payload_of(self, version_id: str) -> dict[str, Any]:
        """Read a stored payload back, bypassing the processor."""
        with self._admin.connect() as connection:
            return connection.execute(
                self._versions.select()
                .with_only_columns(self._versions.c.payload)
                .where(self._versions.c.id == version_id)
            ).scalar_one()


@pytest.fixture(scope="session")
def test_settings() -> TestSettings:
    """Load the test service URLs once per session."""
    return TestSettings()


@pytest.fixture(scope="session")
def admin_engine(test_settings: TestSettings) -> Iterator[Engine]:
    """Engine that writes fixture rows, outside the processor's read-only path."""
    engine = create_engine(test_settings.database_url)
    yield engine
    engine.dispose()


@pytest.fixture
def upstream(
    admin_engine: Engine, test_settings: TestSettings
) -> Iterator[UpstreamFixture]:
    """Crawler tables in a schema unique to this test, dropped afterwards."""
    schema = f"raw_test_{uuid.uuid4().hex[:12]}"
    with admin_engine.begin() as connection:
        connection.execute(CreateSchema(schema))
    fixture = UpstreamFixture(admin_engine, test_settings.database_url, schema)
    fixture.metadata.create_all(admin_engine)
    try:
        yield fixture
    finally:
        fixture.engine.dispose()
        with admin_engine.begin() as connection:
            connection.execute(DropSchema(schema, cascade=True))


def _alembic_config(database_url: str) -> Config:
    """Build an Alembic config pointed at the processor migration tree."""
    config = Config(str(_REPO_ROOT / "alembic.ini"))
    # ConfigParser treats "%" as interpolation, so percent-encoded passwords need escaping.
    config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    return config


@pytest.fixture(scope="session")
def _migrated_processor_engine(test_settings: TestSettings) -> Iterator[Engine]:
    """Apply processor migrations once per session on `pwf_test`."""
    engine = create_engine(test_settings.database_url, pool_pre_ping=True)
    config = _alembic_config(test_settings.database_url)
    command.upgrade(config, "head")
    try:
        yield engine
    finally:
        command.downgrade(config, "base")
        with engine.begin() as connection:
            connection.execute(text("DROP TABLE IF EXISTS migrations.alembic_bertopic"))
        engine.dispose()


@pytest.fixture
def processor_engine(_migrated_processor_engine: Engine) -> Iterator[Engine]:
    """Processor engine with topic-run tables truncated for this test."""
    with _migrated_processor_engine.begin() as connection:
        for table in _PROCESSOR_TABLES:
            connection.execute(table.delete())
    yield _migrated_processor_engine
