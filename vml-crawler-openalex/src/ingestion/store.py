"""PostgreSQL collection migrations, publications, and consumer reads."""

import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import Any

import sqlalchemy as sa
from alembic import command
from sqlalchemy import Connection, create_engine
from sqlalchemy.dialects import postgresql

from src.common.settings import Settings
from src.ingestion.taxonomy import canonical
from src.migrations import configuration
from src.models.raw import TaxonomyBundles, TaxonomyObservations
from src.models.tmd import TaxonomyRawFiles, TaxonomyRuns

TABLE_MODEL_MAP: dict[str, type] = {
    "attempts": TaxonomyRuns,
    "openalex_taxonomy_runs": TaxonomyRuns,
    "raw_objects": TaxonomyRawFiles,
    "openalex_taxonomy_raw_files": TaxonomyRawFiles,
}


class Catalog:
    """Own the dedicated collection namespace, never unrelated tables."""

    def __init__(self, settings: Settings) -> None:
        """Connect using masked credentials and a validated namespace."""
        self.engine = create_engine(
            settings.database_url.get_secret_value(), hide_parameters=True
        )
        self.schema_raw = settings.schema_raw
        self.schema_tmd = settings.schema_tmd
        self.schema = settings.schema_raw
        self.settings = settings

    @contextmanager
    def transaction(self) -> Iterator[Connection]:
        """Open a transaction restricted to the collection namespace."""
        with self.engine.begin() as connection:
            connection = connection.execution_options(
                schema_translate_map={"raw": self.schema_raw, "tmd": self.schema_tmd}
            )
            connection.execute(
                sa.text(
                    f'SET LOCAL search_path TO "{self.schema_raw}", "{self.schema_tmd}"'
                )
            )
            yield connection

    def migrate(self) -> None:
        """Upgrade the collection through Alembic's transactional environment."""
        config = configuration(self.settings)
        with self.engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "head")

    def insert(self, table: str, values: dict[str, Any]) -> None:
        """Register an attempt or durable raw observation."""
        if table not in TABLE_MODEL_MAP:
            raise ValueError("Unsupported collection table")
        model = TABLE_MODEL_MAP[table]
        with self.transaction() as connection:
            connection.execute(sa.insert(model).values(values))

    def fail(self, attempt: str, ended_at: datetime, error: str) -> None:
        """Keep incomplete attempts visible without replacing valid bundles."""
        with self.transaction() as connection:
            connection.execute(
                sa.update(TaxonomyRuns)
                .where(
                    sa.and_(
                        TaxonomyRuns.id == attempt,
                        TaxonomyRuns.status == "running",
                    )
                )
                .values(status="failed", ended_at=ended_at, error=error)
            )

    def publish(
        self,
        attempt: str,
        started: datetime,
        ended: datetime,
        records: dict,
        identity: str,
        dependency: str,
    ) -> None:
        """Atomically publish an immutable bundle and its observation."""
        with self.transaction() as connection:
            stmt = (
                postgresql.insert(TaxonomyBundles)
                .values(
                    id=identity,
                    classification_hash=dependency,
                    canonicalization_version=1,
                    records=json.loads(canonical(records).decode()),
                )
                .on_conflict_do_nothing()
            )
            connection.execute(stmt)
            connection.execute(
                sa.insert(TaxonomyObservations).values(
                    attempt_id=attempt,
                    bundle_id=identity,
                    started_at=started,
                    ended_at=ended,
                )
            )
            connection.execute(
                sa.update(TaxonomyRuns)
                .where(TaxonomyRuns.id == attempt)
                .values(status="complete", ended_at=ended)
            )

    def read(self) -> dict[str, Any]:
        """Read committed bundles, provenance, and incomplete attempts."""
        with self.transaction() as connection:
            attempts = [
                dict(row)
                for row in connection.execute(
                    sa.select(TaxonomyRuns.__table__).order_by(
                        TaxonomyRuns.started_at, TaxonomyRuns.id
                    )
                ).mappings()
            ]
            bundles = [
                dict(row)
                for row in connection.execute(
                    sa.select(TaxonomyBundles.__table__).order_by(TaxonomyBundles.id)
                ).mappings()
            ]
            observations = [
                dict(row)
                for row in connection.execute(
                    sa.select(TaxonomyObservations.__table__).order_by(
                        TaxonomyObservations.sequence
                    )
                ).mappings()
            ]
            raw_objects = [
                dict(row)
                for row in connection.execute(
                    sa.select(TaxonomyRawFiles.__table__).order_by(
                        TaxonomyRawFiles.sequence
                    )
                ).mappings()
            ]
            current = connection.execute(
                sa.select(TaxonomyObservations.bundle_id)
                .order_by(
                    TaxonomyObservations.ended_at.desc(),
                    TaxonomyObservations.sequence.desc(),
                )
                .limit(1)
            ).scalar()

            return {
                "attempts": attempts,
                "openalex_taxonomy_runs": attempts,
                "bundles": bundles,
                "openalex_taxonomy_bundles": bundles,
                "observations": observations,
                "openalex_taxonomy_observations": observations,
                "raw_objects": raw_objects,
                "openalex_taxonomy_raw_files": raw_objects,
                "current_bundle_id": current,
            }
