"""Consumer reads for committed snapshot ingestion state."""

import hashlib
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy import Connection
from sqlalchemy.dialects import postgresql

from src.ingestion.scope import CollectionScope
from src.ingestion.store import Catalog
from src.ingestion.taxonomy import canonical
from src.models.raw import (
    WorkCurrent,
    WorkObservations,
    WorkProcessing,
    WorkVersions,
)
from src.models.tmd import (
    WorkApiAllowances,
    WorkApiPages,
    WorkBaselines,
    WorkBatches,
    WorkChunks,
    WorkPartitions,
    WorkReleaseFiles,
    WorkReleases,
    WorkRuns,
    WorkScans,
    WorkScopes,
    WorkSources,
)


class LegacyScanRequiresBinding(ValueError):
    """A pre-scope run needs an explicit scan_id to bind its source request."""


class WorksCatalog(Catalog):
    """Extend taxonomy metadata with committed work history."""

    def start_scan(
        self, config: dict, run_id: str, observed: datetime
    ) -> tuple[str, CollectionScope]:
        """Resume a frozen scan or create independent scope coverage."""
        source = {key: value for key, value in config.items() if key != "scan_id"}
        requested = config.get("scan_id")
        with self.transaction() as connection:
            # Serialize selection and registration of scan generations.
            connection.execute(
                sa.select(
                    sa.func.pg_advisory_xact_lock(
                        sa.func.hashtext(f"{self.schema}:scan")
                    )
                )
            )
            if requested:
                scan = (
                    connection.execute(
                        sa.select(WorkScans).where(WorkScans.id == requested)
                    )
                    .mappings()
                    .one()
                )
                if (
                    scan["request_key"].startswith("legacy:")
                    and not scan["source_request"]
                ):
                    retained_sources = (
                        connection.execute(
                            sa.select(
                                WorkSources.release,
                                WorkSources.source,
                                WorkSources.bytes,
                                WorkSources.rows,
                                WorkSources.sha256,
                            )
                            .distinct()
                            .join(
                                WorkObservations,
                                WorkObservations.source_id == WorkSources.id,
                            )
                            .join(WorkRuns, WorkRuns.id == WorkObservations.run_id)
                            .where(WorkRuns.scan_id == scan["id"])
                        )
                        .mappings()
                        .all()
                    )
                    for retained in retained_sources:
                        if (
                            any(
                                source[key] != retained[column]
                                for key, column in (
                                    ("release", "release"),
                                    ("source", "source"),
                                    ("size", "bytes"),
                                    ("rows", "rows"),
                                )
                            )
                            or source["sha256"]
                            and source["sha256"] != retained["sha256"]
                        ):
                            raise ValueError(
                                "Legacy source does not match retained evidence"
                            )
                    policy = dict(
                        connection.execute(
                            sa.select(WorkScopes.definition).where(
                                WorkScopes.id == scan["scope_id"]
                            )
                        ).scalar_one()
                    )
                    policy["publication_through"] = (
                        None  # Revision 0002 always used runtime UTC date.
                    )
                    request_key = hashlib.sha256(
                        canonical([source, policy])
                    ).hexdigest()
                    connection.execute(
                        sa.update(WorkScans)
                        .where(WorkScans.id == scan["id"])
                        .values(
                            request_key=request_key,
                            source_request=source,
                        )
                    )
                elif scan["source_request"] != source:
                    raise ValueError("A scan cannot change its source request")
            else:
                scope = CollectionScope(
                    publication_from=self.settings.publication_from,
                    publication_through=self.settings.publication_through
                    or observed.date(),
                    domain_ids=self.settings.domain_ids,
                    field_ids=self.settings.field_ids,
                    exclude_domain_ids=self.settings.exclude_domain_ids,
                    exclude_field_ids=self.settings.exclude_field_ids,
                )
                policy = scope.model_dump(mode="json")
                if self.settings.publication_through is None:
                    policy["publication_through"] = None
                request_key = hashlib.sha256(canonical([source, policy])).hexdigest()
                scan = (
                    connection.execute(
                        sa.select(WorkScans)
                        .where(
                            WorkScans.request_key == request_key,
                            WorkScans.status != "complete",
                        )
                        .order_by(WorkScans.id)
                        .limit(1)
                    )
                    .mappings()
                    .first()
                )
                if scan is None:
                    legacy = connection.execute(
                        sa.select(WorkScopes.definition)
                        .select_from(WorkScans)
                        .join(WorkScopes, WorkScopes.id == WorkScans.scope_id)
                        .where(
                            WorkScans.request_key.like("legacy:%"),
                            WorkScans.status != "complete",
                        )
                    ).scalars()
                    for definition in legacy:
                        previous = dict(definition)
                        previous.setdefault("exclude_domain_ids", [])
                        previous.setdefault("exclude_field_ids", [])
                        if policy["publication_through"] is None:
                            previous["publication_through"] = None
                        if previous == policy:
                            raise LegacyScanRequiresBinding(
                                "Supply the migrated scan_id to bind its source"
                            )
                    connection.execute(
                        postgresql.insert(WorkScopes)
                        .values(
                            id=scope.identity,
                            definition=scope.model_dump(mode="json"),
                        )
                        .on_conflict_do_nothing()
                    )
                    scan = {"id": str(uuid4()), "scope_id": scope.identity}
                    connection.execute(
                        sa.insert(WorkScans).values(
                            id=scan["id"],
                            scope_id=scope.identity,
                            request_key=request_key,
                            source_request=source,
                            status="running",
                        )
                    )
            definition = connection.execute(
                sa.select(WorkScopes.definition).where(
                    WorkScopes.id == scan["scope_id"]
                )
            ).scalar_one()
            scope = CollectionScope.model_validate(definition)
            connection.execute(
                sa.insert(WorkRuns).values(
                    id=run_id,
                    observed_at=observed,
                    lower_date=scope.publication_from,
                    upper_date=scope.publication_through,
                    status="running",
                    scope_id=scan["scope_id"],
                    scan_id=scan["id"],
                )
            )
            return str(scan["id"]), scope

    def reconciliation(
        self,
        connection: Connection,
        scope_id: str | None = None,
        entities: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Resolve observations as a set, retaining conflicting version identities.

        Date-only and timezone-free source values use UTC. Discard comparable
        timestamps below the maximum before applying fallback precedence. This
        prevents missing dates from making selection depend on database row order.
        """
        p_sub = (
            sa.select(sa.func.max(WorkApiPages.ended_at).label("observed_at"))
            .where(
                WorkApiPages.run_id == WorkObservations.run_id,
                WorkApiPages.source_id == WorkObservations.source_id,
            )
            .correlate(WorkObservations)
            .subquery()
            .lateral("p")
        )
        stmt = (
            sa.select(
                WorkRuns.scope_id,
                WorkVersions.entity_id,
                WorkObservations.version_id,
                WorkVersions.payload["updated_date"].astext.label("updated_date"),
                WorkSources.release.label("release_kind"),
                sa.func.coalesce(p_sub.c.observed_at, WorkRuns.observed_at).label(
                    "observed_at"
                ),
            )
            .select_from(WorkObservations)
            .join(WorkVersions, WorkVersions.id == WorkObservations.version_id)
            .join(WorkRuns, WorkRuns.id == WorkObservations.run_id)
            .join(WorkSources, WorkSources.id == WorkObservations.source_id)
            .outerjoin(p_sub, sa.true())
        )
        if scope_id is not None:
            stmt = stmt.where(WorkRuns.scope_id == scope_id)
        if entities is not None:
            stmt = stmt.where(WorkVersions.entity_id.in_(entities))

        rows = connection.execute(stmt).mappings()
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            item = dict(row)
            try:
                timestamp = datetime.fromisoformat(item["updated_date"])
                item["timestamp"] = (
                    timestamp.replace(tzinfo=UTC)
                    if timestamp.tzinfo is None
                    else timestamp.astimezone(UTC)
                )
            except TypeError, ValueError, OverflowError:
                item["timestamp"] = None
            grouped[(row["scope_id"], row["entity_id"])].append(item)
        reports = []
        for (scope, entity), observations in sorted(grouped.items()):
            comparable = [
                row["timestamp"] for row in observations if row["timestamp"] is not None
            ]
            greatest = max(comparable) if comparable else None
            candidates = [
                row
                for row in observations
                if row["timestamp"] is None or row["timestamp"] == greatest
            ]
            best = max(
                candidates,
                key=lambda row: (
                    row["release_kind"] == "api",
                    row["observed_at"],
                    row["version_id"],
                ),
            )
            versions = sorted({row["version_id"] for row in observations})
            reports.append(
                {
                    "scope_id": scope,
                    "entity_id": entity,
                    "version_id": best["version_id"],
                    "version_ids": versions,
                    "conflict": len(versions) > 1,
                    "uncertain": any(row["timestamp"] is None for row in observations),
                }
            )
        return reports

    def update_current_selection(
        self, connection: Connection, scope_id: str, entities: list[str]
    ) -> None:
        """Publish one deterministic selection per scope and work."""
        for report in self.reconciliation(connection, scope_id, entities):
            stmt = (
                postgresql.insert(WorkCurrent)
                .values(
                    entity_id=report["entity_id"],
                    version_id=report["version_id"],
                    scope_id=report["scope_id"],
                )
                .on_conflict_do_update(
                    index_elements=[WorkCurrent.scope_id, WorkCurrent.entity_id],
                    set_={"version_id": report["version_id"]},
                )
            )
            connection.execute(stmt)

    def read_works(self, scope_id: str | None = None) -> dict[str, Any]:
        """Expose scoped coverage and selections alongside shared retained history."""
        table_models = {
            "scopes": WorkScopes,
            "scans": WorkScans,
            "runs": WorkRuns,
            "sources": WorkSources,
            "versions": WorkVersions,
            "observations": WorkObservations,
            "batches": WorkBatches,
            "processing": WorkProcessing,
            "current": WorkCurrent,
            "releases": WorkReleases,
            "release_files": WorkReleaseFiles,
            "baselines": WorkBaselines,
            "chunks": WorkChunks,
            "partitions": WorkPartitions,
            "api_pages": WorkApiPages,
            "api_allowances": WorkApiAllowances,
        }
        with self.transaction() as connection:
            connection.execute(
                sa.text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            )
            result = {
                name: [
                    dict(row) for row in connection.execute(sa.select(model)).mappings()
                ]
                for name, model in table_models.items()
            }
            if scope_id is not None:
                for name in ("runs", "scans", "current", "baselines"):
                    result[name] = [
                        row for row in result[name] if row["scope_id"] == scope_id
                    ]
                result["scopes"] = [
                    row for row in result["scopes"] if row["id"] == scope_id
                ]
                result["batches"] = [
                    row
                    for row in result["batches"]
                    if row["manifest"].get("scope_id") == scope_id
                ]
                runs = {row["id"] for row in result["runs"]}
                result["observations"] = [
                    row for row in result["observations"] if row["run_id"] in runs
                ]
                baselines = {row["id"] for row in result["baselines"]}
                result["chunks"] = [
                    row for row in result["chunks"] if row["baseline_id"] in baselines
                ]
            # File completion derives only from committed chunks in this baseline.
            file_cov_stmt = (
                sa.select(
                    WorkBaselines.id.label("baseline_id"),
                    WorkReleaseFiles.id.label("file_id"),
                    (WorkReleaseFiles.source_id.is_not(None)).label("acquired"),
                    sa.or_(
                        WorkBaselines.selection_status == "complete",
                        sa.and_(
                            WorkReleaseFiles.source_id.is_not(None),
                            sa.func.coalesce(sa.func.sum(WorkChunks.row_count), 0)
                            == sa.cast(
                                WorkReleaseFiles.inventory["meta"][
                                    "record_count"
                                ].astext,
                                sa.BigInteger,
                            ),
                            sa.func.count(WorkChunks.id) > 0,
                        ),
                    ).label("processed"),
                )
                .select_from(WorkBaselines)
                .join(
                    WorkReleaseFiles,
                    WorkReleaseFiles.release_id == WorkBaselines.release_id,
                )
                .outerjoin(
                    WorkChunks,
                    sa.and_(
                        WorkChunks.baseline_id == WorkBaselines.id,
                        WorkChunks.file_id == WorkReleaseFiles.id,
                    ),
                )
                .group_by(WorkBaselines.id, WorkReleaseFiles.id)
            )
            if scope_id is not None:
                file_cov_stmt = file_cov_stmt.where(WorkBaselines.scope_id == scope_id)
            result["file_coverage"] = [
                dict(row) for row in connection.execute(file_cov_stmt).mappings()
            ]
            result["quarantine"] = [
                row
                for row in result["observations"]
                if row["disposition"].startswith("quarantine:")
            ]
            result["reconciliation"] = self.reconciliation(connection, scope_id)
            return result
