"""Publish a bounded, byte-preserving snapshot sample through a Dagster job.

Source mapping v1: each Parquet row is one work object. DuckDB's JSON row
encoding retains column names, nested values, nulls, and unknown fields. JSON
numbers use canonicalization v2; SQL dates become ISO strings. Arrays keep
source order. Abstract reconstruction exists only in derived payloads.
"""

import hashlib
import json
import os
import tempfile
from collections.abc import Iterator
from datetime import UTC, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import duckdb
import httpx
import sqlalchemy as sa
from dagster import Failure, Field, OpExecutionContext, job, op
from sqlalchemy import Connection
from sqlalchemy.dialects import postgresql

from src.common.log import logger
from src.ingestion.job import Runtime, runtime_resource
from src.ingestion.raw import durable_directory, sync_directory
from src.ingestion.taxonomy import canonical
from src.ingestion.works_store import WorksCatalog
from src.models.raw import (
    WorkCurrent,
    WorkObservations,
    WorkProcessing,
    WorkVersions,
)
from src.models.tmd import (
    WorkBatchClaims,
    WorkBatches,
    WorkRuns,
    WorkScans,
    WorkScopes,
    WorkSources,
)

CANONICALIZATION = 2
TRANSFORMATION = "snapshot-abstract-v1"
MAX_BYTES = 64 * 1024 * 1024
MAX_ROWS = 10000
MAX_RECORD_BYTES = 1024 * 1024
MAX_EXPANDED_BYTES = 16 * 1024 * 1024


def digest(value: object) -> str:
    """Hash a canonical identity payload."""
    return hashlib.sha256(canonical(value)).hexdigest()


def work_identity(record: dict) -> tuple[str, str]:
    """Return canonical content and work-version identities for snapshot rows."""
    content_hash = digest(record)
    return content_hash, digest(["works", record["id"], CANONICALIZATION, content_hash])


def publish_file(path: Path, destination: Path) -> None:
    """Flush and atomically link an immutable file into durable storage."""
    durable_directory(destination.parent)
    with path.open("rb") as stream:
        os.fsync(stream.fileno())
    try:
        os.link(path, destination)
    except FileExistsError:
        with path.open("rb") as left, destination.open("rb") as right:
            if (
                hashlib.file_digest(left, "sha256").digest()
                != hashlib.file_digest(right, "sha256").digest()
            ):
                raise ValueError("Immutable output conflict") from None
    sync_directory(destination.parent)
    path.unlink()
    sync_directory(path.parent)


def derived(record: dict) -> dict:
    """Reconstruct supplied inverted abstracts without altering raw evidence."""
    inverted = record.get("abstract_inverted_index")
    if isinstance(inverted, str):
        try:
            inverted = json.loads(inverted)
        except ValueError, TypeError:
            inverted = None
    abstract = None
    if isinstance(inverted, dict):
        words = sorted(
            (position, word)
            for word, positions in inverted.items()
            if isinstance(positions, list)
            for position in positions
            if isinstance(position, int)
        )
        abstract = " ".join(word for _, word in words)
    return record | {"abstract": abstract}


def acquire_source(
    runtime: Runtime, config: dict, run_id: str
) -> tuple[Path, str, int]:
    """Preserve a bounded original source and validate transport integrity."""
    root = runtime.settings.storage_root
    if not 0 < config["size"] <= MAX_BYTES or not 0 <= config["rows"] <= MAX_ROWS:
        raise ValueError("Source exceeds bounded sample limits")
    staging = root / "staging"
    durable_directory(staging)
    temporary = staging / (run_id + ".parquet")
    checksum = hashlib.sha256()
    size = 0
    with (
        httpx.Client(
            transport=runtime.transport, timeout=runtime.settings.timeout_seconds
        ) as client,
        client.stream("GET", config["source"]) as response,
        temporary.open("xb") as stream,
    ):
        response.raise_for_status()
        for chunk in response.iter_bytes():
            size += len(chunk)
            if size > MAX_BYTES or size > config["size"]:
                raise ValueError("Source size exceeded")
            checksum.update(chunk)
            stream.write(chunk)
    actual = checksum.hexdigest()
    # Keep original Parquet, including excluded and quarantined rows.
    raw_path = root / "raw" / "snapshot" / (actual + ".parquet")
    runtime.failure("before_snapshot_raw_publish")
    publish_file(temporary, raw_path)
    runtime.failure("after_snapshot_raw_publish")
    if size != config["size"] or (config["sha256"] and actual != config["sha256"]):
        raise ValueError("Source integrity mismatch")
    return raw_path, actual, size


def read_records(duck: duckdb.DuckDBPyConnection, path: Path) -> Iterator[dict]:
    """Bound expanded data in DuckDB before decoding any rows into Python."""
    total, largest = duck.execute(
        "SELECT coalesce(sum(size), 0), coalesce(max(size), 0) FROM "
        "(SELECT octet_length(encode(to_json(t))) AS size FROM read_parquet(?) t)",
        [str(path)],
    ).fetchall()[0]
    if total > MAX_EXPANDED_BYTES or largest > MAX_RECORD_BYTES:
        raise ValueError("Expanded source exceeds bounded sample limits")
    cursor = duck.execute("SELECT to_json(t) FROM read_parquet(?) t", [str(path)])
    while row := cursor.fetchone():
        yield json.loads(row[0], parse_float=Decimal)


def encode_output(
    duck: duckdb.DuckDBPyConnection, pending: dict[str, dict], root: Path, run_id: str
) -> tuple[Path, str]:
    """Validate and publish immutable derived Parquet before metadata commit."""
    staging = root / "staging"
    duck.execute(
        "CREATE TABLE selected(entity_id VARCHAR, version_id VARCHAR, payload JSON)"
    )
    if pending:
        duck.executemany(
            "INSERT INTO selected VALUES (?, ?, ?)",
            [
                (
                    record["id"],
                    version,
                    canonical(derived(record)).decode(),
                )
                for version, record in pending.items()
            ],
        )
    output = staging / (run_id + "-derived.parquet")
    duck.execute(
        "COPY selected TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
        [str(output)],
    )
    if duck.execute("SELECT count(*) FROM read_parquet(?)", [str(output)]).fetchall()[
        0
    ][0] != len(pending):
        raise ValueError("Output count mismatch")
    output_hash = hashlib.sha256(output.read_bytes()).hexdigest()
    destination = root / "derived" / (output_hash + ".parquet")
    publish_file(output, destination)
    return destination, output_hash


def stage_version(connection: Connection, record: dict) -> str:
    """Register a selected version."""
    content_hash, version = work_identity(record)
    clean_payload = json.loads(canonical(record).decode())
    connection.execute(
        postgresql.insert(WorkVersions)
        .values(
            id=version,
            entity_id=record["id"],
            canonicalization_version=CANONICALIZATION,
            content_hash=content_hash,
            payload=clean_payload,
        )
        .on_conflict_do_nothing()
    )
    return version


def register_processing(
    connection: Connection, pending: dict[str, dict], dependency: str, batch_id: str
) -> None:
    """Acknowledge transformations in the caller's publication transaction."""
    for version in pending:
        connection.execute(
            postgresql.insert(WorkProcessing)
            .values(
                version_id=version,
                transformation=TRANSFORMATION,
                dependency=dependency,
                batch_id=batch_id,
            )
            .on_conflict_do_nothing()
        )


@op(
    required_resource_keys={"runtime"},
    config_schema={
        "release": str,
        "source": str,
        "size": int,
        "rows": int,
        "sha256": Field(str, default_value=""),
        "scan_id": Field(str, default_value=""),
    },
)
def collect_snapshot(context: OpExecutionContext) -> str:
    """Validate a bounded source and atomically publish its selected works."""
    runtime: Runtime = context.resources.runtime
    config = context.op_config
    catalog = WorksCatalog(runtime.settings)
    root = runtime.settings.storage_root
    run_id = str(uuid4())
    observed = runtime.now().astimezone(UTC)
    scan_id = None
    try:
        catalog.migrate()
        scan_id, scope = catalog.start_scan(config, run_id, observed)
        raw_path, actual, size = acquire_source(runtime, config, run_id)
        source_id = digest([config["release"], config["source"], actual])
        with duckdb.connect(
            config={
                "memory_limit": "256MB",
                "threads": "1",
                "temp_directory": tempfile.gettempdir(),
            }
        ) as duck:
            count = duck.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(raw_path)]
            ).fetchall()[0][0]
            if count != config["rows"] or count > MAX_ROWS:
                raise ValueError("Source row count mismatch")
            source = {
                "id": source_id,
                "release": config["release"],
                "source": config["source"],
                "path": str(raw_path.relative_to(root)),
                "sha256": actual,
                "bytes": size,
                "rows": count,
            }
            with catalog.transaction() as connection:
                connection.execute(
                    postgresql.insert(WorkSources)
                    .values(source)
                    .on_conflict_do_nothing()
                )
            taxonomy = catalog.read()
            taxonomy_id = taxonomy["current_bundle_id"]
            batch_id = digest(
                [
                    source_id,
                    scope.identity,
                    TRANSFORMATION,
                    taxonomy_id,
                ]
            )
            # Serialize bounded publishers; metadata and consumer visibility commit together.
            with catalog.transaction() as connection:
                connection.execute(
                    sa.select(
                        sa.func.pg_advisory_xact_lock(
                            sa.func.hashtext(f"{catalog.schema}:snapshot")
                        )
                    )
                )
                selected = []
                for number, record in enumerate(read_records(duck, raw_path)):
                    status = scope.disposition(record)
                    version = None
                    if status == "selected":
                        version = stage_version(connection, record)
                        selected.append((record["id"], version))
                    connection.execute(
                        sa.insert(WorkObservations).values(
                            run_id=run_id,
                            row_number=number,
                            source_id=source_id,
                            version_id=version,
                            disposition=status,
                        )
                    )
                if taxonomy_id is None:
                    connection.execute(
                        sa.update(WorkRuns)
                        .where(WorkRuns.id == run_id)
                        .values(status="waiting_taxonomy")
                    )
                    connection.execute(
                        sa.update(WorkScans)
                        .where(
                            WorkScans.id == scan_id,
                            WorkScans.status != "complete",
                        )
                        .values(status="waiting_taxonomy")
                    )
                    return "waiting_taxonomy"

                for entity, version in selected:
                    connection.execute(
                        postgresql.insert(WorkCurrent)
                        .values(
                            entity_id=entity,
                            version_id=version,
                            scope_id=scope.identity,
                        )
                        .on_conflict_do_update(
                            index_elements=[
                                WorkCurrent.scope_id,
                                WorkCurrent.entity_id,
                            ],
                            set_={"version_id": version},
                        )
                    )
                entities = list({entity for entity, _ in selected})
                catalog.update_current_selection(connection, scope.identity, entities)
                connection.execute(
                    sa.update(WorkRuns)
                    .where(WorkRuns.id == run_id)
                    .values(status="complete")
                )
                connection.execute(
                    sa.update(WorkScans)
                    .where(WorkScans.id == scan_id)
                    .values(status="complete", next_row=count)
                )
                runtime.failure("before_snapshot_commit")
            runtime.failure("after_snapshot_commit")
        return batch_id
    except Exception as error:  # noqa: BLE001 - sanitize errors at the job boundary
        with catalog.transaction() as connection:
            connection.execute(
                sa.update(WorkRuns)
                .where(
                    WorkRuns.id == run_id,
                    WorkRuns.status == "running",
                )
                .values(status="failed")
            )
            if scan_id is not None:
                connection.execute(
                    sa.update(WorkScans)
                    .where(
                        WorkScans.id == scan_id,
                        WorkScans.status != "complete",
                    )
                    .values(status="failed")
                )
        logger.error("snapshot_failed", run_id=run_id, error_type=type(error).__name__)
        raise Failure(
            f"Snapshot run {run_id} failed ({type(error).__name__})"
        ) from None
    finally:
        catalog.engine.dispose()


@op(required_resource_keys={"runtime"})
def process_batches(context: OpExecutionContext, _dependency: str = "") -> None:
    """Process claimed batches and write immutable output."""
    runtime: Runtime = context.resources.runtime
    catalog = WorksCatalog(runtime.settings)
    root = runtime.settings.storage_root

    with catalog.transaction() as connection:
        # Get taxonomy dependency
        taxonomy = catalog.read()
        taxonomy_id = taxonomy["current_bundle_id"]
        if taxonomy_id is None:
            return

        dependency = next(
            (
                bundle["classification_hash"]
                for bundle in taxonomy["bundles"]
                if bundle["id"] == taxonomy_id
            ),
            "",
        )

        # Find or enqueue claims for new taxonomy dependency
        sub = sa.select(1).where(
            WorkBatchClaims.source_id == WorkObservations.source_id,
            WorkBatchClaims.scope_id == WorkRuns.scope_id,
            WorkBatchClaims.transformation == TRANSFORMATION,
            WorkBatchClaims.taxonomy_id == taxonomy_id,
        )
        claim_id_expr = sa.func.md5(
            WorkObservations.source_id
            + WorkRuns.scope_id
            + TRANSFORMATION
            + taxonomy_id
        )
        sel = (
            sa.select(
                claim_id_expr.label("id"),
                WorkObservations.source_id,
                WorkRuns.scope_id,
                sa.literal(TRANSFORMATION).label("transformation"),
                sa.literal(dependency).label("dependency"),
                sa.literal(taxonomy_id).label("taxonomy_id"),
                sa.literal("pending").label("status"),
            )
            .select_from(WorkObservations)
            .join(WorkRuns, WorkRuns.id == WorkObservations.run_id)
            .where(~sub.exists())
            .distinct()
        )
        connection.execute(
            sa.insert(WorkBatchClaims).from_select(
                [
                    "id",
                    "source_id",
                    "scope_id",
                    "transformation",
                    "dependency",
                    "taxonomy_id",
                    "status",
                ],
                sel,
            )
        )

    while True:
        with catalog.transaction() as connection:
            # Claim a pending batch
            now = runtime.now().astimezone(UTC)
            claim_sub = (
                sa.select(WorkBatchClaims.id)
                .where(
                    sa.or_(
                        WorkBatchClaims.status == "pending",
                        sa.and_(
                            WorkBatchClaims.status == "running",
                            WorkBatchClaims.claimed_at < now - timedelta(minutes=5),
                        ),
                    )
                )
                .limit(1)
                .with_for_update(skip_locked=True)
                .scalar_subquery()
            )
            claim = (
                connection.execute(
                    sa.update(WorkBatchClaims)
                    .where(WorkBatchClaims.id == claim_sub)
                    .values(status="running", claimed_at=now)
                    .returning(
                        WorkBatchClaims.id,
                        WorkBatchClaims.source_id,
                        WorkBatchClaims.scope_id,
                        WorkBatchClaims.transformation,
                        WorkBatchClaims.dependency,
                        WorkBatchClaims.taxonomy_id,
                        WorkBatchClaims.status,
                        WorkBatchClaims.claimed_at,
                    )
                )
                .mappings()
                .first()
            )

        if not claim:
            break

        # Process the claim
        run_id = str(uuid4())
        try:
            with (
                duckdb.connect(
                    config={
                        "memory_limit": "256MB",
                        "threads": "1",
                        "temp_directory": tempfile.gettempdir(),
                    }
                ) as duck,
                catalog.transaction() as connection,
            ):
                connection.execute(
                    sa.select(
                        sa.func.pg_advisory_xact_lock(
                            sa.func.hashtext(f"{catalog.schema}:snapshot")
                        )
                    )
                )

                source = (
                    connection.execute(
                        sa.select(WorkSources).where(
                            WorkSources.id == claim["source_id"]
                        )
                    )
                    .mappings()
                    .one()
                )
                scope_def = connection.execute(
                    sa.select(WorkScopes.definition).where(
                        WorkScopes.id == claim["scope_id"]
                    )
                ).scalar_one()

                from src.ingestion.scope import CollectionScope

                scope = CollectionScope.model_validate(scope_def)

                # Find the records that need processing
                proc_sub = sa.select(1).where(
                    WorkProcessing.version_id == WorkObservations.version_id,
                    WorkProcessing.transformation == TRANSFORMATION,
                    WorkProcessing.dependency == claim["dependency"],
                )
                stmt = (
                    sa.select(WorkObservations.version_id, WorkVersions.payload)
                    .select_from(WorkObservations)
                    .join(WorkRuns, WorkRuns.id == WorkObservations.run_id)
                    .join(WorkVersions, WorkVersions.id == WorkObservations.version_id)
                    .where(
                        WorkObservations.source_id == claim["source_id"],
                        WorkRuns.scope_id == claim["scope_id"],
                        WorkObservations.disposition == "selected",
                        ~proc_sub.exists(),
                    )
                )
                rows = connection.execute(stmt).fetchall()

                pending = {
                    row[0]: row[1] if isinstance(row[1], dict) else json.loads(row[1])
                    for row in rows
                }

                if pending:
                    destination, output_hash = encode_output(
                        duck, pending, root, run_id
                    )
                else:
                    destination, output_hash = encode_output(duck, {}, root, run_id)

                selected_count = connection.execute(
                    sa.select(sa.func.count())
                    .select_from(WorkObservations)
                    .join(WorkRuns, WorkRuns.id == WorkObservations.run_id)
                    .where(
                        WorkObservations.source_id == claim["source_id"],
                        WorkRuns.scope_id == claim["scope_id"],
                        WorkObservations.disposition == "selected",
                    )
                ).scalar()

                manifest = {
                    "source_batch_ids": [source["id"]],
                    "transformation": TRANSFORMATION,
                    "taxonomy_snapshot_id": claim["taxonomy_id"],
                    "taxonomy_classification_hash": claim["dependency"],
                    "scope_id": scope.identity,
                    "api_parameters": scope.api_parameters(),
                    "selected_count": selected_count,
                    "lower_date": scope.publication_from.isoformat(),
                    "upper_date": scope.publication_through.isoformat(),
                    "path": str(destination.relative_to(root)),
                    "sha256": output_hash,
                    "count": len(pending),
                    "source_count": source["rows"],
                    "source_sha256": source["sha256"],
                }

                batch_id = digest(
                    [
                        claim["source_id"],
                        claim["scope_id"],
                        TRANSFORMATION,
                        claim["taxonomy_id"],
                    ]
                )

                runtime.failure("after_snapshot_output_publish")
                connection.execute(
                    postgresql.insert(WorkBatches)
                    .values(
                        id=batch_id,
                        taxonomy_id=claim["taxonomy_id"],
                        manifest=manifest,
                    )
                    .on_conflict_do_nothing()
                )
                register_processing(connection, pending, claim["dependency"], batch_id)

                connection.execute(
                    sa.update(WorkBatchClaims)
                    .where(WorkBatchClaims.id == claim["id"])
                    .values(status="complete")
                )

        except Exception:
            with catalog.transaction() as connection:
                connection.execute(
                    sa.update(WorkBatchClaims)
                    .where(WorkBatchClaims.id == claim["id"])
                    .values(status="pending")
                )
            raise


@job(resource_defs={"runtime": runtime_resource})
def snapshot_job() -> None:
    """Publish a bounded initial works snapshot sample."""
    process_batches(collect_snapshot())
