"""Resume the public quarterly works inventory without unbounded file buffers."""

import hashlib
import json
from collections.abc import Iterable, Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import duckdb
import httpx
import sqlalchemy as sa
from dagster import (
    AssetObservation,
    Backoff,
    Failure,
    Field,
    Jitter,
    OpExecutionContext,
    RetryPolicy,
    job,
    op,
)
from pyarrow import parquet
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import SQLAlchemyError
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
    wait_none,
)

from src.common.log import logger
from src.ingestion.job import Runtime, runtime_resource
from src.ingestion.raw import durable_directory
from src.ingestion.scope import CollectionScope
from src.ingestion.snapshot import (
    CANONICALIZATION,
    MAX_EXPANDED_BYTES,
    MAX_RECORD_BYTES,
    TRANSFORMATION,
    digest,
    encode_output,
    publish_file,
    work_identity,
)
from src.ingestion.taxonomy import canonical
from src.ingestion.works_store import WorksCatalog
from src.models.raw import (
    WorkObservations,
    WorkProcessing,
    WorkVersions,
)
from src.models.tmd import (
    WorkBaselines,
    WorkBatches,
    WorkChunks,
    WorkReleaseFiles,
    WorkReleases,
    WorkRuns,
    WorkScans,
    WorkSources,
)

MANIFEST = "https://openalex.s3.amazonaws.com/data/parquet/works/manifest.json"


def inventory(body: bytes) -> tuple[str, list[dict]]:
    """Validate the official per-entity manifest, including aggregate totals."""
    manifest = json.loads(body)
    if manifest.get("format") != "parquet" or manifest.get("entity") != "works":
        raise ValueError("Expected a works Parquet manifest")
    date.fromisoformat(manifest["date"])
    files = manifest["files"]
    seen = set()
    for entry in files:
        url, meta = entry["url"], entry["meta"]
        if (
            not url.startswith("s3://openalex/data/parquet/works/")
            or not url.endswith(".parquet")
            or url in seen
        ):
            raise ValueError("Invalid or duplicate works inventory")
        seen.add(url)
        if any(
            type(meta[key]) is not int or meta[key] < 0
            for key in ("content_length", "record_count")
        ):
            raise ValueError("Invalid inventory sizes")
        for key, length in (("sha256", 64), ("md5", 32)):
            if key in meta and (
                not isinstance(meta[key], str)
                or len(meta[key]) != length
                or any(c not in "0123456789abcdef" for c in meta[key])
            ):
                raise ValueError("Invalid inventory checksum")
    if not files or any(
        sum(f["meta"][key] for f in files) != manifest[key]
        for key in ("content_length", "record_count")
    ):
        raise ValueError("Incomplete inventory totals")
    return manifest["date"], sorted(files, key=lambda f: f["url"])


def is_transient_network_error(exc: BaseException) -> bool:
    """Identify transient network and server errors eligible for retry."""
    if isinstance(exc, (httpx.TransportError, ConnectionError, OSError)):
        return True
    return isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in {
        408,
        429,
        500,
        502,
        503,
        504,
    }


def log_download_retry(retry_state: RetryCallState) -> None:
    """Log retry attempts for transient network failures."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    logger.warning(
        "bootstrap_download_retry",
        attempt=retry_state.attempt_number,
        error_type=type(exc).__name__ if exc else None,
        error=str(exc) if exc else None,
    )


def fetch_manifest(
    client: httpx.Client,
    max_retries: int = 5,
    retry_seconds: float = 2.0,
) -> bytes:
    """Bound manifest retrieval independently of source data size with retries."""

    def _fetch() -> bytes:
        body = bytearray()
        with client.stream("GET", MANIFEST) as response:
            response.raise_for_status()
            for block in response.iter_bytes(65536):
                body.extend(block)
                if len(body) > 32 * 1024 * 1024:
                    raise ValueError("Manifest exceeds inventory limit")
        return bytes(body)

    retryer = Retrying(
        stop=stop_after_attempt(max(1, max_retries)),
        wait=wait_none()
        if retry_seconds == 0
        else wait_exponential(multiplier=retry_seconds, min=1, max=30),
        retry=retry_if_exception(is_transient_network_error),
        before_sleep=log_download_retry,
        reraise=True,
    )
    return retryer(_fetch)


def validate_file(path: Path, entry: dict) -> str:
    """Check all compressed bytes and decode every Parquet row in bounded memory."""
    meta = entry["meta"]
    if path.stat().st_size != meta["content_length"]:
        raise ValueError("Snapshot size mismatch")
    hashes = {key: hashlib.new(key) for key in ("sha256", "md5")}
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            for value in hashes.values():
                value.update(block)
    for key, value in hashes.items():
        if key in meta and value.hexdigest() != meta[key]:
            raise ValueError("Snapshot checksum mismatch")
    if parquet.ParquetFile(path).metadata.num_rows != meta["record_count"]:
        raise ValueError("Snapshot rows mismatch")
    return hashes["sha256"].hexdigest()


def acquire(
    runtime: Runtime, client: httpx.Client, entry: dict, identity: str
) -> tuple[Path, str, datetime]:
    """Adopt validated durable files or reacquire incomplete source transfers with retries."""
    root = runtime.settings.storage_root
    destination = root / "raw" / "snapshot" / "inventory" / (identity + ".parquet")
    receipt = destination.with_suffix(".json")
    if destination.exists() and receipt.exists():
        try:
            metadata = json.loads(receipt.read_bytes())
            actual = validate_file(destination, entry)
            if metadata["sha256"] == actual:
                return (
                    destination,
                    actual,
                    datetime.fromisoformat(metadata["retrieved_at"]),
                )
        except ValueError, OSError, duckdb.Error:
            pass
        # Corrupt local evidence cannot be adopted. Keep it for diagnosis.
        destination.rename(destination.with_suffix(".invalid-" + uuid4().hex))
        receipt.rename(receipt.with_suffix(".invalid-" + uuid4().hex))
    staging = root / "staging"
    durable_directory(staging)

    def download_to_staging() -> Path:
        temporary = staging / (uuid4().hex + ".parquet")
        try:
            size = 0
            with (
                client.stream(
                    "GET",
                    entry["url"].replace(
                        "s3://openalex/", "https://openalex.s3.amazonaws.com/"
                    ),
                ) as response,
                temporary.open("xb") as stream,
            ):
                response.raise_for_status()
                for block in response.iter_bytes(1024 * 1024):
                    size += len(block)
                    if size > entry["meta"]["content_length"]:
                        raise ValueError("Snapshot exceeds inventory size")
                    stream.write(block)
            validate_file(temporary, entry)
            return temporary
        except Exception:
            temporary.unlink(missing_ok=True)
            raise

    retryer = Retrying(
        stop=stop_after_attempt(max(1, runtime.settings.download_max_retries)),
        wait=wait_none()
        if runtime.settings.download_retry_seconds == 0
        else wait_exponential(
            multiplier=runtime.settings.download_retry_seconds, min=1, max=30
        ),
        retry=retry_if_exception(is_transient_network_error),
        before_sleep=log_download_retry,
        reraise=True,
    )
    temporary = retryer(download_to_staging)
    actual = validate_file(temporary, entry)
    retrieved = runtime.now().astimezone(UTC)
    runtime.failure("before_bootstrap_raw_publish")
    publish_file(temporary, destination)
    runtime.failure("after_bootstrap_raw_link")
    note = staging / (uuid4().hex + ".json")
    note.write_bytes(
        canonical({"sha256": actual, "retrieved_at": retrieved.isoformat()})
    )
    publish_file(note, receipt)
    runtime.failure("after_bootstrap_raw_publish")
    return destination, actual, retrieved


def json_records(path: Path) -> Iterator[str]:
    """Stream Parquet decoding natively in DuckDB using small bounded limits to reduce memory."""
    import gc

    with duckdb.connect(config={"memory_limit": "256MB", "threads": "1"}) as duck:
        offset = 0
        limit = 100
        while True:
            rows = duck.execute(
                f"SELECT to_json(t) FROM read_parquet(?) t LIMIT {limit} OFFSET {offset}",
                [str(path)],
            ).fetchall()
            if not rows:
                break
            for row in rows:
                yield row[0]
            offset += limit
            gc.collect()


def coverage_gaps(
    total: int, ranges: Iterable[tuple[int, int]]
) -> list[tuple[int, int]]:
    """Validate authoritative committed ranges and return uncovered half-open ranges."""
    if total < 0:
        raise ValueError("Negative source row count")
    end = 0
    gaps = []
    for start, count in sorted(ranges):
        if start < end or count <= 0 or start + count > total:
            raise ValueError("Invalid committed source coverage")
        if start > end:
            gaps.append((end, start))
        end = start + count
    if end < total:
        gaps.append((end, total))
    return gaps


def source_chunks(
    path: Path, total: int, ranges: list[tuple[int, int]], limit: int
) -> Iterator[tuple[int, list[str]]]:
    """Process gaps without changing committed boundaries or source positions."""
    from itertools import islice

    gaps = coverage_gaps(total, ranges)
    if not gaps:
        return
    cursor = json_records(path)
    position = 0
    try:
        for start, end in gaps:
            for _ in islice(cursor, start - position):
                position += 1
            if position != start:
                raise ValueError("Stored source row count mismatch")
            for rows in bounded_chunks(islice(cursor, end - start), limit):
                yield position, rows
                position += len(rows)
            if position != end:
                raise ValueError("Stored source row count mismatch")
        for _ in cursor:
            position += 1
        if position != total:
            raise ValueError("Stored source row count mismatch")
    finally:
        cursor.close()


def bounded_chunks(cursor: Iterable[str], limit: int) -> Iterator[list[str]]:
    """Split deterministically by row count and expanded byte budget."""
    rows: list[str] = []
    size = 0
    import gc

    for value in cursor:
        length = len(value.encode())
        if length > MAX_RECORD_BYTES:
            raise ValueError("Expanded record exceeds limit")
        if rows and (len(rows) == limit or size + length > MAX_EXPANDED_BYTES):
            gc.collect()
            yield rows
            rows, size = [], 0
        rows.append(value)
        size += length
    if rows:
        gc.collect()
        yield rows


def commit_chunk(
    catalog: WorksCatalog,
    runtime: Runtime,
    baseline: str,
    file_id: str,
    source: dict,
    scope: CollectionScope,
    scan: str,
    start: int,
    records: list[dict],
    taxonomy_id: str,
    dependency: str,
) -> None:
    """Commit one bounded chunk and its output atomically using batched SQL."""
    chunk_id = digest([baseline, file_id, start, len(records)])
    run_id = str(uuid4())
    with catalog.transaction() as connection:
        # Share publication serialization with bounded sample jobs.
        connection.execute(
            sa.select(
                sa.func.pg_advisory_xact_lock(
                    sa.func.hashtext(f"{catalog.schema}:snapshot")
                )
            )
        )
        if connection.execute(
            sa.select(WorkChunks.id).where(WorkChunks.id == chunk_id)
        ).scalar():
            return
        connection.execute(
            sa.insert(WorkRuns).values(
                id=run_id,
                observed_at=runtime.now(),
                lower_date=scope.publication_from,
                upper_date=scope.publication_through,
                status="complete",
                scope_id=scope.identity,
                scan_id=scan,
            )
        )
        versions, observations, selected = {}, [], set()
        for offset, record in enumerate(records):
            disposition = scope.disposition(record)
            version = None
            if disposition == "selected":
                content_hash, version = work_identity(record)
                versions[version] = {
                    "id": version,
                    "entity_id": record["id"],
                    "content_hash": content_hash,
                    "canonicalization_version": CANONICALIZATION,
                    "payload": json.loads(canonical(record).decode()),
                }
                selected.add(record["id"])
            observations.append(
                {
                    "run_id": run_id,
                    "row_number": start + offset,
                    "source_id": source["id"],
                    "version_id": version,
                    "disposition": disposition,
                }
            )
        if versions:
            connection.execute(
                postgresql.insert(WorkVersions)
                .values(list(versions.values()))
                .on_conflict_do_nothing()
            )
        existing = set(
            connection.execute(
                sa.select(WorkProcessing.version_id).where(
                    WorkProcessing.version_id.in_(list(versions)),
                    WorkProcessing.transformation == TRANSFORMATION,
                    WorkProcessing.dependency == dependency,
                )
            ).scalars()
        )
        pending = {
            version: row["payload"]
            for version, row in versions.items()
            if version not in existing
        }
        with duckdb.connect(config={"memory_limit": "256MB", "threads": "1"}) as duck:
            destination, output_hash = encode_output(
                duck, pending, runtime.settings.storage_root, run_id
            )
        manifest = {
            "source_batch_ids": [source["id"]],
            "release_manifest_hash": source["release_id"],
            "transformation": TRANSFORMATION,
            "taxonomy_snapshot_id": taxonomy_id,
            "taxonomy_classification_hash": dependency,
            "scope_id": scope.identity,
            "lower_date": str(scope.publication_from),
            "upper_date": str(scope.publication_through),
            "path": str(destination.relative_to(runtime.settings.storage_root)),
            "sha256": output_hash,
            "count": len(pending),
            "selected_count": len(selected),
            "source_count": len(records),
            "start_row": start,
            "source_sha256": source["sha256"],
        }
        runtime.failure("after_bootstrap_output_publish")
        connection.execute(
            postgresql.insert(WorkBatches).values(
                id=chunk_id,
                taxonomy_id=taxonomy_id,
                manifest=manifest,
            )
        )
        if pending:
            connection.execute(
                sa.insert(WorkProcessing).values(
                    [
                        {
                            "version_id": version,
                            "transformation": TRANSFORMATION,
                            "dependency": dependency,
                            "batch_id": chunk_id,
                        }
                        for version in pending
                    ]
                )
            )
        if observations:
            connection.execute(sa.insert(WorkObservations).values(observations))
        catalog.update_current_selection(connection, scope.identity, list(selected))
        connection.execute(
            sa.insert(WorkChunks).values(
                id=chunk_id,
                baseline_id=baseline,
                file_id=file_id,
                start_row=start,
                row_count=len(records),
                batch_id=chunk_id,
            )
        )
        runtime.failure("before_bootstrap_chunk_commit")
    runtime.failure("after_bootstrap_chunk_commit")


@op(
    required_resource_keys={"runtime"},
    retry_policy=RetryPolicy(
        max_retries=5,
        delay=15,
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.FULL,
    ),
    config_schema={
        "chunk_rows": Field(int, default_value=1000),
        "scan_id": Field(str, default_value=""),
    },
)
def collect_bootstrap(context: OpExecutionContext) -> str:
    """Acquire a consistent works release, then resume its frozen selection."""
    runtime: Runtime = context.resources.runtime
    catalog = WorksCatalog(runtime.settings)
    root = runtime.settings.storage_root
    release_id = baseline = scan = run_id = None
    try:
        catalog.migrate()
        chunk_rows = context.op_config["chunk_rows"]
        if not 1 <= chunk_rows <= 10000:
            raise ValueError("Chunk rows must be between 1 and 10000")
        with (
            catalog.engine.connect() as lock,
            httpx.Client(
                transport=runtime.transport,
                timeout=runtime.settings.timeout_seconds,
            ) as client,
        ):
            # Session lock survives chunk transactions; server releases it on process death.
            lock.execute(
                sa.select(
                    sa.func.pg_advisory_lock(
                        sa.func.hashtext(f"{catalog.schema}:bootstrap")
                    )
                )
            )
            try:
                body = fetch_manifest(
                    client,
                    runtime.settings.download_max_retries,
                    runtime.settings.download_retry_seconds,
                )
                release, files = inventory(body)
                release_id = hashlib.sha256(body).hexdigest()
                manifest_path = root / "manifests" / "snapshot" / (release_id + ".json")
                durable_directory(root / "staging")
                temporary = root / "staging" / (uuid4().hex + ".json")
                temporary.write_bytes(body)
                publish_file(temporary, manifest_path)
                with catalog.transaction() as connection:
                    connection.execute(
                        postgresql.insert(WorkReleases)
                        .values(
                            id=release_id,
                            release=release,
                            manifest_path=str(manifest_path.relative_to(root)),
                            manifest_source=MANIFEST,
                            retrieved_at=runtime.now(),
                            checked_at=None,
                            acquisition_status="partial",
                        )
                        .on_conflict_do_nothing()
                    )
                    connection.execute(
                        postgresql.insert(WorkReleaseFiles)
                        .values(
                            [
                                {
                                    "id": digest([release_id, entry]),
                                    "release_id": release_id,
                                    "inventory": entry,
                                }
                                for entry in files
                            ]
                        )
                        .on_conflict_do_nothing()
                    )
                run_id = str(uuid4())
                scan, scope = catalog.start_scan(
                    {
                        "release_manifest": release_id,
                        "scan_id": context.op_config["scan_id"],
                    },
                    run_id,
                    runtime.now().astimezone(UTC),
                )
                baseline = digest([release_id, scope.identity])
                with catalog.transaction() as connection:
                    connection.execute(
                        postgresql.insert(WorkBaselines)
                        .values(
                            id=baseline,
                            release_id=release_id,
                            scope_id=scope.identity,
                            chunk_rows=chunk_rows,
                            selection_status="partial",
                        )
                        .on_conflict_do_nothing()
                    )
                    connection.execute(
                        sa.update(WorkBaselines)
                        .where(
                            WorkBaselines.id == baseline,
                            WorkBaselines.selection_status == "failed",
                        )
                        .values(selection_status="partial")
                    )
                    connection.execute(
                        sa.update(WorkScans)
                        .where(
                            WorkScans.id == scan,
                            WorkScans.status == "failed",
                        )
                        .values(status="running")
                    )
                    chunk_rows = connection.execute(
                        sa.select(WorkBaselines.chunk_rows).where(
                            WorkBaselines.id == baseline
                        )
                    ).scalar_one()
                    acquired = {
                        row.id: row.source_id
                        for row in connection.execute(
                            sa.select(
                                WorkReleaseFiles.id, WorkReleaseFiles.source_id
                            ).where(WorkReleaseFiles.release_id == release_id)
                        )
                    }
                    acquired_bytes_count = int(
                        connection.execute(
                            sa.select(
                                sa.func.coalesce(sa.func.sum(WorkSources.bytes), 0)
                            )
                            .select_from(WorkReleaseFiles)
                            .join(
                                WorkSources,
                                WorkSources.id == WorkReleaseFiles.source_id,
                            )
                            .where(WorkReleaseFiles.release_id == release_id)
                        ).scalar_one()
                    )

                total_files = len(files)
                total_bytes = sum(
                    int(entry["meta"]["content_length"]) for entry in files
                )
                acquired_files_count = sum(
                    1 for s_id in acquired.values() if s_id is not None
                )

                if acquired_files_count > 0:
                    pct_downloaded = (
                        round(100.0 * acquired_files_count / total_files, 2)
                        if total_files
                        else 0.0
                    )
                    acquired_gib = round(acquired_bytes_count / (1024**3), 2)
                    context.log_event(
                        AssetObservation(
                            asset_key=["openalex", "bootstrap_download"],
                            metadata={
                                "phase": "acquisition",
                                "total_files": total_files,
                                "acquired_files": acquired_files_count,
                                "remaining_files": max(
                                    0, total_files - acquired_files_count
                                ),
                                "pct_downloaded": pct_downloaded,
                                "acquired_bytes_str": str(acquired_bytes_count),
                                "total_bytes_str": str(total_bytes),
                                "total_gib": round(total_bytes / (1024**3), 2),
                                "acquired_gib": acquired_gib,
                                "current_file": "resumed_cache",
                            },
                        )
                    )
                    context.log.info(
                        f"Bootstrap download resumed: {acquired_files_count}/{total_files} files "
                        f"({pct_downloaded}%) | {acquired_gib} GiB already acquired"
                    )

                for entry in files:
                    file_id = digest([release_id, entry])
                    if acquired[file_id] is not None:
                        continue
                    path, checksum, retrieved = acquire(runtime, client, entry, file_id)
                    source_id = digest([release, entry["url"], checksum])
                    with catalog.transaction() as connection:
                        connection.execute(
                            postgresql.insert(WorkSources)
                            .values(
                                id=source_id,
                                release=release,
                                source=entry["url"],
                                path=str(path.relative_to(root)),
                                sha256=checksum,
                                bytes=entry["meta"]["content_length"],
                                rows=entry["meta"]["record_count"],
                            )
                            .on_conflict_do_nothing()
                        )
                        connection.execute(
                            sa.update(WorkReleaseFiles)
                            .where(WorkReleaseFiles.id == file_id)
                            .values(source_id=source_id, retrieved_at=retrieved)
                        )
                    acquired[file_id] = source_id
                    acquired_files_count += 1
                    acquired_bytes_count += int(entry["meta"]["content_length"])
                    pct_downloaded = (
                        round(100.0 * acquired_files_count / total_files, 2)
                        if total_files
                        else 0.0
                    )
                    acquired_gib = round(acquired_bytes_count / (1024**3), 2)
                    context.log_event(
                        AssetObservation(
                            asset_key=["openalex", "bootstrap_download"],
                            metadata={
                                "phase": "acquisition",
                                "total_files": total_files,
                                "acquired_files": acquired_files_count,
                                "remaining_files": max(
                                    0, total_files - acquired_files_count
                                ),
                                "pct_downloaded": pct_downloaded,
                                "acquired_bytes_str": str(acquired_bytes_count),
                                "total_bytes_str": str(total_bytes),
                                "total_gib": round(total_bytes / (1024**3), 2),
                                "acquired_gib": acquired_gib,
                                "current_file": entry["url"],
                            },
                        )
                    )
                    context.log.info(
                        f"Bootstrap download [{acquired_files_count}/{total_files}] "
                        f"({pct_downloaded}%) | {acquired_gib} GiB | "
                        f"file: {entry['url'].split('/')[-1]}"
                    )
                    runtime.failure("after_bootstrap_raw_commit")
                checked = fetch_manifest(
                    client,
                    runtime.settings.download_max_retries,
                    runtime.settings.download_retry_seconds,
                )
                if checked != body:
                    with catalog.transaction() as connection:
                        connection.execute(
                            sa.update(WorkReleases)
                            .where(WorkReleases.id == release_id)
                            .values(
                                acquisition_status="superseded",
                                checked_at=runtime.now(),
                            )
                        )
                    raise ValueError(
                        "Manifest changed; resume against the new inventory"
                    )
                with catalog.transaction() as connection:
                    connection.execute(
                        sa.update(WorkReleases)
                        .where(WorkReleases.id == release_id)
                        .values(
                            acquisition_status="complete",
                            checked_at=runtime.now(),
                        )
                    )
                    complete = connection.execute(
                        sa.select(WorkBaselines.selection_status == "complete").where(
                            WorkBaselines.id == baseline
                        )
                    ).scalar_one()
                taxonomy = catalog.read()
                taxonomy_id = taxonomy["current_bundle_id"]
                if not complete and taxonomy_id is not None:
                    dependency = next(
                        b["classification_hash"]
                        for b in taxonomy["bundles"]
                        if b["id"] == taxonomy_id
                    )
                    with catalog.transaction() as connection:
                        total_snapshot_rows = int(
                            connection.execute(
                                sa.select(
                                    sa.func.coalesce(sa.func.sum(WorkSources.rows), 0)
                                )
                                .select_from(WorkReleaseFiles)
                                .join(
                                    WorkSources,
                                    WorkSources.id == WorkReleaseFiles.source_id,
                                )
                                .where(WorkReleaseFiles.release_id == release_id)
                            ).scalar_one()
                        )
                        total_processed_rows = int(
                            connection.execute(
                                sa.select(
                                    sa.func.coalesce(
                                        sa.func.sum(WorkChunks.row_count), 0
                                    )
                                ).where(WorkChunks.baseline_id == baseline)
                            ).scalar_one()
                        )
                        committed_chunks_count = int(
                            connection.execute(
                                sa.select(sa.func.count(WorkChunks.id)).where(
                                    WorkChunks.baseline_id == baseline
                                )
                            ).scalar_one()
                        )
                    for file_idx, entry in enumerate(files):
                        file_id = digest([release_id, entry])
                        with catalog.transaction() as connection:
                            source = dict(
                                connection.execute(
                                    sa.select(WorkSources, WorkReleaseFiles.release_id)
                                    .select_from(WorkSources)
                                    .join(
                                        WorkReleaseFiles,
                                        WorkReleaseFiles.source_id == WorkSources.id,
                                    )
                                    .where(WorkReleaseFiles.id == file_id)
                                )
                                .mappings()
                                .one()
                            )
                            committed = list(
                                connection.execute(
                                    sa.select(
                                        WorkChunks.start_row, WorkChunks.row_count
                                    ).where(
                                        WorkChunks.baseline_id == baseline,
                                        WorkChunks.file_id == file_id,
                                    )
                                ).tuples()
                            )
                        chunks_in_file = 0
                        for start, rows in source_chunks(
                            root / source["path"], source["rows"], committed, chunk_rows
                        ):
                            commit_chunk(
                                catalog,
                                runtime,
                                baseline,
                                file_id,
                                source,
                                scope,
                                scan,
                                start,
                                [json.loads(row, parse_float=Decimal) for row in rows],
                                taxonomy_id,
                                dependency,
                            )
                            total_processed_rows += len(rows)
                            committed_chunks_count += 1
                            chunks_in_file += 1  # noqa: SIM113
                            if chunks_in_file % 50 == 0:
                                pct_processed = (
                                    round(
                                        100.0
                                        * total_processed_rows
                                        / total_snapshot_rows,
                                        4,
                                    )
                                    if total_snapshot_rows
                                    else 0.0
                                )
                                context.log_event(
                                    AssetObservation(
                                        asset_key=[
                                            "openalex",
                                            "bootstrap_selection",
                                        ],
                                        metadata={
                                            "phase": "selection",
                                            "current_file_index": file_idx + 1,
                                            "total_files": len(files),
                                            "processed_rows": total_processed_rows,
                                            "total_rows": total_snapshot_rows,
                                            "pct_processed": pct_processed,
                                            "committed_chunks": committed_chunks_count,
                                            "current_file": entry["url"].split("/")[-1],
                                        },
                                    )
                                )

                        pct_processed = (
                            round(
                                100.0 * total_processed_rows / total_snapshot_rows,
                                4,
                            )
                            if total_snapshot_rows
                            else 0.0
                        )
                        context.log_event(
                            AssetObservation(
                                asset_key=["openalex", "bootstrap_selection"],
                                metadata={
                                    "phase": "selection",
                                    "current_file_index": file_idx + 1,
                                    "total_files": len(files),
                                    "processed_rows": total_processed_rows,
                                    "total_rows": total_snapshot_rows,
                                    "pct_processed": pct_processed,
                                    "committed_chunks": committed_chunks_count,
                                    "current_file": entry["url"].split("/")[-1],
                                },
                            )
                        )
                        context.log.info(
                            f"Bootstrap selection progress: {total_processed_rows}/{total_snapshot_rows} rows "
                            f"({pct_processed}%) | file {file_idx + 1}/{len(files)} | "
                            f"chunks: {committed_chunks_count}"
                        )
                with catalog.transaction() as connection:
                    # A sum cannot detect overlapping ranges or gaps.
                    stored_ranges: dict[str, list[tuple[int, int]]] = {}
                    for file_id, start, count in connection.execute(
                        sa.select(
                            WorkChunks.file_id,
                            WorkChunks.start_row,
                            WorkChunks.row_count,
                        ).where(WorkChunks.baseline_id == baseline)
                    ):
                        stored_ranges.setdefault(file_id, []).append((start, count))
                    missing = sum(
                        bool(
                            coverage_gaps(
                                entry["meta"]["record_count"],
                                stored_ranges.get(digest([release_id, entry]), []),
                            )
                        )
                        for entry in files
                    )
                    status = (
                        "complete"
                        if missing == 0 and taxonomy_id is not None
                        else "waiting_taxonomy"
                    )
                    connection.execute(
                        sa.update(WorkBaselines)
                        .where(WorkBaselines.id == baseline)
                        .values(selection_status=status)
                    )
                    connection.execute(
                        sa.update(WorkScans)
                        .where(WorkScans.id == scan)
                        .values(status=status)
                    )
                    connection.execute(
                        sa.update(WorkRuns)
                        .where(WorkRuns.id == run_id)
                        .values(status=status)
                    )
                return baseline
            finally:
                lock.execute(
                    sa.select(
                        sa.func.pg_advisory_unlock(
                            sa.func.hashtext(f"{catalog.schema}:bootstrap")
                        )
                    )
                )
    except Exception as error:  # noqa: BLE001 - sanitize errors at job boundary
        try:
            with catalog.transaction() as connection:
                connection.execute(
                    sa.update(WorkBaselines)
                    .where(
                        WorkBaselines.id == baseline,
                        WorkBaselines.selection_status != "complete",
                    )
                    .values(selection_status="failed")
                )
                connection.execute(
                    sa.update(WorkScans)
                    .where(
                        WorkScans.id == scan,
                        WorkScans.status != "complete",
                    )
                    .values(status="failed")
                )
                connection.execute(
                    sa.update(WorkRuns)
                    .where(
                        WorkRuns.id == run_id,
                        WorkRuns.status != "complete",
                    )
                    .values(status="failed")
                )
        except SQLAlchemyError:
            logger.error("bootstrap_failure_not_recorded")
        logger.error("bootstrap_failed", error_type=type(error).__name__)
        raise Failure(
            f"Snapshot bootstrap failed ({type(error).__name__})",
            allow_retries=is_transient_network_error(error),
        ) from None
    finally:
        catalog.engine.dispose()


@job(resource_defs={"runtime": runtime_resource})
def bootstrap_job() -> None:
    """Acquire and publish the complete free works release incrementally."""
    collect_bootstrap()
