"""Durable API refresh of one frozen publication partition."""

import hashlib
import json
import random
import time
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from email.utils import parsedate_to_datetime
from urllib.parse import urlencode
from uuid import uuid4

import httpx
import sqlalchemy as sa
from dagster import Config, Failure, OpExecutionContext, job, op
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import SQLAlchemyError

from src.common.log import logger
from src.ingestion.job import Runtime, runtime_resource
from src.ingestion.raw import preserve
from src.ingestion.scope import CollectionScope
from src.ingestion.snapshot import (
    digest,
    stage_version,
)
from src.ingestion.works_store import WorksCatalog
from src.models.raw import WorkObservations
from src.models.tmd import (
    WorkApiAllowances,
    WorkApiPages,
    WorkPartitions,
    WorkRuns,
    WorkScans,
    WorkScopes,
    WorkSources,
)


class RefreshConfig(Config):
    """Identify a frozen scan and its publication partition."""

    publication_date: str
    scan_id: str


def commit_page(
    catalog: WorksCatalog,
    runtime: Runtime,
    partition: dict,
    run_id: str,
    scope: CollectionScope,
    source_id: str,
    rows: list[dict],
    following: str | None,
    count: int,
    taxonomy_id: str,
    dependency: str,
) -> None:
    """Publish selected output and advance the cursor in one fenced transaction."""
    with catalog.transaction() as connection:
        connection.execute(
            sa.select(
                sa.func.pg_advisory_xact_lock(
                    sa.func.hashtext(f"{catalog.schema}:snapshot")
                )
            )
        )
        current = (
            connection.execute(
                sa.select(WorkPartitions)
                .where(WorkPartitions.id == partition["id"])
                .with_for_update()
            )
            .mappings()
            .one()
        )
        if (
            current["fencing_revision"] != partition["fencing_revision"]
            or current["lease_expires_at"] <= runtime.now()
        ):
            raise ValueError("Stale partition worker")
        received = current["received_count"] + len(rows)
        cursors = current["seen_cursors"] + [current["next_cursor"] or "*"]
        connection.execute(
            sa.update(WorkPartitions)
            .where(WorkPartitions.id == partition["id"])
            .values(
                received_count=received,
                expected_count=count,
                seen_cursors=cursors,
                completed_at=runtime.now() if following is None else None,
            )
        )
        offset = connection.execute(
            sa.select(
                sa.func.coalesce(sa.func.max(WorkObservations.row_number) + 1, 0)
            ).where(WorkObservations.run_id == run_id)
        ).scalar_one()
        entities = []
        for number, record in enumerate(rows):
            disposition = scope.disposition(record)
            if (
                disposition == "selected"
                and record["publication_date"]
                != partition["publication_date"].isoformat()
            ):
                disposition = "excluded:partition"
            version = None
            if disposition == "selected":
                version = stage_version(connection, record)
                entities.append(record["id"])
            connection.execute(
                sa.insert(WorkObservations).values(
                    run_id=run_id,
                    row_number=offset + number,
                    source_id=source_id,
                    version_id=version,
                    disposition=disposition,
                )
            )
        catalog.update_current_selection(connection, scope.identity, entities)
        connection.execute(
            sa.update(WorkPartitions)
            .where(WorkPartitions.id == partition["id"])
            .values(
                next_cursor=following,
                status="complete" if following is None else "running",
                record_count=count,
            )
        )
        runtime.failure("before_api_commit")
    runtime.failure("after_api_commit")


def _refresh_partition(runtime: Runtime, scan_id: str, publication_date: str) -> str:
    """Collect API pages without claiming whole-corpus freshness."""
    catalog = WorksCatalog(runtime.settings)
    run_id = str(uuid4())
    owned = False
    failed = False
    partition_id = hashlib.sha256(f"{scan_id}:{publication_date}".encode()).hexdigest()
    try:
        catalog.migrate()
        with catalog.transaction() as connection:
            # Serialize partition claims; a live owner cannot be superseded.
            connection.execute(
                sa.select(
                    sa.func.pg_advisory_xact_lock(
                        sa.func.hashtext(f"{catalog.schema}:partition")
                    )
                )
            )
            definition = connection.execute(
                sa.select(WorkScopes.definition)
                .select_from(WorkScopes)
                .join(WorkScans, WorkScans.scope_id == WorkScopes.id)
                .where(WorkScans.id == scan_id)
            ).scalar_one()
            scope = CollectionScope.model_validate(definition)
            day = date.fromisoformat(publication_date)
            if not scope.publication_from <= day <= scope.publication_through:
                raise ValueError("Partition outside frozen scope")
            connection.execute(
                postgresql.insert(WorkPartitions)
                .values(
                    id=partition_id,
                    scan_id=scan_id,
                    publication_date=day,
                    status="running",
                )
                .on_conflict_do_nothing()
            )
            partition = dict(
                connection.execute(
                    sa.select(WorkPartitions)
                    .where(WorkPartitions.id == partition_id)
                    .with_for_update()
                )
                .mappings()
                .one()
            )
            if partition["status"] == "complete":
                return "complete"
            now = runtime.now().astimezone(UTC)
            if partition["eligible_at"] and partition["eligible_at"] > now:
                return partition["status"]
            if partition["lease_expires_at"] and partition["lease_expires_at"] > now:
                return "leased"
            day_scope = scope.model_copy(
                update={"publication_from": day, "publication_through": day}
            )
            parameters = partition[
                "request_parameters"
            ] or day_scope.api_parameters() | {
                "per_page": str(runtime.settings.page_size)
            }
            partition["request_parameters"] = parameters
            connection.execute(
                sa.update(WorkPartitions)
                .where(WorkPartitions.id == partition_id)
                .values(
                    request_parameters=parameters,
                    request_fingerprint=digest(parameters),
                )
            )
            # Ownership expiry preserves committed pagination; only rejected cursors restart.
            partition["fencing_revision"] += 1
            connection.execute(
                sa.update(WorkPartitions)
                .where(WorkPartitions.id == partition_id)
                .values(
                    fencing_revision=partition["fencing_revision"],
                    status="running",
                    lease_expires_at=now
                    + timedelta(seconds=runtime.settings.api_lease_seconds),
                )
            )
            connection.execute(
                sa.insert(WorkRuns).values(
                    id=run_id,
                    observed_at=runtime.now(),
                    lower_date=day,
                    upper_date=day,
                    status="running",
                    scope_id=scope.identity,
                    scan_id=scan_id,
                )
            )
        owned = True
        taxonomy = catalog.read()
        taxonomy_id = taxonomy["current_bundle_id"]
        if taxonomy_id is None:
            with catalog.transaction() as connection:
                connection.execute(
                    sa.update(WorkPartitions)
                    .where(
                        WorkPartitions.id == partition_id,
                        WorkPartitions.fencing_revision
                        == partition["fencing_revision"],
                    )
                    .values(status="waiting_taxonomy")
                )
            return "waiting_taxonomy"
        dependency = next(
            b["classification_hash"]
            for b in taxonomy["bundles"]
            if b["id"] == taxonomy_id
        )
        parameters = partition["request_parameters"]
        cursor = partition["next_cursor"] or "*"
        seen = set(partition["seen_cursors"])
        retries = 0
        restarted = False
        with httpx.Client(
            transport=runtime.transport, timeout=runtime.settings.timeout_seconds
        ) as client:
            while True:
                now = runtime.now().astimezone(UTC)
                # Reserve before transport; crashes conservatively retain the reservation.
                with catalog.transaction() as connection:
                    connection.execute(
                        sa.select(
                            sa.func.pg_advisory_xact_lock(
                                sa.func.hashtext(f"{catalog.schema}:api_allowance")
                            )
                        )
                    )
                    lease = connection.execute(
                        sa.update(WorkPartitions)
                        .where(
                            WorkPartitions.id == partition_id,
                            WorkPartitions.fencing_revision
                            == partition["fencing_revision"],
                            WorkPartitions.lease_expires_at > now,
                        )
                        .values(
                            lease_expires_at=now
                            + timedelta(seconds=runtime.settings.api_lease_seconds)
                        )
                        .returning(WorkPartitions.id)
                    ).scalar()
                    if lease is None:
                        raise ValueError("Expired partition lease")
                    connection.execute(
                        postgresql.insert(WorkApiAllowances)
                        .values(day=now.date())
                        .on_conflict_do_nothing()
                    )
                    reserved = connection.execute(
                        sa.select(WorkApiAllowances.reserved)
                        .where(WorkApiAllowances.day == now.date())
                        .with_for_update()
                    ).scalar_one()
                    if reserved >= runtime.settings.api_request_limit:
                        eligible = datetime.combine(
                            now.date() + timedelta(days=1),
                            datetime.min.time(),
                            tzinfo=UTC,
                        )
                        connection.execute(
                            sa.update(WorkPartitions)
                            .where(
                                WorkPartitions.id == partition_id,
                                WorkPartitions.fencing_revision
                                == partition["fencing_revision"],
                            )
                            .values(status="paused_budget", eligible_at=eligible)
                        )
                        return "paused_budget"
                    connection.execute(
                        sa.update(WorkApiAllowances)
                        .where(WorkApiAllowances.day == now.date())
                        .values(reserved=WorkApiAllowances.reserved + 1)
                    )
                seen.add(cursor)
                locator = "https://api.openalex.org/works?" + urlencode(
                    parameters | {"cursor": cursor}
                )
                url = httpx.URL(locator)
                if runtime.settings.api_key:
                    url = url.copy_add_param(
                        "api_key", runtime.settings.api_key.get_secret_value()
                    )
                started = runtime.now()
                try:
                    response = client.get(url)
                except httpx.TransportError:
                    if retries >= runtime.settings.api_max_retries:
                        raise
                    time.sleep(
                        min(
                            10,
                            runtime.settings.api_retry_seconds
                            * 2**retries
                            * random.uniform(0.5, 1.5),
                        )
                    )
                    retries += 1
                    continue
                ended = runtime.now()
                # Publish original bytes before parsing or advancing the checkpoint.
                raw = preserve(
                    runtime.settings.storage_root,
                    response.content,
                    runtime.failure,
                )
                source_id = digest(["api", locator, raw["compressed_checksum"]])
                with catalog.transaction() as connection:
                    connection.execute(
                        postgresql.insert(WorkSources)
                        .values(
                            id=source_id,
                            release="api",
                            source=locator,
                            path=raw["path"],
                            sha256=raw["compressed_checksum"],
                            bytes=raw["bytes"],
                            rows=0,
                        )
                        .on_conflict_do_nothing()
                    )
                    connection.execute(
                        sa.insert(WorkApiPages).values(
                            id=str(uuid4()),
                            partition_id=partition_id,
                            run_id=run_id,
                            source_id=source_id,
                            generation=partition["generation"],
                            cursor=cursor,
                            started_at=started,
                            ended_at=ended,
                            status_code=response.status_code,
                            raw_path=raw["path"],
                        )
                    )
                runtime.failure("after_api_raw_commit")
                if response.status_code == 400 and cursor != "*" and not restarted:
                    error = response.json()
                    message = (
                        str(error.get("error", "")).lower()
                        if isinstance(error, dict)
                        else ""
                    )
                    if "cursor" in message and (
                        "invalid" in message or "expired" in message
                    ):
                        with catalog.transaction() as connection:
                            changed = connection.execute(
                                sa.update(WorkPartitions)
                                .where(
                                    WorkPartitions.id == partition_id,
                                    WorkPartitions.fencing_revision
                                    == partition["fencing_revision"],
                                )
                                .values(
                                    generation=WorkPartitions.generation + 1,
                                    next_cursor=None,
                                    seen_cursors=[],
                                    received_count=0,
                                    expected_count=None,
                                )
                                .returning(WorkPartitions.id)
                            ).scalar()
                            if changed is None:
                                raise ValueError("Stale partition worker")
                        partition["generation"] += 1
                        cursor, seen, restarted = "*", set(), True
                        continue
                if response.status_code == 429 or response.status_code >= 500:
                    delay = min(
                        10,
                        runtime.settings.api_retry_seconds
                        * 2**retries
                        * random.uniform(0.5, 1.5),
                    )
                    supplied = response.headers.get("Retry-After")
                    if supplied:
                        try:
                            delay = max(delay, float(supplied))
                        except ValueError:
                            delay = max(
                                delay,
                                (
                                    parsedate_to_datetime(supplied) - runtime.now()
                                ).total_seconds(),
                            )
                    remaining = response.headers.get("X-RateLimit-Remaining")
                    if remaining is not None and Decimal(remaining) <= 0:
                        delay = max(
                            delay,
                            float(response.headers.get("X-RateLimit-Reset", 86400)),
                        )
                    if delay > 10 or retries >= runtime.settings.api_max_retries:
                        status = (
                            "paused_budget"
                            if response.status_code == 429
                            else "paused_retry"
                        )
                        with catalog.transaction() as connection:
                            connection.execute(
                                sa.update(WorkPartitions)
                                .where(
                                    WorkPartitions.id == partition_id,
                                    WorkPartitions.fencing_revision
                                    == partition["fencing_revision"],
                                )
                                .values(
                                    status=status,
                                    eligible_at=runtime.now()
                                    + timedelta(seconds=max(delay, 1)),
                                )
                            )
                        return status
                    time.sleep(delay)
                    retries += 1
                    continue
                if response.status_code in (400, 401, 403, 404, 422):
                    with catalog.transaction() as connection:
                        connection.execute(
                            sa.update(WorkPartitions)
                            .where(
                                WorkPartitions.id == partition_id,
                                WorkPartitions.fencing_revision
                                == partition["fencing_revision"],
                            )
                            .values(status="intervention_required")
                        )
                    raise ValueError(
                        "API credentials or request require operator intervention"
                    )
                response.raise_for_status()
                retries = 0
                page = json.loads(response.content, parse_float=Decimal)
                rows, metadata = page["results"], page["meta"]
                following, count = metadata["next_cursor"], metadata["count"]
                if not isinstance(rows, list) or not all(
                    isinstance(row, dict) for row in rows
                ):
                    raise ValueError("Malformed works results")
                if (
                    type(count) is not int
                    or count < 0
                    or (
                        following is not None
                        and (
                            not isinstance(following, str)
                            or not following
                            or following in seen
                            or not rows
                        )
                    )
                ):
                    raise ValueError("Malformed works pagination")
                with catalog.transaction() as connection:
                    connection.execute(
                        sa.update(WorkSources)
                        .where(WorkSources.id == source_id)
                        .values(rows=len(rows))
                    )
                commit_page(
                    catalog,
                    runtime,
                    partition,
                    run_id,
                    scope,
                    source_id,
                    rows,
                    following,
                    count,
                    taxonomy_id,
                    dependency,
                )
                if following is None:
                    with catalog.transaction() as connection:
                        connection.execute(
                            sa.update(WorkRuns)
                            .where(WorkRuns.id == run_id)
                            .values(status="complete")
                        )
                    return "complete"
                remaining = response.headers.get("X-RateLimit-Remaining")
                if remaining is not None and Decimal(remaining) <= 0:
                    eligible = runtime.now() + timedelta(
                        seconds=max(
                            1,
                            float(response.headers.get("X-RateLimit-Reset", 86400)),
                        )
                    )
                    with catalog.transaction() as connection:
                        connection.execute(
                            sa.update(WorkPartitions)
                            .where(
                                WorkPartitions.id == partition_id,
                                WorkPartitions.fencing_revision
                                == partition["fencing_revision"],
                            )
                            .values(status="paused_budget", eligible_at=eligible)
                        )
                    return "paused_budget"
                cursor = following
    except Exception as error:  # noqa: BLE001 - sanitize source failures
        failed = True
        logger.error(
            "api_partition_failed",
            run_id=run_id,
            error_type=type(error).__name__,
        )
        raise Failure(f"API partition failed ({type(error).__name__})") from None
    finally:
        try:
            if owned:
                with catalog.transaction() as connection:
                    status = connection.execute(
                        sa.update(WorkPartitions)
                        .where(
                            WorkPartitions.id == partition_id,
                            WorkPartitions.fencing_revision
                            == partition["fencing_revision"],
                        )
                        .values(
                            lease_expires_at=None,
                            status=sa.case(
                                (
                                    sa.and_(
                                        failed,
                                        WorkPartitions.status == "running",
                                    ),
                                    "failed",
                                ),
                                else_=WorkPartitions.status,
                            ),
                        )
                        .returning(WorkPartitions.status)
                    ).scalar()
                    connection.execute(
                        sa.update(WorkRuns)
                        .where(WorkRuns.id == run_id)
                        .values(status="failed" if failed else status or "superseded")
                    )
        except SQLAlchemyError:
            logger.error("api_cleanup_failed", run_id=run_id)
        finally:
            catalog.engine.dispose()


@op(required_resource_keys={"runtime"})
def refresh_partition(context: OpExecutionContext, config: RefreshConfig) -> str:
    """Refresh one API partition under its persisted collection scope."""
    runtime: Runtime = context.resources.runtime
    return _refresh_partition(runtime, config.scan_id, config.publication_date)


@job(resource_defs={"runtime": runtime_resource})
def refresh_job() -> None:
    """Refresh one API partition under its persisted collection scope."""
    from src.ingestion.snapshot import process_batches

    process_batches(refresh_partition())
