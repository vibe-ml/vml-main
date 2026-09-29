"""Dagster sensor for monitoring and triggering work batch claim transformations."""

import sqlalchemy as sa
from dagster import (
    AssetObservation,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
)

from src.common.log import logger
from src.common.settings import Settings
from src.ingestion.bootstrap import bootstrap_job
from src.ingestion.rebuild import rebuild_job
from src.ingestion.works_store import WorksCatalog
from src.models.tmd import (
    WorkBaselines,
    WorkBatchClaims,
    WorkChunks,
    WorkReleaseFiles,
    WorkReleases,
    WorkScopes,
    WorkSources,
)


@sensor(job=rebuild_job, minimum_interval_seconds=60)
def batch_claims_sensor(
    context: SensorEvaluationContext,
) -> SensorResult:
    """Monitor batch claim queue depth and trigger rebuild runs for pending claims.

    Args:
        context: Dagster sensor evaluation context.

    Returns:
        SensorResult with backlog AssetObservation, cursor, and optional RunRequests.
    """
    if (
        hasattr(context, "resources")
        and hasattr(context.resources, "catalog")
        and context.resources.catalog is not None
    ):
        catalog = context.resources.catalog
    elif (
        hasattr(context, "resources")
        and hasattr(context.resources, "runtime")
        and context.resources.runtime is not None
    ):
        catalog = WorksCatalog(context.resources.runtime.settings)
    else:
        catalog = WorksCatalog(Settings())

    # Aggregate claim statuses and collect pending claim IDs
    with catalog.transaction() as connection:
        status_counts = dict(
            connection.execute(
                sa.select(
                    WorkBatchClaims.status,
                    sa.func.count(WorkBatchClaims.id),
                ).group_by(WorkBatchClaims.status)
            ).fetchall()
        )
        pending_ids = list(
            connection.execute(
                sa.select(WorkBatchClaims.id)
                .where(WorkBatchClaims.status == "pending")
                .order_by(WorkBatchClaims.id)
            ).scalars()
        )

    pending = int(status_counts.get("pending", 0))
    running = int(status_counts.get("running", 0) + status_counts.get("claimed", 0))
    complete = int(status_counts.get("complete", 0))
    failed = int(status_counts.get("failed", 0))
    total = sum(int(c) for c in status_counts.values())

    status_str = (
        f"Pending: {pending} | Running: {running} | "
        f"Complete: {complete} | Failed: {failed}"
    )
    context.log.info(f"Batch claims status: {status_str}")
    logger.info("batch_claims_sensor_tick", status=status_str, pending=pending)

    asset_event = AssetObservation(
        asset_key=["openalex", "batch_claims"],
        description=status_str,
        metadata={
            "pending": pending,
            "running": running,
            "complete": complete,
            "failed": failed,
            "total": total,
        },
    )

    # Prevent duplicate runs for the same claim set
    prev_cursor = context.cursor or ""
    claim_set_key = (
        f"{pending}:{pending_ids[0]}:{pending_ids[-1]}" if pending_ids else ""
    )

    if claim_set_key:
        if claim_set_key != prev_cursor:
            return SensorResult(
                run_requests=[RunRequest(run_key=f"batch_claims_{claim_set_key}")],
                cursor=claim_set_key,
                asset_events=[asset_event],
            )
        return SensorResult(
            run_requests=[],
            skip_reason=f"Claim set already queued | {status_str}",
            cursor=claim_set_key,
            asset_events=[asset_event],
        )

    return SensorResult(
        run_requests=[],
        skip_reason=status_str,
        cursor="",
        asset_events=[asset_event],
    )


@sensor(job=bootstrap_job, minimum_interval_seconds=60)
def bootstrap_progress_sensor(
    context: SensorEvaluationContext,
) -> SensorResult:
    """Monitor active snapshot bootstrap file acquisition and row selection progress.

    Args:
        context: Dagster sensor evaluation context.

    Returns:
        SensorResult with progress AssetObservation.
    """
    if (
        hasattr(context, "resources")
        and hasattr(context.resources, "catalog")
        and context.resources.catalog is not None
    ):
        catalog = context.resources.catalog
    elif (
        hasattr(context, "resources")
        and hasattr(context.resources, "runtime")
        and context.resources.runtime is not None
    ):
        catalog = WorksCatalog(context.resources.runtime.settings)
    else:
        catalog = WorksCatalog(Settings())

    with catalog.transaction() as connection:
        subq_chunks = (
            sa.select(
                WorkChunks.baseline_id,
                sa.func.count(WorkChunks.id).label("chunk_count"),
            )
            .group_by(WorkChunks.baseline_id)
            .subquery()
        )

        pub_from_val = (
            str(catalog.settings.publication_from)
            if catalog.settings.publication_from
            else None
        )

        order_clauses = [WorkReleases.retrieved_at.desc()]
        if pub_from_val:
            order_clauses.append(
                sa.case(
                    (
                        sa.func.jsonb_extract_path_text(
                            WorkScopes.definition, "publication_from"
                        )
                        == pub_from_val,
                        0,
                    ),
                    else_=1,
                )
            )
        order_clauses.extend(
            [
                sa.case(
                    (WorkBaselines.selection_status == "partial", 0),
                    else_=1,
                ),
                sa.func.coalesce(subq_chunks.c.chunk_count, 0).desc(),
                WorkBaselines.id.desc(),
            ]
        )

        latest_baseline = (
            connection.execute(
                sa.select(
                    WorkBaselines.id,
                    WorkBaselines.release_id,
                    WorkBaselines.selection_status,
                    WorkReleases.release,
                    WorkReleases.acquisition_status,
                )
                .select_from(WorkBaselines)
                .join(WorkReleases, WorkReleases.id == WorkBaselines.release_id)
                .join(WorkScopes, WorkScopes.id == WorkBaselines.scope_id)
                .outerjoin(subq_chunks, subq_chunks.c.baseline_id == WorkBaselines.id)
                .order_by(*order_clauses)
                .limit(1)
            )
            .mappings()
            .first()
        )

        if not latest_baseline:
            return SensorResult(
                run_requests=[],
                skip_reason="No snapshot baseline found",
            )

        release_id = latest_baseline["release_id"]
        baseline_id = latest_baseline["id"]

        files_stats = connection.execute(
            sa.select(
                sa.func.count(WorkReleaseFiles.id),
                sa.func.count(WorkReleaseFiles.source_id),
                sa.func.coalesce(sa.func.sum(WorkSources.bytes), 0),
            )
            .select_from(WorkReleaseFiles)
            .outerjoin(WorkSources, WorkSources.id == WorkReleaseFiles.source_id)
            .where(WorkReleaseFiles.release_id == release_id)
        ).first()

        total_files = int(files_stats[0] or 0)
        acquired_files = int(files_stats[1] or 0)
        acquired_bytes = int(files_stats[2] or 0)

        rows_stats = connection.execute(
            sa.select(
                sa.func.coalesce(sa.func.sum(WorkChunks.row_count), 0),
                sa.func.count(WorkChunks.id),
            ).where(WorkChunks.baseline_id == baseline_id)
        ).first()

        processed_rows = int(rows_stats[0] or 0)
        committed_chunks = int(rows_stats[1] or 0)

        total_rows = int(
            connection.execute(
                sa.select(sa.func.coalesce(sa.func.sum(WorkSources.rows), 0))
                .select_from(WorkReleaseFiles)
                .join(WorkSources, WorkSources.id == WorkReleaseFiles.source_id)
                .where(WorkReleaseFiles.release_id == release_id)
            ).scalar_one()
        )

    pct_downloaded = (
        round(100.0 * acquired_files / total_files, 2) if total_files else 0.0
    )
    pct_processed = round(100.0 * processed_rows / total_rows, 4) if total_rows else 0.0
    acquired_gib = round(acquired_bytes / (1024**3), 2)

    status_str = (
        f"Bootstrap [{latest_baseline['selection_status']}]: "
        f"{acquired_files}/{total_files} files ({pct_downloaded}%) | "
        f"{processed_rows}/{total_rows} rows ({pct_processed}%) | "
        f"{acquired_gib} GiB | chunks: {committed_chunks}"
    )
    context.log.info(status_str)
    logger.info(
        "bootstrap_progress_sensor_tick",
        status=latest_baseline["selection_status"],
        pct_downloaded=pct_downloaded,
        pct_processed=pct_processed,
        processed_rows=processed_rows,
        total_rows=total_rows,
    )

    asset_event = AssetObservation(
        asset_key=["openalex", "bootstrap_progress"],
        description=status_str,
        metadata={
            "baseline_id": baseline_id,
            "release_id": release_id,
            "release": latest_baseline["release"],
            "selection_status": latest_baseline["selection_status"],
            "acquisition_status": latest_baseline["acquisition_status"],
            "total_files": total_files,
            "acquired_files": acquired_files,
            "pct_downloaded": pct_downloaded,
            "acquired_gib": acquired_gib,
            "acquired_bytes_str": f"{acquired_bytes:,} B",
            "processed_rows": processed_rows,
            "total_rows": total_rows,
            "pct_processed": pct_processed,
            "committed_chunks": committed_chunks,
        },
    )

    return SensorResult(
        run_requests=[],
        skip_reason=status_str,
        asset_events=[asset_event],
    )
