from datetime import UTC, date, datetime, timedelta
from uuid import uuid4

import httpx
import sqlalchemy as sa
from dagster import AssetObservation, OpExecutionContext, job, op
from sqlalchemy.dialects import postgresql

from src.ingestion.job import Runtime, runtime_resource
from src.ingestion.refresh import _refresh_partition
from src.ingestion.works_store import WorksCatalog
from src.models.raw import WorkObservations
from src.models.tmd import (
    WorkApiAllowances,
    WorkPartitions,
    WorkRuns,
    WorkSources,
)


@op(required_resource_keys={"runtime"})
def schedule_refresh(context: OpExecutionContext) -> dict:
    runtime: Runtime = context.resources.runtime
    settings = runtime.settings
    catalog = WorksCatalog(settings)

    with httpx.Client(
        transport=runtime.transport, timeout=settings.timeout_seconds
    ) as client:
        url = httpx.URL("https://api.openalex.org/rate-limit")
        if settings.api_key:
            url = url.copy_add_param("api_key", settings.api_key.get_secret_value())
        response = client.get(url)
        response.raise_for_status()
        rate_limit = response.json()["rate_limit"]

    list_cost = rate_limit.get("credit_costs", {}).get("list", 10)
    credits_used = rate_limit.get("credits_used", 0)
    account_used = credits_used // list_cost

    now = runtime.now().astimezone(UTC)
    today = now.date()

    with catalog.transaction() as connection:
        connection.execute(
            postgresql.insert(WorkApiAllowances)
            .values(day=today)
            .on_conflict_do_nothing()
        )
        # Update reserved to account for external usage
        connection.execute(
            sa.update(WorkApiAllowances)
            .where(WorkApiAllowances.day == today)
            .values(reserved=sa.func.greatest(WorkApiAllowances.reserved, account_used))
        )
        reserved_before = connection.execute(
            sa.select(WorkApiAllowances.reserved).where(WorkApiAllowances.day == today)
        ).scalar_one()

    cap = settings.api_request_limit
    available_allowance = max(0, cap - reserved_before)

    reserve = int(available_allowance * settings.reserve_fraction)
    works_capacity = available_allowance - reserve

    with catalog.transaction() as connection:
        connection.execute(
            sa.update(WorkApiAllowances)
            .where(WorkApiAllowances.day == today)
            .values(reserved=WorkApiAllowances.reserved + reserve)
        )

    scan_id, scope = catalog.start_scan({}, str(uuid4()), runtime.now())

    # Scheduling needs partition metadata, not the retained corpus and payloads.
    with catalog.transaction() as connection:
        partitions = list(connection.execute(sa.select(WorkPartitions)).mappings())

    # Track completed dates per scan
    # For rotating, we just want to sort by oldest completed across all scans, or never scanned
    completed_at_by_date = {}
    for p in partitions:
        if p["status"] == "complete":
            d = p["publication_date"]
            if (
                d not in completed_at_by_date
                or completed_at_by_date[d] < p["completed_at"]
            ):
                completed_at_by_date[d] = p["completed_at"]

    current_scan_partitions = {
        p["publication_date"]: p for p in partitions if p["scan_id"] == scan_id
    }

    def is_eligible(d: date) -> bool:
        p = current_scan_partitions.get(d)
        if not p:
            return True
        if p["status"] == "complete":
            return False
        if p["eligible_at"] and p["eligible_at"] > now:
            return False
        return not (p["lease_expires_at"] and p["lease_expires_at"] > now)

    start_date = scope.publication_from
    end_date = scope.publication_through
    all_dates = [
        start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)
    ]
    eligible_dates = [d for d in all_dates if is_eligible(d)]

    recent_start = max(start_date, today - timedelta(days=29))
    recent_dates = [d for d in eligible_dates if d >= recent_start]
    # Prioritize today and nearby dates fairly (distance to today, then date)
    recent_dates.sort(key=lambda d: (abs((today - d).days), d))

    recent_set = set(recent_dates)
    rotating_dates = [d for d in eligible_dates if d not in recent_set]
    # Rotate through full collection scope: never-scanned first, then oldest completed
    rotating_dates.sort(
        key=lambda d: (
            d in completed_at_by_date,
            completed_at_by_date.get(d, datetime.min.replace(tzinfo=UTC)),
            d,
        )
    )

    recent_target = round(works_capacity * settings.recent_fraction)
    rotating_target = works_capacity - recent_target

    def get_reserved():
        with catalog.transaction() as conn:
            return (
                conn.execute(
                    sa.select(WorkApiAllowances.reserved).where(
                        WorkApiAllowances.day == today
                    )
                ).scalar()
                or 0
            )

    completed_count = 0
    reserved_used = 0

    def process_queue(
        queue: list[date],
        capacity_limit: int,
        queue_name: str,
        total_planned: int | None = None,
    ) -> tuple[int, list[date]]:
        nonlocal completed_count, reserved_used
        total = total_planned if total_planned is not None else len(queue)
        offset = total - len(queue)
        used_here = 0
        remaining_queue = []
        for i, d in enumerate(queue):
            if used_here >= capacity_limit:
                remaining_queue.extend(queue[i:])
                break
            before = get_reserved()
            status = "failed"
            try:
                status = _refresh_partition(runtime, scan_id, d.isoformat())
            finally:
                after = get_reserved()
                cost = after - before
                used_here += cost
                reserved_used += cost
                if status == "complete":
                    completed_count += 1
                processed = offset + i + 1
                remaining = max(0, total - processed)
                context.log_event(
                    AssetObservation(
                        asset_key=["openalex", "daily_refresh"],
                        metadata={
                            "queue_name": queue_name,
                            "current_partition": d.isoformat(),
                            "queue_total": total,
                            "queue_processed": processed,
                            "queue_remaining": remaining,
                            "status": str(status),
                            "budget_reserved_used": reserved_used,
                            "completed_count": completed_count,
                        },
                    )
                )
                context.log.info(
                    f"Daily refresh [{queue_name}] partition {d.isoformat()} {status} "
                    f"({processed}/{total}, {remaining} remaining) | "
                    f"budget used: {reserved_used}, completed: {completed_count}"
                )
            if status == "paused_budget":
                remaining_queue.extend(queue[i + 1 :])
                break
        return used_here, remaining_queue

    used_recent, remaining_recent = process_queue(
        recent_dates, recent_target, "recent", len(recent_dates)
    )

    # Borrow reserved queue capacity only when that queue has no eligible work
    unused_recent = max(0, recent_target - used_recent)
    rotating_capacity = rotating_target + unused_recent

    used_rotating, _remaining_rotating = process_queue(
        rotating_dates, rotating_capacity, "rotating", len(rotating_dates)
    )

    unused_rotating = max(0, rotating_capacity - used_rotating)
    # If rotating didn't use all its capacity, give it back to recent
    if unused_rotating > 0 and remaining_recent:
        process_queue(remaining_recent, unused_rotating, "recent", len(recent_dates))

    with catalog.transaction() as conn:
        stats = conn.execute(
            sa.select(
                sa.func.count()
                .filter(WorkPartitions.status == "complete")
                .label("completed_parts"),
                sa.func.count()
                .filter(WorkPartitions.status != "complete")
                .label("unfinished_parts"),
                sa.func.sum(WorkPartitions.record_count).label("total_records"),
            ).where(WorkPartitions.scan_id == scan_id)
        ).fetchone()

        run_stats = conn.execute(
            sa.select(
                sa.func.count().filter(WorkRuns.status == "failed").label("failures"),
            ).where(WorkRuns.scan_id == scan_id)
        ).fetchone()

        version_stats = conn.execute(
            sa.select(
                sa.func.count()
                .filter(WorkObservations.disposition == "selected")
                .label("new_count"),
                sa.func.count()
                .filter(WorkObservations.disposition.like("quarantine:%"))
                .label("quarantine_count"),
            )
            .select_from(WorkObservations)
            .join(WorkRuns, WorkObservations.run_id == WorkRuns.id)
            .where(WorkRuns.scan_id == scan_id)
        ).fetchone()

        bytes_stat = (
            conn.execute(
                sa.select(sa.func.sum(WorkSources.bytes))
                .select_from(WorkSources)
                .join(
                    WorkObservations,
                    WorkSources.id == WorkObservations.source_id,
                )
                .join(WorkRuns, WorkObservations.run_id == WorkRuns.id)
                .where(WorkRuns.scan_id == scan_id)
            ).scalar()
            or 0
        )

        age_stat = (
            conn.execute(
                sa.select(sa.func.max(WorkSources.release))
                .select_from(WorkSources)
                .join(
                    WorkObservations,
                    WorkSources.id == WorkObservations.source_id,
                )
                .join(WorkRuns, WorkObservations.run_id == WorkRuns.id)
                .where(WorkRuns.scan_id == scan_id)
            ).scalar()
            or ""
        )

    context.add_output_metadata(
        {
            "completed_partitions": int(getattr(stats, "completed_parts", 0) or 0),
            "unfinished_partitions": int(getattr(stats, "unfinished_parts", 0) or 0),
            "record_counts": int(getattr(stats, "total_records", 0) or 0),
            "new_counts": int(getattr(version_stats, "new_count", 0) or 0),
            "changed_counts": 0,
            "unchanged_counts": 0,
            "quarantine": int(getattr(version_stats, "quarantine_count", 0) or 0),
            "bytes": int(bytes_stat),
            "failures": int(getattr(run_stats, "failures", 0) or 0),
            "release_age": age_stat,
            "source_check_times": runtime.now().isoformat(),
            "scan_intervals": f"{start_date.isoformat()} to {end_date.isoformat()}",
            "budget_state": get_reserved(),
        }
    )
    return {
        "completed_partitions": completed_count,
        "reserved_requests": reserved_used,
        "whole_corpus_fresh": False,
    }


@job(resource_defs={"runtime": runtime_resource})
def daily_refresh_job() -> None:
    schedule_refresh()


scheduler_job = daily_refresh_job
