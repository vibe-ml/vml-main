"""Dagster ops and job: profile, per-platform collection, attention aggregation."""

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

import httpx
from dagster import Failure, In, OpExecutionContext, job, op, resource

from src.collection import platforms
from src.collection.collect import collect
from src.collection.platforms.base import Http
from src.collection.profile import build_profile
from src.collection.store import Catalog
from src.collection.windows import month_start
from src.common.log import logger
from src.common.settings import Settings


def utc_now() -> datetime:
    """Return the observation clock in UTC."""
    return datetime.now(UTC)


@dataclass
class Runtime:
    """Inject only external transport, time, and sleeping."""

    settings: Settings
    transport: httpx.BaseTransport | None = field(default=None, repr=False)
    now: Callable[[], datetime] = utc_now
    sleep: Callable[[float], None] = time.sleep

    def window(self) -> tuple[date, date]:
        """Closed months from collect_from to the current month start."""
        return month_start(self.settings.collect_from), month_start(self.now().date())


@resource
def runtime_resource() -> Runtime:
    """Resolve secrets only inside execution."""
    return Runtime(Settings())


def fail(
    catalog: Catalog,
    run_id: Any,
    runtime: Runtime,
    stage: str,
    error: Exception,
    requests: int = 0,
) -> Failure:
    """Record a sanitized failure: external errors can contain URLs or credentials."""
    catalog.finish_run(run_id, runtime.now(), requests, type(error).__name__)
    logger.error(f"{stage}_failed", run_id=str(run_id), error_type=type(error).__name__)
    return Failure(f"{stage} run {run_id} failed ({type(error).__name__})")


@op(required_resource_keys={"runtime"})
def build_search_profile(context: OpExecutionContext) -> str:
    """Migrate the schema and derive the active profile from OpenAlex."""
    runtime: Runtime = context.resources.runtime
    catalog = Catalog(runtime.settings)
    try:
        catalog.migrate()
        run_id = catalog.start_run("profile", None, runtime.now())
        try:
            profile_id = build_profile(catalog, runtime.settings, runtime.now())
        except Exception as error:  # noqa: BLE001 - sanitize at the job boundary
            raise fail(catalog, run_id, runtime, "profile", error) from None
        catalog.finish_run(run_id, runtime.now())
        context.add_output_metadata({"profile_id": profile_id})
        return profile_id
    finally:
        catalog.engine.dispose()


def collect_platform(context: OpExecutionContext, name: str, profile_id: str) -> str:
    """Collect one platform within its request budget."""
    runtime: Runtime = context.resources.runtime
    settings = runtime.settings
    if name not in settings.platforms:
        return f"{name}:disabled"
    catalog = Catalog(settings)
    http = Http(
        settings.request_budget,
        settings.max_retries,
        settings.retry_seconds,
        settings.timeout_seconds,
        runtime.transport,
        runtime.sleep,
    )
    run_id = catalog.start_run("collect", name, runtime.now())
    try:
        platform = platforms.build(name, settings, http)
        if platform is None:
            # Coverage stays missing: absent credentials are not zero attention.
            logger.warning("platform_unavailable", platform=name)
            catalog.finish_run(run_id, runtime.now())
            return f"{name}:unavailable"
        start, end = runtime.window()
        stats = collect(
            catalog,
            platform,
            run_id,
            profile_id,
            start,
            end,
            settings.window_cap,
            settings.lease_seconds,
            runtime.now,
        )
        catalog.finish_run(run_id, runtime.now(), http.requests)
        context.add_output_metadata(
            {
                "requests": http.requests,
                "volumes": stats.volumes,
                "budget_exhausted": stats.budget_exhausted,
            }
            | {f"scans_{status}": count for status, count in stats.scans.items()}
        )
        return f"{name}:done"
    except Exception as error:  # noqa: BLE001 - sanitize at the job boundary
        raise fail(catalog, run_id, runtime, "collect", error, http.requests) from None
    finally:
        http.close()
        catalog.engine.dispose()


@op(required_resource_keys={"runtime"})
def collect_hackernews(context: OpExecutionContext, profile_id: str) -> str:
    """Collect Hacker News stories and comments."""
    return collect_platform(context, "hackernews", profile_id)


@op(required_resource_keys={"runtime"})
def collect_stackexchange(context: OpExecutionContext, profile_id: str) -> str:
    """Collect Stack Exchange questions."""
    return collect_platform(context, "stackexchange", profile_id)


@op(required_resource_keys={"runtime"})
def collect_bluesky(context: OpExecutionContext, profile_id: str) -> str:
    """Collect Bluesky posts."""
    return collect_platform(context, "bluesky", profile_id)


@op(required_resource_keys={"runtime"}, ins={"collected": In(list[str])})
def aggregate_attention(
    context: OpExecutionContext, profile_id: str, collected: list[str]
) -> int:
    """Rebuild monthly attention after all collectors finish."""
    runtime: Runtime = context.resources.runtime
    catalog = Catalog(runtime.settings)
    run_id = catalog.start_run("aggregate", None, runtime.now())
    try:
        start, end = runtime.window()
        rows = catalog.aggregate(
            profile_id, platforms.channels(runtime.settings), start, end, runtime.now()
        )
        catalog.finish_run(run_id, runtime.now())
        context.add_output_metadata({"rows": rows, "collectors": collected})
        return rows
    except Exception as error:  # noqa: BLE001 - sanitize at the job boundary
        raise fail(catalog, run_id, runtime, "aggregate", error) from None
    finally:
        catalog.engine.dispose()


@job(resource_defs={"runtime": runtime_resource})
def social_job() -> None:
    """Collect social mentions of OpenAlex topics and aggregate attention."""
    profile_id = build_search_profile()
    aggregate_attention(
        profile_id,
        [
            collect_hackernews(profile_id),
            collect_stackexchange(profile_id),
            collect_bluesky(profile_id),
        ],
    )
