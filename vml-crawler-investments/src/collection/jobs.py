"""Dagster op and job: collect funding news for social profile terms."""

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

import httpx
from dagster import Failure, OpExecutionContext, job, op, resource

from src.collection.collect import collect
from src.collection.gdelt import Gdelt
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
    clock: Callable[[], float] = time.monotonic


@resource
def runtime_resource() -> Runtime:
    """Resolve secrets only inside execution."""
    return Runtime(Settings())


@op(required_resource_keys={"runtime"})
def collect_funding_news(context: OpExecutionContext) -> int:
    """Search closed months since collect_from for funding news per profile term."""
    runtime: Runtime = context.resources.runtime
    settings = runtime.settings
    catalog = Catalog(settings)
    source = Gdelt(
        settings.request_budget,
        settings.request_interval,
        settings.max_retries,
        settings.retry_seconds,
        settings.timeout_seconds,
        runtime.transport,
        runtime.sleep,
        runtime.clock,
    )
    run_id = None
    try:
        catalog.migrate()
        run_id = catalog.start_run(runtime.now())
        stats = collect(
            catalog,
            source,
            run_id,
            settings.languages,
            month_start(runtime.now().date()),
            settings.window_cap,
            settings.lease_seconds,
            runtime.now,
        )
        catalog.finish_run(run_id, runtime.now(), source.requests)
        context.add_output_metadata(
            {
                "requests": source.requests,
                "events": stats.events,
                "budget_exhausted": stats.budget_exhausted,
            }
            | {f"scans_{status}": count for status, count in stats.scans.items()}
        )
        return stats.events
    except Exception as error:  # noqa: BLE001 - sanitize at the job boundary
        if run_id is not None:
            catalog.finish_run(
                run_id, runtime.now(), source.requests, type(error).__name__
            )
        logger.error("collect_failed", error_type=type(error).__name__)
        raise Failure(f"Collection failed ({type(error).__name__})") from None
    finally:
        source.close()
        catalog.engine.dispose()


@job(resource_defs={"runtime": runtime_resource})
def investments_job() -> None:
    """Collect funding news and extract funding events."""
    collect_funding_news()
