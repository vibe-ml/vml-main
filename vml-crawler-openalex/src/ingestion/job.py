"""Runnable Dagster taxonomy collection with controllable system boundaries."""

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from urllib.parse import urlencode
from uuid import uuid4

import httpx
from dagster import Failure, OpExecutionContext, job, op, resource
from sqlalchemy.exc import SQLAlchemyError

from src.common.log import logger
from src.common.settings import Settings
from src.ingestion.raw import preserve
from src.ingestion.store import Catalog
from src.ingestion.taxonomy import KINDS, identities, validate


def utc_now() -> datetime:
    """Return the observation clock in UTC."""
    return datetime.now(UTC)


def no_failure(stage: str) -> None:
    """Default persistence fault hook."""


@dataclass
class Runtime:
    """Inject only external transport, time, and persistence failures."""

    settings: Settings
    transport: httpx.BaseTransport | None = field(default=None, repr=False)
    now: Callable[[], datetime] = utc_now
    failure: Callable[[str], None] = no_failure


@resource
def runtime_resource() -> Runtime:
    """Resolve secrets only inside execution."""
    return Runtime(Settings())


@op(required_resource_keys={"runtime"})
def collect_taxonomy(context: OpExecutionContext) -> str:
    """Publish one complete taxonomy observation, or retain a visible failure."""
    runtime: Runtime = context.resources.runtime
    settings = runtime.settings
    catalog = Catalog(settings)
    attempt = str(uuid4())
    started = runtime.now()
    try:
        catalog.migrate()
        catalog.insert(
            "attempts", {"id": attempt, "started_at": started, "status": "running"}
        )
        records = {kind: [] for kind in KINDS}
        with httpx.Client(
            transport=runtime.transport, timeout=settings.timeout_seconds
        ) as client:
            for kind in KINDS:
                cursor = "*"
                seen = set()
                expected_count = None
                while True:
                    seen.add(cursor)
                    locator = (
                        "https://api.openalex.org/"
                        + kind
                        + "?"
                        + urlencode({"cursor": cursor, "per_page": settings.page_size})
                    )
                    observed_start = runtime.now()
                    request_url = httpx.URL(locator)
                    if settings.api_key:
                        request_url = request_url.copy_add_param(
                            "api_key", settings.api_key.get_secret_value()
                        )
                    response = client.get(request_url)
                    observed_end = runtime.now()
                    raw = preserve(
                        settings.storage_root, response.content, runtime.failure
                    )
                    catalog.insert(
                        "raw_objects",
                        raw
                        | {
                            "attempt_id": attempt,
                            "source_locator": locator,
                            "started_at": observed_start,
                            "ended_at": observed_end,
                        },
                    )
                    runtime.failure("after_raw_commit")
                    response.raise_for_status()
                    page = json.loads(response.content, parse_float=Decimal)
                    rows, metadata = page["results"], page["meta"]
                    following, count = metadata["next_cursor"], metadata["count"]
                    if not isinstance(rows, list) or not all(
                        isinstance(row, dict) for row in rows
                    ):
                        raise ValueError("Malformed taxonomy results")
                    if (
                        type(count) is not int
                        or count < 0
                        or (expected_count is not None and count != expected_count)
                    ):
                        raise ValueError("Unstable taxonomy count")
                    expected_count = count
                    records[kind].extend(rows)
                    if len(records[kind]) > count:
                        raise ValueError("Taxonomy results exceed advertised count")
                    if following is None:
                        if len(records[kind]) != count:
                            raise ValueError("Incomplete terminal taxonomy page")
                        break
                    if (
                        not isinstance(following, str)
                        or not following
                        or following in seen
                        or not rows
                    ):
                        raise ValueError("Malformed taxonomy cursor")
                    cursor = following
        validate(records)
        identity, dependency = identities(records)
        runtime.failure("before_bundle_commit")
        catalog.publish(attempt, started, runtime.now(), records, identity, dependency)
        runtime.failure("after_bundle_commit")
        context.add_output_metadata(
            {
                "bundle_id": identity,
                "classification_hash": dependency,
                "attempt_id": attempt,
                "record_count": sum(map(len, records.values())),
            }
        )
        logger.info("taxonomy_published", attempt_id=attempt, bundle_id=identity)
        return identity
    except Exception as error:  # noqa: BLE001 - sanitize failures at the job boundary
        # Store only exception class: external errors can contain credentials or URLs.
        try:
            catalog.fail(attempt, runtime.now(), type(error).__name__)
        except SQLAlchemyError:
            logger.error("taxonomy_failure_not_recorded", attempt_id=attempt)
        logger.error(
            "taxonomy_failed", attempt_id=attempt, error_type=type(error).__name__
        )
        raise Failure(
            f"Taxonomy attempt {attempt} failed ({type(error).__name__})"
        ) from None
    finally:
        catalog.engine.dispose()


@job(resource_defs={"runtime": runtime_resource})
def taxonomy_job() -> None:
    """Collect the full hierarchy independently of work selection."""
    collect_taxonomy()
