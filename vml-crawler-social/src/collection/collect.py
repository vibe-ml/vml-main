"""Budgeted scan processing with adaptive month-window splitting."""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from uuid import UUID

from src.collection.platforms.base import BudgetExhausted, Item, Platform
from src.collection.store import Catalog, ClaimedScan
from src.collection.terms import matches
from src.collection.windows import add_months, months_between, split
from src.common.log import logger

# Abort a run when failures look systemic rather than query-specific.
MAX_CONSECUTIVE_FAILURES = 5


@dataclass
class Stats:
    """Per-run scan outcomes."""

    scans: dict[str, int] = field(default_factory=dict)
    volumes: int = 0
    budget_exhausted: bool = False

    def count(self, status: str) -> None:
        """Increment one outcome."""
        self.scans[status] = self.scans.get(status, 0) + 1


def process(
    catalog: Catalog,
    platform: Platform,
    run_id: UUID,
    scan: ClaimedScan,
    cap: int,
    now: Callable[[], datetime],
) -> str:
    """Fetch one window; split it when the platform holds more than cap hits."""
    span = months_between(scan.window_start, scan.window_end)
    fetched: list[tuple[Item, int]] = []
    total: int | None = None
    status = "complete"
    for page in platform.search(
        scan.channel, scan.term, scan.window_start, scan.window_end, cap
    ):
        raw_page_id = catalog.raw_page(run_id, scan.id, platform.name, page, now())
        if page.total is not None:
            total = page.total
        if total is not None and total > cap and span > 1:
            status = "split"
            break
        fetched.extend((item, raw_page_id) for item in page.items)
        if len(fetched) >= cap and page.has_more:
            status = "split" if span > 1 else "truncated"
            break
    else:
        # Platforms may stop paging early (Algolia serves at most 1000 hits).
        if total is not None and total > len(fetched):
            status = "split" if span > 1 else "truncated"
    matched = [
        (platform.mention_id(scan.channel, item.native_id), item, raw_page_id)
        for item, raw_page_id in fetched
        if matches(scan.normalized, item.match_text)
    ]
    children = split(scan.window_start, scan.window_end) if status == "split" else ()
    catalog.finish_scan(
        scan, platform.name, status, total, len(fetched), matched, children, now()
    )
    return status


def collect_volumes(
    catalog: Catalog,
    platform: Platform,
    run_id: UUID,
    start: date,
    end: date,
    now: Callable[[], datetime],
) -> int:
    """Fill closed-month channel baselines that are still unknown."""
    stored = 0
    for channel, month in catalog.missing_volumes(
        platform.name, platform.channels, start, end
    ):
        page = platform.volume(channel, month, add_months(month, 1))
        if page is None:
            return stored
        raw_page_id = catalog.raw_page(run_id, None, platform.name, page, now())
        catalog.save_volume(platform.name, channel, month, page, raw_page_id, now())
        stored += 1
    return stored


def collect(
    catalog: Catalog,
    platform: Platform,
    run_id: UUID,
    profile_id: str,
    start: date,
    end: date,
    cap: int,
    lease_seconds: int,
    now: Callable[[], datetime],
) -> Stats:
    """Plan root windows, then drain the scan queue until budget or queue ends."""
    stats = Stats()
    started = now()
    planned = catalog.plan(
        profile_id, platform.name, platform.channels, start, end, now()
    )
    logger.info("scans_planned", platform=platform.name, planned=planned)
    try:
        stats.volumes = collect_volumes(catalog, platform, run_id, start, end, now)
        failures = 0
        while scan := catalog.claim(
            profile_id, platform.name, lease_seconds, now(), started
        ):
            try:
                status = process(catalog, platform, run_id, scan, cap, now)
            except BudgetExhausted:
                catalog.release(scan.id, now())
                raise
            except Exception as error:
                catalog.fail_scan(scan.id, type(error).__name__, now())
                logger.warning(
                    "scan_failed", scan_id=scan.id, error_type=type(error).__name__
                )
                stats.count("failed")
                failures += 1
                if failures >= MAX_CONSECUTIVE_FAILURES:
                    raise
                continue
            failures = 0
            stats.count(status)
    except BudgetExhausted:
        stats.budget_exhausted = True
        logger.info("budget_exhausted", platform=platform.name)
    return stats
