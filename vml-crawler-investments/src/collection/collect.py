"""Budgeted scan processing with adaptive month-window splitting."""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from uuid import UUID

from src.collection.gdelt import BudgetExhausted, Gdelt
from src.collection.store import Catalog, ClaimedScan
from src.collection.windows import months_between, split
from src.common.log import logger

MAX_CONSECUTIVE_FAILURES = 5


@dataclass
class Stats:
    """Per-run scan outcomes."""

    scans: dict[str, int] = field(default_factory=dict)
    events: int = 0
    budget_exhausted: bool = False

    def count(self, status: str) -> None:
        """Increment one outcome."""
        self.scans[status] = self.scans.get(status, 0) + 1


def process(
    catalog: Catalog,
    source: Gdelt,
    run_id: UUID,
    scan: ClaimedScan,
    cap: int,
    now: Callable[[], datetime],
) -> tuple[str, int]:
    """Search one window; a full result list means more articles exist."""
    page = source.search(
        scan.normalized, scan.language, scan.window_start, scan.window_end, cap
    )
    raw_page_id = catalog.raw_page(run_id, scan.id, page, now())
    if page.payload is None:
        # GDELT answers malformed queries with plain-text errors.
        raise ValueError("Source returned a non-JSON response")
    status = "complete"
    children: tuple = ()
    if len(page.articles) >= cap:
        if months_between(scan.window_start, scan.window_end) > 1:
            status = "split"
            children = split(scan.window_start, scan.window_end)
        else:
            status = "truncated"
    events = catalog.finish_scan(
        scan, status, page.articles, raw_page_id, children, now()
    )
    return status, events


def collect(
    catalog: Catalog,
    source: Gdelt,
    run_id: UUID,
    languages: tuple[str, ...],
    end: date,
    cap: int,
    lease_seconds: int,
    now: Callable[[], datetime],
) -> Stats:
    """Plan root windows for profile terms, then drain the queue within budget."""
    stats = Stats()
    started = now()
    terms = catalog.profile_terms()
    planned = catalog.plan(terms, languages, end, now())
    logger.info("scans_planned", terms=len(terms), planned=planned)
    term_ids = [identity for identity, _ in terms]
    failures = 0
    try:
        while scan := catalog.claim(term_ids, lease_seconds, now(), started):
            try:
                status, events = process(catalog, source, run_id, scan, cap, now)
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
            stats.events += events
    except BudgetExhausted:
        stats.budget_exhausted = True
        logger.info("budget_exhausted")
    return stats
