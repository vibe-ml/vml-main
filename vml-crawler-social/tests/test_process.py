"""Scan outcome rules without a database."""

from collections.abc import Iterator
from datetime import UTC, date, datetime
from typing import Any, cast
from uuid import uuid4

from src.collection.collect import process
from src.collection.platforms.base import Item, Page
from src.collection.store import Catalog, ClaimedScan


class Recorder:
    """Catalog stub capturing the scan outcome."""

    def __init__(self) -> None:
        self.outcome: dict[str, Any] = {}

    def raw_page(self, *args: Any) -> int:
        return 1

    def finish_scan(
        self, scan, platform, status, total, fetched, matched, children, now
    ) -> None:
        self.outcome = {"status": status, "total": total, "fetched": fetched}


class CappedPlatform:
    """One full page, no further pages, but a larger reported total."""

    name = "fake"
    channels: tuple[str, ...] = ("all",)

    def mention_id(self, channel: str, native_id: str) -> str:
        return native_id

    def search(self, channel, term, start, end, cap) -> Iterator[Page]:
        item = Item(
            "1", "post", "u", None, datetime(2024, 1, 2, tzinfo=UTC), None, "x", "x"
        )
        yield Page("l", 200, b"", {}, [item] * cap, 1500, has_more=False)

    def volume(self, channel, start, end) -> None:
        return None


def test_single_month_beyond_pagination_limit_is_truncated() -> None:
    catalog = Recorder()
    scan = ClaimedScan(1, "all", "t", "x", "x", date(2024, 1, 1), date(2024, 2, 1))
    status = process(
        cast(Catalog, catalog),
        CappedPlatform(),
        uuid4(),
        scan,
        1000,
        lambda: datetime.now(UTC),
    )
    assert status == "truncated"
    assert catalog.outcome == {"status": "truncated", "total": 1500, "fetched": 1000}
