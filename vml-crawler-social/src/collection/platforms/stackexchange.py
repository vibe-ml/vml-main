"""Stack Exchange questions via the public API, one channel per site."""

from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, date, datetime
from typing import Any

from src.collection.platforms.base import (
    BudgetExhausted,
    Http,
    Item,
    Page,
    locator,
    quoted,
)
from src.collection.terms import plain
from src.collection.windows import timestamp

API = "https://api.stackexchange.com/2.3"
PAGE_SIZE = 100


class StackExchange:
    """Search questions by phrase; totals come from the built-in total filter."""

    name = "stackexchange"

    def __init__(self, http: Http, sites: tuple[str, ...], key: str | None) -> None:
        """Use a budgeted client; an app key raises the daily quota."""
        self.http = http
        self.channels = sites
        self.key = key
        self.pause = 0.0

    def mention_id(self, channel: str, native_id: str) -> str:
        """Build a site-qualified mention identifier."""
        return f"stackexchange:{channel}:{native_id}"

    def _get(self, path: str, params: dict[str, Any]) -> tuple[Page, dict[str, Any]]:
        """Fetch one page, honoring backoff, quota, and throttle signals."""
        if self.pause:
            self.http.sleep(self.pause)
            self.pause = 0.0
        if self.key:
            params = params | {"key": self.key}
        response = self.http.request("GET", f"{API}{path}", params=params)
        data = response.json()
        if data.get("error_name") == "throttle_violation":
            raise BudgetExhausted
        response.raise_for_status()
        self.pause = float(data.get("backoff", 0))
        if data.get("quota_remaining") == 0:
            # Keep this page; the next request would be rejected.
            self.http.remaining = 0
        page = Page(
            locator=locator(response, "key"),
            status_code=response.status_code,
            body=response.content,
            payload=data,
            items=[],
            total=data.get("total"),
            has_more=bool(data.get("has_more", False)),
        )
        return page, data

    @staticmethod
    def dates(start: date, end: date) -> dict[str, int]:
        """Express [start, end) with the API's inclusive bounds."""
        return {"fromdate": timestamp(start), "todate": timestamp(end) - 1}

    def search(
        self, channel: str, term: str, start: date, end: date, cap: int
    ) -> Iterator[Page]:
        """Yield a total-only page first, then question pages with bodies."""
        common = {"q": quoted(term), "site": channel} | self.dates(start, end)
        page, data = self._get("/search/advanced", common | {"filter": "total"})
        yield replace(page, has_more=data["total"] > 0)
        if data["total"] == 0:
            return
        number = 1
        while True:
            page, data = self._get(
                "/search/advanced",
                common
                | {
                    "filter": "withbody",
                    "sort": "creation",
                    "order": "asc",
                    "pagesize": PAGE_SIZE,
                    "page": number,
                },
            )
            items = [self.item(entry) for entry in data.get("items", [])]
            yield replace(page, items=items, total=None)
            if not page.has_more:
                return
            number += 1

    def volume(self, channel: str, start: date, end: date) -> Page | None:
        """Count questions asked on the site in [start, end)."""
        page, _ = self._get(
            "/questions", {"site": channel, "filter": "total"} | self.dates(start, end)
        )
        return page

    @staticmethod
    def item(entry: dict[str, Any]) -> Item:
        """Normalize one question."""
        owner = entry.get("owner") or {}
        author = owner.get("account_id") or owner.get("user_id")
        title = plain(entry.get("title"))
        text = plain(entry.get("body"))
        return Item(
            native_id=str(entry["question_id"]),
            kind="question",
            url=entry["link"],
            author=None if author is None else str(author),
            created_at=datetime.fromtimestamp(entry["creation_date"], UTC),
            title=title,
            text=text,
            match_text=f"{title} {text}",
            engagement={
                "score": max(entry.get("score") or 0, 0),
                "answers": entry.get("answer_count") or 0,
            },
        )
