"""GDELT DOC 2.0 article search with a request budget and polite spacing."""

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

import httpx

from src.common.log import logger

API = "https://api.gdeltproject.org/api/v2/doc/doc"
USER_AGENT = "vml-crawler-investments/0.1 (weak-signal research)"
RETRY_STATUSES = {429, 500, 502, 503, 504}
FUNDING = (
    '(raises OR raised OR funding OR "series a" OR "series b" OR seed OR '
    "investment OR acquires OR acquisition OR ipo OR grant)"
)


class BudgetExhausted(Exception):
    """Stop the run and leave remaining scans pending."""


class Throttled(BudgetExhausted):
    """The source keeps rejecting requests with 429 after all retries."""


@dataclass(frozen=True)
class Article:
    """One article from the search result list."""

    url: str
    title: str
    seen_at: datetime
    domain: str
    language: str | None
    source_country: str | None


@dataclass(frozen=True)
class Page:
    """One search response."""

    locator: str
    status_code: int
    body: bytes
    payload: dict[str, Any] | None
    articles: list[Article]


class Gdelt:
    """Search funding news for a phrase in a month window."""

    def __init__(
        self,
        budget: int,
        interval: float,
        max_retries: int,
        retry_seconds: float,
        timeout: float,
        transport: httpx.BaseTransport | None,
        sleep: Callable[[float], None],
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Create a client; the budget counts retries too."""
        self.client = httpx.Client(
            transport=transport, timeout=timeout, headers={"User-Agent": USER_AGENT}
        )
        self.remaining = budget
        self.requests = 0
        self.interval = interval
        self.max_retries = max_retries
        self.retry_seconds = retry_seconds
        self.sleep = sleep
        self.clock = clock
        self.last: float | None = None

    def _get(self, params: dict[str, Any]) -> httpx.Response:
        """Send one spaced request, retrying transport errors, 429, and 5xx."""
        for attempt in range(self.max_retries + 1):
            if self.remaining <= 0:
                raise BudgetExhausted
            if self.last is not None:
                wait = self.interval - (self.clock() - self.last)
                if wait > 0:
                    self.sleep(wait)
            self.remaining -= 1
            self.requests += 1
            self.last = self.clock()
            final = attempt == self.max_retries
            try:
                response = self.client.get(API, params=params)
            except httpx.TransportError as error:
                if final:
                    raise
                logger.warning("gdelt_retry", error_type=type(error).__name__)
                self.sleep(self.retry_seconds * 2**attempt)
                continue
            if response.status_code in RETRY_STATUSES and not final:
                logger.warning("gdelt_retry", status_code=response.status_code)
                self.sleep(self.retry_seconds * 2**attempt)
                continue
            if response.status_code == 429:
                # Persistent throttling ends the run; the scan stays pending.
                raise Throttled
            return response
        raise AssertionError("unreachable")

    def search(
        self, normalized: str, language: str, start: date, end: date, cap: int
    ) -> Page:
        """Return up to cap funding articles in one source language."""
        response = self._get(
            {
                "query": f'"{normalized}" {FUNDING} sourcelang:{language}',
                "mode": "artlist",
                "format": "json",
                "maxrecords": cap,
                "sort": "datedesc",
                "startdatetime": start.strftime("%Y%m%d000000"),
                "enddatetime": end.strftime("%Y%m%d000000"),
            }
        )
        response.raise_for_status()
        try:
            payload = json.loads(response.content) if response.content.strip() else {}
        except json.JSONDecodeError:
            payload = None
        return Page(
            locator=str(response.request.url),
            status_code=response.status_code,
            body=response.content,
            payload=payload,
            articles=[
                self.article(item) for item in (payload or {}).get("articles", [])
            ],
        )

    @staticmethod
    def article(item: dict[str, Any]) -> Article:
        """Normalize one result entry."""
        return Article(
            url=item["url"],
            title=item.get("title") or "",
            seen_at=datetime.strptime(item["seendate"], "%Y%m%dT%H%M%SZ").replace(
                tzinfo=UTC
            ),
            domain=item.get("domain") or "",
            language=item.get("language") or None,
            source_country=item.get("sourcecountry") or None,
        )

    def close(self) -> None:
        """Release pooled connections."""
        self.client.close()
