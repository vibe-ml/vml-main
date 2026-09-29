"""Platform adapter contract and a budgeted HTTP client with retries."""

from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Protocol

import httpx

from src.common.log import logger

RETRY_STATUSES = {429, 500, 502, 503, 504}
USER_AGENT = "vml-crawler-social/0.1 (weak-signal research)"


class BudgetExhausted(Exception):
    """Stop a platform run and leave remaining work pending."""


@dataclass(frozen=True)
class Item:
    """One social post normalized across platforms."""

    native_id: str
    kind: str
    url: str
    author: str | None
    created_at: datetime
    title: str | None
    text: str
    match_text: str
    lang: str | None = None
    engagement: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class Page:
    """One platform response with parsed items and paging hints."""

    locator: str
    status_code: int
    body: bytes
    payload: dict[str, Any] | None
    items: list[Item]
    total: int | None
    has_more: bool
    exact: bool = True


class Platform(Protocol):
    """Search a platform channel for a phrase in a month window."""

    name: str
    channels: tuple[str, ...]

    def mention_id(self, channel: str, native_id: str) -> str:
        """Build a platform-qualified mention identifier."""
        ...

    def search(
        self, channel: str, term: str, start: date, end: date, cap: int
    ) -> Iterator[Page]:
        """Yield result pages for a quoted phrase in [start, end)."""
        ...

    def volume(self, channel: str, start: date, end: date) -> Page | None:
        """Return a page whose total is the channel volume, or None if unknown."""
        ...


class Http:
    """Count every request against a run budget and retry transient failures."""

    def __init__(
        self,
        budget: int,
        max_retries: int,
        retry_seconds: float,
        timeout: float,
        transport: httpx.BaseTransport | None,
        sleep: Callable[[float], None],
    ) -> None:
        """Create a client; the budget covers retries too."""
        self.client = httpx.Client(
            transport=transport, timeout=timeout, headers={"User-Agent": USER_AGENT}
        )
        self.remaining = budget
        self.requests = 0
        self.max_retries = max_retries
        self.retry_seconds = retry_seconds
        self.sleep = sleep

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """Send one request, retrying transport errors, 429, and 5xx."""
        for attempt in range(self.max_retries + 1):
            if self.remaining <= 0:
                raise BudgetExhausted
            self.remaining -= 1
            self.requests += 1
            last = attempt == self.max_retries
            try:
                response = self.client.request(method, url, **kwargs)
            except httpx.TransportError as error:
                if last:
                    raise
                logger.warning("http_retry", error_type=type(error).__name__)
                self.sleep(self.retry_seconds * 2**attempt)
                continue
            if response.status_code in RETRY_STATUSES and not last:
                delay = response.headers.get("Retry-After", "")
                wait = (
                    float(delay) if delay.isdigit() else self.retry_seconds * 2**attempt
                )
                logger.warning("http_retry", status_code=response.status_code)
                self.sleep(wait)
                continue
            return response
        raise AssertionError("unreachable")

    def close(self) -> None:
        """Release pooled connections."""
        self.client.close()


def locator(response: httpx.Response, *secret_params: str) -> str:
    """Render the request URL without credential parameters."""
    url = response.request.url
    for name in secret_params:
        url = url.copy_remove_param(name)
    return str(url)


def quoted(term: str) -> str:
    """Quote a phrase for exact-phrase search syntax."""
    return '"' + term.replace('"', " ").strip() + '"'
