"""Pure helpers: terms, windows, and the budgeted HTTP client."""

from datetime import date

import httpx
import pytest

from src.collection.platforms.base import BudgetExhausted, Http
from src.collection.terms import matches, normalize, plain
from src.collection.windows import add_months, months, months_between, split


def test_normalize_and_match_ignore_punctuation() -> None:
    assert normalize("Lab-on-a-Chip!") == "lab on a chip"
    assert matches("lab on a chip", "Built a lab on a chip; cool")
    assert matches("lab on a chip", "<p>LAB-ON-A-CHIP</p>")
    assert not matches("lidar", "Solidarity matters")
    assert plain("<p>a &amp; b</p>") == "a & b"


def test_month_windows() -> None:
    start, end = date(2024, 1, 1), date(2026, 9, 1)
    assert months_between(start, end) == 32
    assert add_months(date(2024, 12, 1), 1) == date(2025, 1, 1)
    assert split(start, end) == ((start, date(2025, 5, 1)), (date(2025, 5, 1), end))
    assert list(months(date(2024, 11, 1), date(2025, 2, 1))) == [
        date(2024, 11, 1),
        date(2024, 12, 1),
        date(2025, 1, 1),
    ]
    with pytest.raises(ValueError):
        split(start, date(2024, 2, 1))


def test_http_retries_count_against_budget() -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(503 if len(calls) < 3 else 200, json={})

    sleeps: list[float] = []
    http = Http(3, 3, 1, 5, httpx.MockTransport(handler), sleeps.append)
    assert http.request("GET", "https://example.test").status_code == 200
    assert http.requests == 3 and sleeps == [1, 2]
    with pytest.raises(BudgetExhausted):
        http.request("GET", "https://example.test")
