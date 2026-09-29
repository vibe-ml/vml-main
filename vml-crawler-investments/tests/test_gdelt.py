"""GDELT client spacing, throttling, and parsing."""

from datetime import UTC, date, datetime

import httpx
import pytest

from src.collection.gdelt import BudgetExhausted, Gdelt, Throttled


def client(handler, sleeps: list[float], budget: int = 10) -> Gdelt:
    """Build a client with a frozen clock."""
    return Gdelt(
        budget, 10, 2, 30, 5, httpx.MockTransport(handler), sleeps.append, lambda: 0.0
    )


def test_persistent_429_raises_throttled_after_backoff() -> None:
    sleeps: list[float] = []
    source = client(lambda request: httpx.Response(429), sleeps)
    with pytest.raises(Throttled):
        source.search(
            "lab on a chip", "english", date(2025, 1, 1), date(2025, 2, 1), 250
        )
    # Two backoffs (30 s, 60 s) plus interval spacing before each retry.
    assert source.requests == 3
    assert sleeps == [30, 10, 60, 10]


def test_empty_object_means_no_articles_and_budget_is_enforced() -> None:
    source = client(lambda request: httpx.Response(200, content=b"{}"), [], budget=1)
    page = source.search(
        "lab on a chip", "english", date(2025, 1, 1), date(2025, 2, 1), 250
    )
    assert page.articles == [] and page.payload == {}
    with pytest.raises(BudgetExhausted):
        source.search(
            "lab on a chip", "english", date(2025, 1, 1), date(2025, 2, 1), 250
        )


def test_article_parsing_and_query_shape() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "articles": [
                    {
                        "url": "https://x.test/a",
                        "title": "Acme raises $3M seed",
                        "seendate": "20250219T224500Z",
                        "domain": "x.test",
                        "language": "English",
                        "sourcecountry": "Finland",
                    }
                ]
            },
        )

    page = client(handler, []).search(
        "lab on a chip", "english", date(2025, 1, 1), date(2025, 2, 1), 250
    )
    article = page.articles[0]
    assert article.seen_at == datetime(2025, 2, 19, 22, 45, tzinfo=UTC)
    assert article.source_country == "Finland"
    params = seen[0].url.params
    assert params["query"].startswith('"lab on a chip" (raises OR')
    assert params["query"].endswith("sourcelang:english")
    assert (params["startdatetime"], params["enddatetime"]) == (
        "20250101000000",
        "20250201000000",
    )
