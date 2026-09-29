"""Adapter parsing, credentials hygiene, and platform control signals."""

from datetime import UTC, date, datetime

import httpx
import pytest

from src.collection.platforms.base import BudgetExhausted, Http
from src.collection.platforms.bluesky import Bluesky
from src.collection.platforms.hackernews import HackerNews
from src.collection.platforms.stackexchange import StackExchange


def client(handler, sleeps: list[float] | None = None, budget: int = 50) -> Http:
    """Build a budgeted client over a mock handler."""
    return Http(
        budget,
        0,
        0,
        5,
        httpx.MockTransport(handler),
        (sleeps if sleeps is not None else []).append,
    )


def test_hackernews_comment_matches_own_text_only() -> None:
    item = HackerNews.item(
        {
            "objectID": "9",
            "created_at_i": 1704067200,
            "author": "carol",
            "story_title": "Lidar startups",
            "comment_text": "<p>I &amp; my team</p>",
            "_tags": ["comment"],
        }
    )
    assert item.kind == "comment" and item.title == "Lidar startups"
    assert item.match_text == "I & my team"
    assert item.created_at == datetime(2024, 1, 1, tzinfo=UTC)


def test_stackexchange_hides_key_and_honors_backoff() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["filter"] == "total":
            return httpx.Response(200, json={"total": 1, "backoff": 3})
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "question_id": 5,
                        "link": "https://x/q/5",
                        "title": "Q",
                        "body": "",
                        "creation_date": 1704067200,
                        "owner": {},
                    }
                ],
                "has_more": False,
            },
        )

    sleeps: list[float] = []
    platform = StackExchange(client(handler, sleeps), ("ai",), "secret")
    pages = list(
        platform.search("ai", "edge ai", date(2024, 1, 1), date(2024, 2, 1), 100)
    )
    assert pages[0].total == 1 and pages[1].items[0].native_id == "5"
    assert all("secret" not in page.locator for page in pages)
    assert '"edge ai"' in httpx.URL(pages[0].locator).params["q"]
    assert sleeps == [3.0]


def test_stackexchange_throttle_stops_platform() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error_name": "throttle_violation"})

    platform = StackExchange(client(handler), ("ai",), None)
    with pytest.raises(BudgetExhausted):
        list(platform.search("ai", "edge ai", date(2024, 1, 1), date(2024, 2, 1), 100))


def test_bluesky_logs_in_once_and_follows_cursor() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        if request.url.path.endswith("createSession"):
            return httpx.Response(200, json={"accessJwt": "jwt"})
        assert request.headers["Authorization"] == "Bearer jwt"
        post = {
            "uri": f"at://did:plc:abc/app.bsky.feed.post/{len(seen)}",
            "author": {"did": "did:plc:abc"},
            "record": {
                "text": "edge ai rocks",
                "createdAt": "2024-01-02T03:04:05Z",
                "langs": ["en"],
            },
            "likeCount": 2,
        }
        cursor = None if "cursor" in request.url.params else "next"
        return httpx.Response(200, json={"posts": [post], "cursor": cursor})

    platform = Bluesky(client(handler), "me.bsky.social", "app-password")
    pages = list(
        platform.search("all", "edge ai", date(2024, 1, 1), date(2024, 2, 1), 100)
    )
    assert [page.has_more for page in pages] == [True, False]
    assert seen.count("/xrpc/com.atproto.server.createSession") == 1
    item = pages[0].items[0]
    assert item.url == "https://bsky.app/profile/did:plc:abc/post/2"
    assert item.lang == "en" and item.engagement["likes"] == 2
    assert all("app-password" not in page.locator for page in pages)
