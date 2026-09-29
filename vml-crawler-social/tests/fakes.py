"""In-memory platform APIs served through httpx.MockTransport."""

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime

import httpx

from src.collection.terms import matches, normalize


@dataclass(frozen=True)
class Post:
    """A synthetic post shared by all fake platforms."""

    id: int
    created: datetime
    text: str
    author: str = "alice"


def epoch(value: datetime) -> int:
    """Unix seconds."""
    return int(value.timestamp())


def phrase(query: str) -> str:
    """Strip exact-phrase quotes."""
    return normalize(query.strip('"'))


@dataclass
class FakeApis:
    """Serve Algolia HN, Stack Exchange, and Bluesky endpoints from one corpus."""

    posts: list[Post]
    hn_volume: int = 5000
    se_volume: int = 800
    requests: list[httpx.Request] = field(default_factory=list)

    def select(self, query: str, lower: int, upper: int) -> list[Post]:
        """Posts inside [lower, upper) containing the phrase, newest first."""
        term = phrase(query)
        return sorted(
            (
                post
                for post in self.posts
                if lower <= epoch(post.created) < upper and matches(term, post.text)
            ),
            key=lambda post: -epoch(post.created),
        )

    def __call__(self, request: httpx.Request) -> httpx.Response:
        """Route one request."""
        self.requests.append(request)
        params = request.url.params
        if request.url.host == "hn.algolia.com":
            lower = upper = 0
            for part in params["numericFilters"].split(","):
                if ">=" in part:
                    lower = int(part.split(">=")[1])
                elif "<" in part:
                    upper = int(part.split("<")[1])
            per_page = int(params["hitsPerPage"])
            if "query" not in params:
                return httpx.Response(
                    200,
                    json={
                        "nbHits": self.hn_volume,
                        "nbPages": 0,
                        "hits": [],
                        "exhaustiveNbHits": True,
                    },
                )
            found = self.select(params["query"], lower, upper)
            number = int(params["page"])
            chunk = found[number * per_page : (number + 1) * per_page]
            hits = [
                {
                    "objectID": str(post.id),
                    "created_at_i": epoch(post.created),
                    "author": post.author,
                    "title": post.text,
                    "story_text": None,
                    "points": 3,
                    "num_comments": 1,
                    "_tags": ["story"],
                }
                for post in chunk
            ]
            pages = -(-len(found) // per_page)
            return httpx.Response(
                200,
                json={
                    "nbHits": len(found),
                    "nbPages": pages,
                    "hits": hits,
                    "exhaustiveNbHits": True,
                },
            )
        if request.url.host == "api.stackexchange.com":
            lower, upper = int(params["fromdate"]), int(params["todate"]) + 1
            if request.url.path.endswith("/questions"):
                return httpx.Response(200, json={"total": self.se_volume})
            found = self.select(params["q"], lower, upper)
            if params["filter"] == "total":
                return httpx.Response(200, json={"total": len(found)})
            size, number = int(params["pagesize"]), int(params["page"])
            chunk = found[(number - 1) * size : number * size]
            items = [
                {
                    "question_id": post.id,
                    "link": f"https://stackoverflow.com/q/{post.id}",
                    "title": post.text,
                    "body": "<p>body</p>",
                    "creation_date": epoch(post.created),
                    "owner": {"account_id": 7},
                    "score": 2,
                    "answer_count": 1,
                }
                for post in chunk
            ]
            return httpx.Response(
                200,
                json={
                    "items": items,
                    "has_more": number * size < len(found),
                    "quota_remaining": 9000,
                },
            )
        return httpx.Response(404, content=json.dumps({"error": "unknown"}).encode())


def month_posts(
    start_id: int,
    year: int,
    month: int,
    count: int,
    text: str,
    author: str = "alice",
) -> list[Post]:
    """Create count posts spread over the first days of a month."""
    return [
        Post(
            start_id + index,
            datetime(year, month, 1 + index % 27, 12, tzinfo=UTC),
            text,
            author,
        )
        for index in range(count)
    ]
