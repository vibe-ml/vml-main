"""Hacker News stories and comments via the public Algolia search API."""

import json
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, date, datetime
from typing import Any

from src.collection.platforms.base import Http, Item, Page, locator, quoted
from src.collection.terms import plain
from src.collection.windows import timestamp

SEARCH = "https://hn.algolia.com/api/v1/search_by_date"
# Algolia never serves hits beyond this offset, whatever the page size.
PAGINATION_LIMIT = 1000
FIELDS = [
    "title",
    "story_title",
    "url",
    "author",
    "created_at_i",
    "points",
    "num_comments",
    "story_text",
    "comment_text",
    "_tags",
]


class HackerNews:
    """Search stories and comments by exact phrase and creation time."""

    name = "hackernews"
    channels: tuple[str, ...] = ("hn",)

    def __init__(self, http: Http) -> None:
        """Use a budgeted client."""
        self.http = http

    def mention_id(self, channel: str, native_id: str) -> str:
        """Build a platform-qualified mention identifier."""
        return f"hackernews:{native_id}"

    def _get(self, params: dict[str, Any]) -> tuple[Page, dict[str, Any]]:
        """Fetch one Algolia page."""
        response = self.http.request("GET", SEARCH, params=params)
        response.raise_for_status()
        data = response.json()
        page = Page(
            locator=locator(response),
            status_code=response.status_code,
            body=response.content,
            payload=data,
            items=[],
            total=data["nbHits"],
            has_more=False,
            exact=bool(data.get("exhaustiveNbHits", False)),
        )
        return page, data

    @staticmethod
    def filters(start: date, end: date) -> str:
        """Restrict hits to [start, end)."""
        return f"created_at_i>={timestamp(start)},created_at_i<{timestamp(end)}"

    def search(
        self, channel: str, term: str, start: date, end: date, cap: int
    ) -> Iterator[Page]:
        """Yield pages of exact-phrase hits without typo tolerance."""
        per_page = min(cap, PAGINATION_LIMIT)
        number = 0
        while True:
            page, data = self._get(
                {
                    "query": quoted(term),
                    "tags": "(story,comment)",
                    "numericFilters": self.filters(start, end),
                    "hitsPerPage": per_page,
                    "page": number,
                    "typoTolerance": "false",
                    "advancedSyntax": "true",
                    "attributesToHighlight": "[]",
                    "attributesToRetrieve": json.dumps(FIELDS),
                }
            )
            following = number + 1
            has_more = (
                following < data["nbPages"] and following * per_page < PAGINATION_LIMIT
            )
            items = [self.item(hit) for hit in data["hits"]]
            yield replace(page, items=items, has_more=has_more)
            if not has_more:
                return
            number = following

    def volume(self, channel: str, start: date, end: date) -> Page | None:
        """Count all stories and comments in [start, end)."""
        page, _ = self._get(
            {
                "tags": "(story,comment)",
                "numericFilters": self.filters(start, end),
                "hitsPerPage": 0,
            }
        )
        return page

    @staticmethod
    def item(hit: dict[str, Any]) -> Item:
        """Normalize one Algolia hit; comments match on their own text only."""
        story = "story" in hit.get("_tags", [])
        text = plain(hit.get("story_text") if story else hit.get("comment_text"))
        title = hit.get("title") if story else hit.get("story_title")
        return Item(
            native_id=str(hit["objectID"]),
            kind="story" if story else "comment",
            url=f"https://news.ycombinator.com/item?id={hit['objectID']}",
            author=hit.get("author"),
            created_at=datetime.fromtimestamp(hit["created_at_i"], UTC),
            title=title,
            text=text,
            match_text=f"{title or ''} {text}" if story else text,
            engagement={
                "points": hit.get("points") or 0,
                "comments": hit.get("num_comments") or 0,
            },
        )
