"""Bluesky posts via authenticated app.bsky.feed.searchPosts."""

from collections.abc import Iterator
from datetime import UTC, date, datetime
from typing import Any

from src.collection.platforms.base import Http, Item, Page, locator, quoted

ENTRYWAY = "https://bsky.social/xrpc"
PAGE_SIZE = 100


class Bluesky:
    """Search posts with an app password; public search rejects anonymous calls."""

    name = "bluesky"
    channels: tuple[str, ...] = ("all",)

    def __init__(self, http: Http, handle: str, password: str) -> None:
        """Use a budgeted client and lazy session creation."""
        self.http = http
        self.handle = handle
        self.password = password
        self.token: str | None = None

    def mention_id(self, channel: str, native_id: str) -> str:
        """Build a platform-qualified mention identifier."""
        return f"bluesky:{native_id}"

    def session(self) -> str:
        """Create an access token once per run."""
        if self.token is None:
            response = self.http.request(
                "POST",
                f"{ENTRYWAY}/com.atproto.server.createSession",
                json={"identifier": self.handle, "password": self.password},
            )
            response.raise_for_status()
            self.token = response.json()["accessJwt"]
        return self.token

    def search(
        self, channel: str, term: str, start: date, end: date, cap: int
    ) -> Iterator[Page]:
        """Yield cursor pages; the API reports no reliable total."""
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {
                "q": quoted(term),
                "sort": "latest",
                "since": f"{start.isoformat()}T00:00:00Z",
                "until": f"{end.isoformat()}T00:00:00Z",
                "limit": PAGE_SIZE,
            }
            if cursor:
                params["cursor"] = cursor
            response = self.http.request(
                "GET",
                f"{ENTRYWAY}/app.bsky.feed.searchPosts",
                params=params,
                headers={"Authorization": f"Bearer {self.session()}"},
            )
            response.raise_for_status()
            data = response.json()
            posts = data.get("posts", [])
            cursor = data.get("cursor")
            has_more = bool(cursor) and bool(posts)
            yield Page(
                locator=locator(response),
                status_code=response.status_code,
                body=response.content,
                payload=data,
                items=[self.item(post) for post in posts],
                total=None,
                has_more=has_more,
            )
            if not has_more:
                return

    def volume(self, channel: str, start: date, end: date) -> Page | None:
        """Bluesky exposes no channel volume."""
        return None

    @staticmethod
    def item(post: dict[str, Any]) -> Item:
        """Normalize one post view."""
        record = post.get("record") or {}
        author = post.get("author") or {}
        rkey = post["uri"].rsplit("/", 1)[-1]
        text = record.get("text") or ""
        langs = record.get("langs") or []
        created = datetime.fromisoformat(record.get("createdAt") or post["indexedAt"])
        return Item(
            native_id=post["uri"].removeprefix("at://"),
            kind="post",
            url=f"https://bsky.app/profile/{author.get('did', '')}/post/{rkey}",
            author=author.get("did"),
            created_at=created if created.tzinfo else created.replace(tzinfo=UTC),
            title=None,
            text=text,
            match_text=text,
            lang=langs[0] if langs else None,
            engagement={
                "likes": post.get("likeCount") or 0,
                "reposts": post.get("repostCount") or 0,
                "replies": post.get("replyCount") or 0,
                "quotes": post.get("quoteCount") or 0,
            },
        )
