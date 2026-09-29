"""Platform adapter registry."""

from src.collection.platforms.base import Http, Platform
from src.collection.platforms.bluesky import Bluesky
from src.collection.platforms.hackernews import HackerNews
from src.collection.platforms.stackexchange import StackExchange
from src.common.settings import Settings


def channels(settings: Settings) -> dict[str, tuple[str, ...]]:
    """List configured channels per enabled platform, available or not."""
    known = {
        "hackernews": HackerNews.channels,
        "stackexchange": settings.stackexchange_sites,
        "bluesky": Bluesky.channels,
    }
    return {name: known[name] for name in settings.platforms}


def build(name: str, settings: Settings, http: Http) -> Platform | None:
    """Create an adapter, or None when required credentials are absent."""
    if name == "hackernews":
        return HackerNews(http)
    if name == "stackexchange":
        key = settings.stackexchange_key
        return StackExchange(
            http, settings.stackexchange_sites, key.get_secret_value() if key else None
        )
    if name == "bluesky":
        if not settings.bluesky_handle or not settings.bluesky_app_password:
            return None
        return Bluesky(
            http,
            settings.bluesky_handle,
            settings.bluesky_app_password.get_secret_value(),
        )
    raise ValueError(f"Unknown platform {name}")
