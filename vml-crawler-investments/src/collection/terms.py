"""Term normalization, identities, and local phrase matching."""

import hashlib
import html
import re

TAG = re.compile(r"<[^>]+>")
NON_WORD = re.compile(r"[^\w]+")


def plain(markup: str | None) -> str:
    """Strip HTML tags and entities from platform text."""
    if not markup:
        return ""
    return " ".join(html.unescape(TAG.sub(" ", markup)).split())


def normalize(text: str | None) -> str:
    """Lowercase and collapse non-alphanumeric runs to single spaces."""
    return NON_WORD.sub(" ", (text or "").lower()).replace("_", " ").strip()


def term_id(normalized: str) -> str:
    """Hash a normalized phrase into a stable term identity."""
    return hashlib.sha256(b"term-v1\n" + normalized.encode()).hexdigest()


def matches(normalized_term: str, text: str) -> bool:
    """Require the whole normalized phrase on word boundaries."""
    return f" {normalized_term} " in f" {normalize(text)} "
