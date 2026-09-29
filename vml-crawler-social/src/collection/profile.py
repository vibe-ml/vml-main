"""Build versioned search profiles from OpenAlex topics and keywords."""

import hashlib
import json
from datetime import datetime
from typing import Any

from src.collection.store import Catalog
from src.collection.terms import normalize, term_id
from src.common.log import logger
from src.common.settings import Settings


def select_topics(
    settings: Settings, topics: dict[str, dict[str, Any]], counts: dict[str, int]
) -> list[str]:
    """Pick explicit topics, else the most frequent in-scope primary topics."""
    if settings.topic_ids:
        return [identity for identity in settings.topic_ids if identity in topics]
    ranked = sorted(counts.items(), key=lambda pair: (-pair[1], pair[0]))
    return [identity for identity, _ in ranked if identity in topics][
        : settings.topic_limit
    ]


def phrases(topic: dict[str, Any]) -> list[tuple[str, str]]:
    """List the topic name and its keywords with their origin."""
    keywords = topic.get("keywords") or []
    return [(topic["display_name"], "display_name")] + [
        (keyword, "keyword") for keyword in keywords if isinstance(keyword, str)
    ]


def build_profile(catalog: Catalog, settings: Settings, now: datetime) -> str:
    """Derive topics and terms from the latest taxonomy and store the profile."""
    bundle_id, records = catalog.latest_taxonomy()
    topics = {record["id"]: record for record in records["topics"]}
    counts = catalog.topic_work_counts()
    chosen = select_topics(settings, topics, counts)
    if not chosen:
        raise LookupError("No OpenAlex topics match the profile settings")
    excluded = {normalize(term) for term in settings.excluded_terms}
    topic_rows, terms, links, identity = [], {}, [], {}
    for topic_id in chosen:
        topic = topics[topic_id]
        topic_rows.append(
            {
                "topic_id": topic_id,
                "topic_name": topic["display_name"],
                "subfield_name": (topic.get("subfield") or {}).get("display_name"),
                "field_name": (topic.get("field") or {}).get("display_name"),
                "domain_name": (topic.get("domain") or {}).get("display_name"),
                "work_count": counts.get(topic_id, 0),
            }
        )
        seen = set()
        for phrase, origin in phrases(topic):
            text = " ".join(phrase.replace('"', " ").split())
            normalized = normalize(text)
            if (
                len(normalized) < settings.min_term_length
                or normalized in excluded
                or normalized in seen
            ):
                continue
            seen.add(normalized)
            identity_hash = term_id(normalized)
            terms.setdefault(
                identity_hash,
                {"id": identity_hash, "term": text, "normalized": normalized},
            )
            links.append(
                {"topic_id": topic_id, "term_id": identity_hash, "origin": origin}
            )
        identity[topic_id] = sorted(seen)
    definition = {
        "topics": identity,
        "topic_ids": list(settings.topic_ids),
        "topic_limit": settings.topic_limit,
        "min_term_length": settings.min_term_length,
        "excluded_terms": sorted(excluded),
    }
    profile_id = hashlib.sha256(
        json.dumps(definition, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()
    created = catalog.save_profile(
        profile_id,
        bundle_id,
        definition,
        [row | {"profile_id": profile_id} for row in topic_rows],
        list(terms.values()),
        [row | {"profile_id": profile_id} for row in links],
        now,
    )
    logger.info(
        "profile_ready",
        profile_id=profile_id,
        created=created,
        topics=len(topic_rows),
        terms=len(terms),
    )
    return profile_id
