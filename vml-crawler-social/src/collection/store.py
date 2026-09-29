"""PostgreSQL persistence for profiles, scans, mentions, volumes, and attention."""

import hashlib
import json
import uuid
from collections import defaultdict
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import sqlalchemy as sa
from alembic import command
from sqlalchemy import Connection, create_engine
from sqlalchemy.dialects import postgresql

from src.collection.platforms.base import Item, Page
from src.collection.windows import months
from src.common.settings import Settings
from src.migrations import configuration
from src.models import openalex
from src.models.social import (
    Attention,
    Mentions,
    MentionTerms,
    Profiles,
    ProfileTerms,
    ProfileTopics,
    RawPages,
    Runs,
    Scans,
    Terms,
    Volumes,
)

CHUNK = 1000
MAX_ATTEMPTS = 3


@dataclass(frozen=True)
class ClaimedScan:
    """A leased scan with its search phrase."""

    id: int
    channel: str
    term_id: str
    term: str
    normalized: str
    window_start: date
    window_end: date


def clean(value: Any) -> Any:
    """Drop NUL characters that PostgreSQL text and JSONB reject."""
    if isinstance(value, str):
        return value.replace("\x00", "")
    return json.loads(json.dumps(value).replace("\\u0000", ""))


def digest(value: str | None) -> str | None:
    """Hash an identifier so author handles never reach the database."""
    return None if value is None else hashlib.sha256(value.encode()).hexdigest()


class Catalog:
    """Own the social namespace; read OpenAlex tables without writing them."""

    def __init__(self, settings: Settings) -> None:
        """Connect using masked credentials and validated namespaces."""
        self.settings = settings
        self.engine = create_engine(
            settings.database_url.get_secret_value(), hide_parameters=True
        )

    @contextmanager
    def transaction(self) -> Iterator[Connection]:
        """Open a transaction with placeholder schemas translated."""
        with self.engine.begin() as connection:
            yield connection.execution_options(
                schema_translate_map={
                    "social": self.settings.schema_social,
                    "openalex_raw": self.settings.openalex_schema_raw,
                }
            )

    def migrate(self) -> None:
        """Upgrade the social schema through Alembic."""
        config = configuration(self.settings)
        with self.engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "head")

    # Runs

    def start_run(self, stage: str, platform: str | None, now: datetime) -> uuid.UUID:
        """Register a running stage."""
        run_id = uuid.uuid4()
        with self.transaction() as connection:
            connection.execute(
                sa.insert(Runs).values(
                    id=run_id,
                    stage=stage,
                    platform=platform,
                    started_at=now,
                    status="running",
                )
            )
        return run_id

    def finish_run(
        self,
        run_id: uuid.UUID,
        now: datetime,
        requests: int = 0,
        error: str | None = None,
    ) -> None:
        """Close a stage as complete or failed."""
        with self.transaction() as connection:
            connection.execute(
                sa.update(Runs)
                .where(Runs.id == run_id)
                .values(
                    status="failed" if error else "complete",
                    ended_at=now,
                    requests=requests,
                    error=error,
                )
            )

    # OpenAlex (read-only)

    def latest_taxonomy(self) -> tuple[str, dict[str, Any]]:
        """Read the most recently observed taxonomy bundle."""
        observations = openalex.taxonomy_observations
        bundles = openalex.taxonomy_bundles
        with self.transaction() as connection:
            row = connection.execute(
                sa.select(bundles.c.id, bundles.c.records)
                .join(observations, observations.c.bundle_id == bundles.c.id)
                .order_by(
                    observations.c.ended_at.desc(), observations.c.sequence.desc()
                )
                .limit(1)
            ).first()
        if row is None:
            raise LookupError("No OpenAlex taxonomy bundle observed yet")
        return row.id, row.records

    def topic_work_counts(self) -> dict[str, int]:
        """Count current in-scope works per primary topic."""
        current, versions = openalex.work_current, openalex.work_versions
        topic = versions.c.payload["primary_topic"]["id"].astext
        with self.transaction() as connection:
            rows = connection.execute(
                sa.select(topic, sa.func.count(sa.distinct(current.c.entity_id)))
                .join(versions, versions.c.id == current.c.version_id)
                .where(topic.is_not(None))
                .group_by(topic)
            ).all()
        return {identity: count for identity, count in rows}

    # Profiles

    def save_profile(
        self,
        profile_id: str,
        bundle_id: str,
        definition: dict[str, Any],
        topics: Sequence[dict[str, Any]],
        terms: Sequence[dict[str, Any]],
        links: Sequence[dict[str, Any]],
        now: datetime,
    ) -> bool:
        """Insert an immutable profile once; return whether it is new."""
        with self.transaction() as connection:
            created = connection.execute(
                postgresql.insert(Profiles)
                .values(
                    id=profile_id,
                    created_at=now,
                    bundle_id=bundle_id,
                    definition=definition,
                )
                .on_conflict_do_nothing()
                .returning(Profiles.id)
            ).first()
            if created is None:
                return False
            connection.execute(postgresql.insert(Terms).on_conflict_do_nothing(), terms)
            connection.execute(sa.insert(ProfileTopics), list(topics))
            connection.execute(sa.insert(ProfileTerms), list(links))
        return True

    def active_profile(self) -> str | None:
        """Return the newest profile identifier."""
        with self.transaction() as connection:
            return connection.execute(
                sa.select(Profiles.id)
                .order_by(Profiles.created_at.desc(), Profiles.id)
                .limit(1)
            ).scalar()

    # Scans

    def plan(
        self,
        profile_id: str,
        platform: str,
        channels: Sequence[str],
        start: date,
        end: date,
        now: datetime,
    ) -> int:
        """Add root windows from each term's coverage boundary up to end."""
        with self.transaction() as connection:
            term_ids = connection.scalars(
                sa.select(ProfileTerms.term_id)
                .where(ProfileTerms.profile_id == profile_id)
                .distinct()
            ).all()
            boundaries = {
                (channel, term): boundary
                for channel, term, boundary in connection.execute(
                    sa.select(
                        Scans.channel, Scans.term_id, sa.func.max(Scans.window_end)
                    )
                    .where(Scans.platform == platform, Scans.parent_id.is_(None))
                    .group_by(Scans.channel, Scans.term_id)
                )
            }
            rows = [
                {
                    "platform": platform,
                    "channel": channel,
                    "term_id": term,
                    "window_start": boundary,
                    "window_end": end,
                    "status": "pending",
                    "updated_at": now,
                }
                for channel in channels
                for term in term_ids
                if (boundary := max(boundaries.get((channel, term), start), start))
                < end
            ]
            for offset in range(0, len(rows), CHUNK):
                connection.execute(
                    postgresql.insert(Scans).on_conflict_do_nothing(),
                    rows[offset : offset + CHUNK],
                )
        return len(rows)

    def claim(
        self,
        profile_id: str,
        platform: str,
        lease_seconds: int,
        now: datetime,
        retry_before: datetime,
    ) -> ClaimedScan | None:
        """Lease the oldest runnable scan; failed scans wait for a later run."""
        profile_terms = sa.select(ProfileTerms.term_id).where(
            ProfileTerms.profile_id == profile_id
        )
        with self.transaction() as connection:
            row = connection.execute(
                sa.select(
                    Scans.id,
                    Scans.channel,
                    Scans.term_id,
                    Scans.window_start,
                    Scans.window_end,
                    Terms.term,
                    Terms.normalized,
                )
                .join(Terms, Terms.id == Scans.term_id)
                .where(
                    Scans.platform == platform,
                    Scans.term_id.in_(profile_terms),
                    sa.or_(
                        Scans.status == "pending",
                        sa.and_(Scans.status == "claimed", Scans.claimed_until < now),
                        sa.and_(
                            Scans.status == "failed",
                            Scans.attempts < MAX_ATTEMPTS,
                            Scans.updated_at < retry_before,
                        ),
                    ),
                )
                .order_by(Scans.id)
                .limit(1)
                .with_for_update(skip_locked=True, of=Scans)
            ).first()
            if row is None:
                return None
            connection.execute(
                sa.update(Scans)
                .where(Scans.id == row.id)
                .values(
                    status="claimed",
                    attempts=Scans.attempts + 1,
                    claimed_until=now + timedelta(seconds=lease_seconds),
                    updated_at=now,
                )
            )
        return ClaimedScan(
            row.id,
            row.channel,
            row.term_id,
            row.term,
            row.normalized,
            row.window_start,
            row.window_end,
        )

    def release(self, scan_id: int, now: datetime) -> None:
        """Return an unfinished scan to the queue without spending an attempt."""
        with self.transaction() as connection:
            connection.execute(
                sa.update(Scans)
                .where(Scans.id == scan_id)
                .values(
                    status="pending",
                    attempts=Scans.attempts - 1,
                    claimed_until=None,
                    updated_at=now,
                )
            )

    def fail_scan(self, scan_id: int, error: str, now: datetime) -> None:
        """Record a failed attempt; claim retries it until attempts run out."""
        with self.transaction() as connection:
            connection.execute(
                sa.update(Scans)
                .where(Scans.id == scan_id)
                .values(
                    status="failed", error=error, claimed_until=None, updated_at=now
                )
            )

    def raw_page(
        self,
        run_id: uuid.UUID,
        scan_id: int | None,
        platform: str,
        page: Page,
        now: datetime,
    ) -> int:
        """Persist a response before interpreting it."""
        with self.transaction() as connection:
            return connection.execute(
                sa.insert(RawPages)
                .values(
                    run_id=run_id,
                    scan_id=scan_id,
                    platform=platform,
                    locator=page.locator,
                    status_code=page.status_code,
                    fetched_at=now,
                    checksum=hashlib.sha256(page.body).hexdigest(),
                    bytes=len(page.body),
                    payload=None if page.payload is None else clean(page.payload),
                )
                .returning(RawPages.id)
            ).scalar_one()

    def finish_scan(
        self,
        scan: ClaimedScan,
        platform: str,
        status: str,
        reported_total: int | None,
        fetched: int,
        matched: Sequence[tuple[str, Item, int]],
        children: Sequence[tuple[date, date]],
        now: datetime,
    ) -> None:
        """Atomically store matched mentions, the scan outcome, and split children."""
        mentions = {}
        for mention_id, item, raw_page_id in matched:
            title = clean(item.title) if item.title else None
            text = clean(item.text[: self.settings.text_limit])
            mentions[mention_id] = {
                "id": mention_id,
                "platform": platform,
                "channel": scan.channel,
                "native_id": item.native_id,
                "kind": item.kind,
                "url": item.url,
                "author_hash": digest(item.author),
                "created_at": item.created_at,
                "title": title,
                "text": text,
                "lang": item.lang,
                "engagement": item.engagement,
                "engagement_total": sum(item.engagement.values()),
                "content_hash": hashlib.sha256(
                    f"{title or ''}\n{text}".encode()
                ).hexdigest(),
                "raw_page_id": raw_page_id,
                "first_seen_at": now,
                "last_seen_at": now,
            }
        with self.transaction() as connection:
            rows = list(mentions.values())
            for offset in range(0, len(rows), CHUNK):
                statement = postgresql.insert(Mentions)
                connection.execute(
                    statement.on_conflict_do_update(
                        index_elements=[Mentions.id],
                        set_={"last_seen_at": statement.excluded.last_seen_at},
                    ),
                    rows[offset : offset + CHUNK],
                )
            links = [
                {"mention_id": mention_id, "term_id": scan.term_id, "scan_id": scan.id}
                for mention_id in mentions
            ]
            for offset in range(0, len(links), CHUNK):
                connection.execute(
                    postgresql.insert(MentionTerms).on_conflict_do_nothing(),
                    links[offset : offset + CHUNK],
                )
            connection.execute(
                sa.update(Scans)
                .where(Scans.id == scan.id)
                .values(
                    status=status,
                    reported_total=reported_total,
                    fetched=fetched,
                    matched=len(mentions),
                    claimed_until=None,
                    error=None,
                    updated_at=now,
                )
            )
            if children:
                connection.execute(
                    postgresql.insert(Scans).on_conflict_do_nothing(),
                    [
                        {
                            "platform": platform,
                            "channel": scan.channel,
                            "term_id": scan.term_id,
                            "window_start": start,
                            "window_end": end,
                            "parent_id": scan.id,
                            "status": "pending",
                            "updated_at": now,
                        }
                        for start, end in children
                    ],
                )

    # Volumes

    def missing_volumes(
        self, platform: str, channels: Sequence[str], start: date, end: date
    ) -> list[tuple[str, date]]:
        """List channel months without a stored baseline."""
        with self.transaction() as connection:
            known = {
                (channel, month)
                for channel, month in connection.execute(
                    sa.select(Volumes.channel, Volumes.month).where(
                        Volumes.platform == platform
                    )
                )
            }
        return [
            (channel, month)
            for channel in channels
            for month in months(start, end)
            if (channel, month) not in known
        ]

    def save_volume(
        self,
        platform: str,
        channel: str,
        month: date,
        page: Page,
        raw_page_id: int,
        now: datetime,
    ) -> None:
        """Store a closed month's channel volume."""
        if page.total is None:
            raise ValueError("Volume page has no total")
        with self.transaction() as connection:
            connection.execute(
                postgresql.insert(Volumes)
                .values(
                    platform=platform,
                    channel=channel,
                    month=month,
                    total=page.total,
                    exact=page.exact,
                    raw_page_id=raw_page_id,
                    observed_at=now,
                )
                .on_conflict_do_nothing()
            )

    # Attention

    def aggregate(
        self,
        profile_id: str,
        channels: dict[str, tuple[str, ...]],
        start: date,
        end: date,
        now: datetime,
    ) -> int:
        """Rebuild monthly topic attention with explicit coverage."""
        month = sa.cast(
            sa.func.date_trunc("month", sa.func.timezone("UTC", Mentions.created_at)),
            sa.Date,
        )
        # One row per topic and mention, whatever the number of matched terms.
        topic_mentions = (
            sa.select(ProfileTerms.topic_id, MentionTerms.mention_id)
            .join(MentionTerms, MentionTerms.term_id == ProfileTerms.term_id)
            .where(ProfileTerms.profile_id == profile_id)
            .distinct()
            .subquery()
        )
        with self.transaction() as connection:
            counts = {
                (row.topic_id, row.platform, row.channel, row.month): row
                for row in connection.execute(
                    sa.select(
                        topic_mentions.c.topic_id,
                        Mentions.platform,
                        Mentions.channel,
                        month.label("month"),
                        sa.func.count().label("mentions"),
                        sa.func.count(sa.distinct(Mentions.author_hash)).label(
                            "authors"
                        ),
                        sa.func.coalesce(
                            sa.func.sum(Mentions.engagement_total), 0
                        ).label("engagement"),
                    )
                    .join(Mentions, Mentions.id == topic_mentions.c.mention_id)
                    .where(
                        Mentions.created_at >= start,
                        Mentions.created_at < end,
                    )
                    .group_by(
                        topic_mentions.c.topic_id,
                        Mentions.platform,
                        Mentions.channel,
                        month,
                    )
                )
            }
            topic_terms: dict[str, list[str]] = defaultdict(list)
            for topic, term in connection.execute(
                sa.select(ProfileTerms.topic_id, ProfileTerms.term_id).where(
                    ProfileTerms.profile_id == profile_id
                )
            ):
                topic_terms[topic].append(term)
            # Terminal scans define which term-months were actually observed.
            observed: dict[tuple[str, str, str, date], tuple[str, int | None]] = {}
            for scan in connection.execute(
                sa.select(
                    Scans.platform,
                    Scans.channel,
                    Scans.term_id,
                    Scans.window_start,
                    Scans.window_end,
                    Scans.status,
                    Scans.reported_total,
                ).where(
                    Scans.status.in_(("complete", "truncated")),
                    Scans.term_id.in_(
                        sa.select(ProfileTerms.term_id).where(
                            ProfileTerms.profile_id == profile_id
                        )
                    ),
                )
            ):
                for covered in months(scan.window_start, scan.window_end):
                    observed[(scan.platform, scan.channel, scan.term_id, covered)] = (
                        scan.status,
                        scan.reported_total,
                    )
            volumes = {
                (row.platform, row.channel, row.month): row.total
                for row in connection.execute(
                    sa.select(
                        Volumes.platform, Volumes.channel, Volumes.month, Volumes.total
                    )
                )
            }
            rows = []
            for topic, terms in topic_terms.items():
                for platform, platform_channels in channels.items():
                    for channel in platform_channels:
                        for current in months(start, end):
                            states = [
                                observed.get((platform, channel, term, current))
                                for term in terms
                            ]
                            known = [state for state in states if state]
                            if not known:
                                coverage = "missing"
                            elif len(known) < len(states):
                                coverage = "partial"
                            elif any(status == "truncated" for status, _ in known):
                                coverage = "truncated"
                            else:
                                coverage = "complete"
                            count = counts.get((topic, platform, channel, current))
                            mentions = count.mentions if count else 0
                            estimated = None
                            if known:
                                estimated = max(
                                    [mentions]
                                    + [
                                        total or 0
                                        for status, total in known
                                        if status == "truncated"
                                    ]
                                )
                            volume = volumes.get((platform, channel, current))
                            rows.append(
                                {
                                    "profile_id": profile_id,
                                    "topic_id": topic,
                                    "platform": platform,
                                    "channel": channel,
                                    "month": current,
                                    "mentions": mentions,
                                    "authors": count.authors if count else 0,
                                    "engagement": count.engagement if count else 0,
                                    "estimated": estimated,
                                    "volume": volume,
                                    "share": estimated / volume
                                    if volume and estimated is not None
                                    else None,
                                    "coverage": coverage,
                                    "computed_at": now,
                                }
                            )
            connection.execute(
                sa.delete(Attention).where(Attention.profile_id == profile_id)
            )
            for offset in range(0, len(rows), CHUNK):
                connection.execute(sa.insert(Attention), rows[offset : offset + CHUNK])
        return len(rows)
