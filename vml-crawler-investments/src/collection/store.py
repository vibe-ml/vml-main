"""PostgreSQL persistence for scans, raw pages, articles, and funding events."""

import hashlib
import json
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import sqlalchemy as sa
from alembic import command
from sqlalchemy import Connection, create_engine
from sqlalchemy.dialects import postgresql

from src.collection.extraction import EXTRACTOR, event_id, extract
from src.collection.gdelt import Article, Page
from src.collection.terms import matches
from src.collection.windows import month_start
from src.common.settings import Settings
from src.migrations import configuration
from src.models import social
from src.models.investments import (
    Articles,
    ArticleTerms,
    EventArticles,
    Events,
    RawPages,
    Runs,
    Scans,
)

CHUNK = 1000
MAX_ATTEMPTS = 3


@dataclass(frozen=True)
class ClaimedScan:
    """A leased scan with its phrase."""

    id: int
    term_id: str
    language: str
    normalized: str
    window_start: date
    window_end: date


def article_id(url: str) -> str:
    """Stable article identity."""
    return hashlib.sha256(url.encode()).hexdigest()


def clean(value: Any) -> Any:
    """Drop NUL characters that PostgreSQL JSONB rejects."""
    return json.loads(json.dumps(value).replace("\\u0000", ""))


class Catalog:
    """Own the investments namespace; read the social profile without writing it."""

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
                    "investments": self.settings.schema_investments,
                    "social_src": self.settings.social_schema,
                }
            )

    def migrate(self) -> None:
        """Upgrade the investments schema through Alembic."""
        config = configuration(self.settings)
        with self.engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "head")

    def start_run(self, now: datetime) -> uuid.UUID:
        """Register a running collection."""
        run_id = uuid.uuid4()
        with self.transaction() as connection:
            connection.execute(
                sa.insert(Runs).values(id=run_id, started_at=now, status="running")
            )
        return run_id

    def finish_run(
        self, run_id: uuid.UUID, now: datetime, requests: int, error: str | None = None
    ) -> None:
        """Close a collection as complete or failed."""
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

    def profile_terms(self) -> list[tuple[str, str]]:
        """Terms of the newest social profile; short keywords are skipped."""
        profiles, links, terms = social.profiles, social.profile_terms, social.terms
        with self.transaction() as connection:
            profile_id = connection.execute(
                sa.select(profiles.c.id)
                .order_by(profiles.c.created_at.desc(), profiles.c.id)
                .limit(1)
            ).scalar()
            if profile_id is None:
                raise LookupError("No vml-crawler-social profile yet")
            rows = connection.execute(
                sa.select(terms.c.id, terms.c.normalized, links.c.origin)
                .join(links, links.c.term_id == terms.c.id)
                .where(links.c.profile_id == profile_id)
            ).all()
        selected = {
            identity: normalized
            for identity, normalized, origin in rows
            if origin == "display_name"
            or len(normalized.split()) >= self.settings.min_keyword_words
        }
        return sorted(selected.items())

    def plan(
        self,
        terms: Sequence[tuple[str, str]],
        languages: Sequence[str],
        end: date,
        now: datetime,
    ) -> int:
        """Add root windows per term and language from the covered boundary."""
        start = month_start(self.settings.collect_from)
        with self.transaction() as connection:
            boundaries = {
                (term, language): boundary
                for term, language, boundary in connection.execute(
                    sa.select(
                        Scans.term_id, Scans.language, sa.func.max(Scans.window_end)
                    )
                    .where(Scans.parent_id.is_(None))
                    .group_by(Scans.term_id, Scans.language)
                )
            }
            rows = [
                {
                    "term_id": identity,
                    "language": language,
                    "term": normalized,
                    "normalized": normalized,
                    "window_start": boundary,
                    "window_end": end,
                    "status": "pending",
                    "updated_at": now,
                }
                for identity, normalized in terms
                for language in languages
                if (boundary := max(boundaries.get((identity, language), start), start))
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
        term_ids: Sequence[str],
        lease_seconds: int,
        now: datetime,
        retry_before: datetime,
    ) -> ClaimedScan | None:
        """Lease the oldest runnable scan; failed scans wait for a later run."""
        with self.transaction() as connection:
            row = connection.execute(
                sa.select(
                    Scans.id,
                    Scans.term_id,
                    Scans.language,
                    Scans.normalized,
                    Scans.window_start,
                    Scans.window_end,
                )
                .where(
                    Scans.term_id.in_(term_ids),
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
                .with_for_update(skip_locked=True)
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
            row.term_id,
            row.language,
            row.normalized,
            row.window_start,
            row.window_end,
        )

    def release(self, scan_id: int, now: datetime) -> None:
        """Return an unfinished scan without spending an attempt."""
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
        """Record a failed attempt."""
        with self.transaction() as connection:
            connection.execute(
                sa.update(Scans)
                .where(Scans.id == scan_id)
                .values(
                    status="failed", error=error, claimed_until=None, updated_at=now
                )
            )

    def raw_page(
        self, run_id: uuid.UUID, scan_id: int, page: Page, now: datetime
    ) -> int:
        """Persist a response before interpreting it."""
        with self.transaction() as connection:
            return connection.execute(
                sa.insert(RawPages)
                .values(
                    run_id=run_id,
                    scan_id=scan_id,
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
        status: str,
        articles: Sequence[Article],
        raw_page_id: int,
        children: Sequence[tuple[date, date]],
        now: datetime,
    ) -> int:
        """Store articles, extracted events, and the scan outcome atomically."""
        unique = {article_id(item.url): item for item in articles}
        article_rows, term_rows, event_rows, links = [], [], {}, []
        for identity, item in unique.items():
            article_rows.append(
                {
                    "id": identity,
                    "url": item.url,
                    "title": item.title,
                    "seen_at": item.seen_at,
                    "domain": item.domain,
                    "language": item.language,
                    "source_country": item.source_country,
                    "raw_page_id": raw_page_id,
                    "first_seen_at": now,
                }
            )
            term_rows.append(
                {
                    "article_id": identity,
                    "term_id": scan.term_id,
                    "scan_id": scan.id,
                    "in_title": matches(scan.normalized, item.title),
                }
            )
            event = extract(item.title)
            if event is None:
                continue
            month = month_start(item.seen_at.date())
            key = event_id(event, month, identity)
            event_rows.setdefault(
                key,
                {
                    "id": key,
                    "event_type": event.event_type,
                    "company": event.company,
                    "stage": event.stage,
                    "amount": event.amount,
                    "currency": event.currency,
                    "investors": event.investors,
                    "month": month,
                    "extractor": EXTRACTOR,
                    "first_seen_at": now,
                },
            )
            links.append({"event_id": key, "article_id": identity})
        with self.transaction() as connection:
            for table, rows in (
                (Articles, article_rows),
                (ArticleTerms, term_rows),
                (Events, list(event_rows.values())),
                (EventArticles, links),
            ):
                for offset in range(0, len(rows), CHUNK):
                    connection.execute(
                        postgresql.insert(table).on_conflict_do_nothing(),
                        rows[offset : offset + CHUNK],
                    )
            connection.execute(
                sa.update(Scans)
                .where(Scans.id == scan.id)
                .values(
                    status=status,
                    fetched=len(unique),
                    events=len(links),
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
                            "term_id": scan.term_id,
                            "language": scan.language,
                            "term": scan.normalized,
                            "normalized": scan.normalized,
                            "window_start": start,
                            "window_end": end,
                            "parent_id": scan.id,
                            "status": "pending",
                            "updated_at": now,
                        }
                        for start, end in children
                    ],
                )
        return len(links)
