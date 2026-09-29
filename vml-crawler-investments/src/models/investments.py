"""SQLAlchemy models for the investments schema: scans, articles, funding events."""

import uuid
from datetime import date, datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base


class Runs(Base):
    """Collection op executions and request usage."""

    __tablename__ = "runs"
    __table_args__ = (
        sa.CheckConstraint("status IN ('running', 'failed', 'complete')", name="runs_status_check"),  # noqa: E501
        {"schema": "investments", "comment": "Collection op executions and request usage"},  # noqa: E501
    )

    id: Mapped[uuid.UUID] = mapped_column(sa.Uuid(), primary_key=True, comment="Run identifier")  # noqa: E501
    started_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False, comment="Run start timestamp")  # noqa: E501
    ended_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True), nullable=True, comment="Run completion timestamp")  # noqa: E501
    status: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Execution status (running, failed, complete)")  # noqa: E501
    requests: Mapped[int] = mapped_column(sa.Integer(), nullable=False, server_default="0", comment="HTTP requests issued")  # noqa: E501
    error: Mapped[str | None] = mapped_column(sa.Text(), nullable=True, comment="Exception class if the run failed")  # noqa: E501


class Scans(Base):
    """Resumable news search windows per profile term."""

    __tablename__ = "scans"
    __table_args__ = (
        sa.UniqueConstraint("term_id", "language", "window_start", "window_end", name="scans_language_window_key"),  # noqa: E501
        sa.CheckConstraint("status IN ('pending', 'claimed', 'complete', 'split', 'truncated', 'failed')", name="scans_status_check"),  # noqa: E501
        sa.CheckConstraint("window_start < window_end", name="scans_window_check"),
        sa.Index("scans_queue_idx", "status"),
        {"schema": "investments", "comment": "Resumable news search windows per profile term"},  # noqa: E501
    )

    id: Mapped[int] = mapped_column(sa.BigInteger(), sa.Identity(always=True), primary_key=True, comment="Scan identifier")  # noqa: E501
    term_id: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="vml-crawler-social term identifier")  # noqa: E501
    language: Mapped[str] = mapped_column(sa.Text(), nullable=False, server_default="english", comment="GDELT source language")  # noqa: E501
    term: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Search phrase")  # noqa: E501
    normalized: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Normalized phrase for local matching")  # noqa: E501
    window_start: Mapped[date] = mapped_column(sa.Date(), nullable=False, comment="Inclusive window start (first day of month)")  # noqa: E501
    window_end: Mapped[date] = mapped_column(sa.Date(), nullable=False, comment="Exclusive window end (first day of month)")  # noqa: E501
    parent_id: Mapped[int | None] = mapped_column(sa.BigInteger(), sa.ForeignKey("investments.scans.id"), nullable=True, comment="Split parent scan identifier")  # noqa: E501
    status: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Scan status (pending, claimed, complete, split, truncated, failed)")  # noqa: E501
    fetched: Mapped[int] = mapped_column(sa.Integer(), nullable=False, server_default="0", comment="Articles returned by the source")  # noqa: E501
    events: Mapped[int] = mapped_column(sa.Integer(), nullable=False, server_default="0", comment="Articles recognized as funding events")  # noqa: E501
    attempts: Mapped[int] = mapped_column(sa.Integer(), nullable=False, server_default="0", comment="Claim attempts")  # noqa: E501
    claimed_until: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True), nullable=True, comment="Lease expiry of the current claim")  # noqa: E501
    updated_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False, comment="Last status change timestamp")  # noqa: E501
    error: Mapped[str | None] = mapped_column(sa.Text(), nullable=True, comment="Exception class of the last failure")  # noqa: E501


class RawPages(Base):
    """Immutable source responses with provenance."""

    __tablename__ = "raw_pages"
    __table_args__ = {"schema": "investments", "comment": "Immutable source responses with provenance"}  # noqa: E501

    id: Mapped[int] = mapped_column(sa.BigInteger(), sa.Identity(always=True), primary_key=True, comment="Raw page identifier")  # noqa: E501
    run_id: Mapped[uuid.UUID] = mapped_column(sa.Uuid(), sa.ForeignKey("investments.runs.id"), nullable=False, comment="Originating run identifier")  # noqa: E501
    scan_id: Mapped[int] = mapped_column(sa.BigInteger(), sa.ForeignKey("investments.scans.id"), nullable=False, comment="Originating scan identifier")  # noqa: E501
    locator: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Request URL")  # noqa: E501
    status_code: Mapped[int] = mapped_column(sa.Integer(), nullable=False, comment="HTTP status code")  # noqa: E501
    fetched_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False, comment="Response timestamp")  # noqa: E501
    checksum: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Response body SHA-256 checksum")  # noqa: E501
    bytes: Mapped[int] = mapped_column(sa.Integer(), nullable=False, comment="Response body size")  # noqa: E501
    payload: Mapped[dict[str, Any] | None] = mapped_column(postgresql.JSONB(), nullable=True, comment="Parsed JSON body, null if not JSON")  # noqa: E501


class Articles(Base):
    """Deduplicated news articles returned for profile terms."""

    __tablename__ = "articles"
    __table_args__ = {"schema": "investments", "comment": "Deduplicated news articles returned for profile terms"}  # noqa: E501

    id: Mapped[str] = mapped_column(sa.Text(), primary_key=True, comment="SHA-256 hash of the article URL")  # noqa: E501
    url: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Article URL")
    title: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Article title")  # noqa: E501
    seen_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False, comment="First time the source saw the article")  # noqa: E501
    domain: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Publisher domain")  # noqa: E501
    language: Mapped[str | None] = mapped_column(sa.Text(), nullable=True, comment="Article language")  # noqa: E501
    source_country: Mapped[str | None] = mapped_column(sa.Text(), nullable=True, comment="Publisher country")  # noqa: E501
    trust_level: Mapped[str] = mapped_column(sa.Text(), nullable=False, server_default="low", comment="Source trust level (GPB hierarchy)")  # noqa: E501
    raw_page_id: Mapped[int] = mapped_column(sa.BigInteger(), sa.ForeignKey("investments.raw_pages.id"), nullable=False, comment="First raw page containing the article")  # noqa: E501
    first_seen_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False, comment="First retrieval timestamp")  # noqa: E501


class ArticleTerms(Base):
    """Profile terms that returned each article."""

    __tablename__ = "article_terms"
    __table_args__ = (
        sa.Index("article_terms_term_idx", "term_id"),
        {"schema": "investments", "comment": "Profile terms that returned each article"},  # noqa: E501
    )

    article_id: Mapped[str] = mapped_column(sa.Text(), sa.ForeignKey("investments.articles.id"), primary_key=True, comment="Article identifier")  # noqa: E501
    term_id: Mapped[str] = mapped_column(sa.Text(), primary_key=True, comment="vml-crawler-social term identifier")  # noqa: E501
    scan_id: Mapped[int] = mapped_column(sa.BigInteger(), sa.ForeignKey("investments.scans.id"), nullable=False, comment="First scan that returned the article")  # noqa: E501
    in_title: Mapped[bool] = mapped_column(sa.Boolean(), nullable=False, comment="Whether the title contains the term (strong link)")  # noqa: E501


class Events(Base):
    """Funding events extracted from headlines; reprints share one event."""

    __tablename__ = "events"
    __table_args__ = (
        sa.CheckConstraint("event_type IN ('vc_round', 'm_and_a', 'ipo', 'grant')", name="events_type_check"),  # noqa: E501
        sa.Index("events_month_idx", "month"),
        {"schema": "investments", "comment": "Funding events extracted from headlines"},
    )

    id: Mapped[str] = mapped_column(sa.Text(), primary_key=True, comment="Hash of company, type, stage, amount, and month (origin group)")  # noqa: E501
    event_type: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Event type (vc_round, m_and_a, ipo, grant)")  # noqa: E501
    company: Mapped[str | None] = mapped_column(sa.Text(), nullable=True, comment="Company named in the headline")  # noqa: E501
    stage: Mapped[str | None] = mapped_column(sa.Text(), nullable=True, comment="Round stage (pre_seed, seed, series_a, ...)")  # noqa: E501
    amount: Mapped[float | None] = mapped_column(sa.Double(), nullable=True, comment="Announced amount in currency units")  # noqa: E501
    currency: Mapped[str | None] = mapped_column(sa.Text(), nullable=True, comment="ISO currency code")  # noqa: E501
    investors: Mapped[str | None] = mapped_column(sa.Text(), nullable=True, comment="Lead investors named in the headline")  # noqa: E501
    month: Mapped[date] = mapped_column(sa.Date(), nullable=False, comment="First day of the announcement month")  # noqa: E501
    extractor: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Extraction rule version")  # noqa: E501
    first_seen_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False, comment="First extraction timestamp")  # noqa: E501


class EventArticles(Base):
    """Articles reporting each funding event."""

    __tablename__ = "event_articles"
    __table_args__ = {"schema": "investments", "comment": "Articles reporting each funding event"}  # noqa: E501

    event_id: Mapped[str] = mapped_column(sa.Text(), sa.ForeignKey("investments.events.id"), primary_key=True, comment="Event identifier")  # noqa: E501
    article_id: Mapped[str] = mapped_column(sa.Text(), sa.ForeignKey("investments.articles.id"), primary_key=True, comment="Article identifier")  # noqa: E501
