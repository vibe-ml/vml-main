"""SQLAlchemy models for the social schema: profiles, scans, mentions, attention."""

import uuid
from datetime import date, datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Mapped, mapped_column

from src.models.base import Base

SCAN_STATUSES = ("pending", "claimed", "complete", "split", "truncated", "failed")


class Profiles(Base):
    """Versioned search profiles derived from OpenAlex topics."""

    __tablename__ = "profiles"
    __table_args__ = {"schema": "social", "comment": "Versioned search profiles derived from OpenAlex topics"}  # noqa: E501

    id: Mapped[str] = mapped_column(sa.Text(), primary_key=True, comment="SHA-256 hash of topics and terms")  # noqa: E501
    created_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False, comment="Profile creation timestamp")  # noqa: E501
    bundle_id: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Source OpenAlex taxonomy bundle identifier")  # noqa: E501
    definition: Mapped[dict[str, Any]] = mapped_column(postgresql.JSONB(), nullable=False, comment="Selection settings used to build the profile")  # noqa: E501


class ProfileTopics(Base):
    """OpenAlex topics selected into a profile with their hierarchy."""

    __tablename__ = "profile_topics"
    __table_args__ = {"schema": "social", "comment": "OpenAlex topics selected into a profile"}  # noqa: E501

    profile_id: Mapped[str] = mapped_column(sa.Text(), sa.ForeignKey("social.profiles.id"), primary_key=True, comment="Profile identifier")  # noqa: E501
    topic_id: Mapped[str] = mapped_column(sa.Text(), primary_key=True, comment="OpenAlex topic identifier")  # noqa: E501
    topic_name: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="OpenAlex topic display name")  # noqa: E501
    subfield_name: Mapped[str | None] = mapped_column(sa.Text(), nullable=True, comment="OpenAlex subfield display name")  # noqa: E501
    field_name: Mapped[str | None] = mapped_column(sa.Text(), nullable=True, comment="OpenAlex field display name")  # noqa: E501
    domain_name: Mapped[str | None] = mapped_column(sa.Text(), nullable=True, comment="OpenAlex domain display name")  # noqa: E501
    work_count: Mapped[int] = mapped_column(sa.Integer(), nullable=False, comment="Current in-scope works with this primary topic")  # noqa: E501


class Terms(Base):
    """Normalized search phrases shared across profiles."""

    __tablename__ = "terms"
    __table_args__ = {"schema": "social", "comment": "Normalized search phrases shared across profiles"}  # noqa: E501

    id: Mapped[str] = mapped_column(sa.Text(), primary_key=True, comment="SHA-256 hash of the normalized phrase")  # noqa: E501
    term: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Search phrase as sent to platforms")  # noqa: E501
    normalized: Mapped[str] = mapped_column(sa.Text(), nullable=False, unique=True, comment="Lowercase alphanumeric phrase used for local matching")  # noqa: E501


class ProfileTerms(Base):
    """Mapping from profile topics to search terms."""

    __tablename__ = "profile_terms"
    __table_args__ = (
        sa.ForeignKeyConstraint(["profile_id", "topic_id"], ["social.profile_topics.profile_id", "social.profile_topics.topic_id"]),  # noqa: E501
        {"schema": "social", "comment": "Mapping from profile topics to search terms"},
    )

    profile_id: Mapped[str] = mapped_column(sa.Text(), primary_key=True, comment="Profile identifier")  # noqa: E501
    topic_id: Mapped[str] = mapped_column(sa.Text(), primary_key=True, comment="OpenAlex topic identifier")  # noqa: E501
    term_id: Mapped[str] = mapped_column(sa.Text(), sa.ForeignKey("social.terms.id"), primary_key=True, comment="Search term identifier")  # noqa: E501
    origin: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Term origin (display_name, keyword)")  # noqa: E501


class Runs(Base):
    """Collection op executions and request usage."""

    __tablename__ = "runs"
    __table_args__ = (
        sa.CheckConstraint("status IN ('running', 'failed', 'complete')", name="runs_status_check"),  # noqa: E501
        {"schema": "social", "comment": "Collection op executions and request usage"},
    )

    id: Mapped[uuid.UUID] = mapped_column(sa.Uuid(), primary_key=True, comment="Run identifier")  # noqa: E501
    stage: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Pipeline stage (profile, collect, volume, aggregate)")  # noqa: E501
    platform: Mapped[str | None] = mapped_column(sa.Text(), nullable=True, comment="Platform for collection stages")  # noqa: E501
    started_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False, comment="Run start timestamp")  # noqa: E501
    ended_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True), nullable=True, comment="Run completion timestamp")  # noqa: E501
    status: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Execution status (running, failed, complete)")  # noqa: E501
    requests: Mapped[int] = mapped_column(sa.Integer(), nullable=False, server_default="0", comment="HTTP requests issued")  # noqa: E501
    error: Mapped[str | None] = mapped_column(sa.Text(), nullable=True, comment="Exception class if the run failed")  # noqa: E501


class Scans(Base):
    """Resumable search windows per platform channel and term."""

    __tablename__ = "scans"
    __table_args__ = (
        sa.UniqueConstraint("platform", "channel", "term_id", "window_start", "window_end", name="scans_window_key"),  # noqa: E501
        sa.CheckConstraint("status IN ('pending', 'claimed', 'complete', 'split', 'truncated', 'failed')", name="scans_status_check"),  # noqa: E501
        sa.CheckConstraint("window_start < window_end", name="scans_window_check"),
        sa.Index("scans_queue_idx", "platform", "status"),
        {"schema": "social", "comment": "Resumable search windows per platform channel and term"},  # noqa: E501
    )

    id: Mapped[int] = mapped_column(sa.BigInteger(), sa.Identity(always=True), primary_key=True, comment="Scan identifier")  # noqa: E501
    platform: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Platform name")  # noqa: E501
    channel: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Platform channel (site, feed)")  # noqa: E501
    term_id: Mapped[str] = mapped_column(sa.Text(), sa.ForeignKey("social.terms.id"), nullable=False, comment="Search term identifier")  # noqa: E501
    window_start: Mapped[date] = mapped_column(sa.Date(), nullable=False, comment="Inclusive window start (first day of month)")  # noqa: E501
    window_end: Mapped[date] = mapped_column(sa.Date(), nullable=False, comment="Exclusive window end (first day of month)")  # noqa: E501
    parent_id: Mapped[int | None] = mapped_column(sa.BigInteger(), sa.ForeignKey("social.scans.id"), nullable=True, comment="Split parent scan identifier")  # noqa: E501
    status: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Scan status (pending, claimed, complete, split, truncated, failed)")  # noqa: E501
    reported_total: Mapped[int | None] = mapped_column(sa.Integer(), nullable=True, comment="Hit count reported by the platform")  # noqa: E501
    fetched: Mapped[int] = mapped_column(sa.Integer(), nullable=False, server_default="0", comment="Items fetched from the platform")  # noqa: E501
    matched: Mapped[int] = mapped_column(sa.Integer(), nullable=False, server_default="0", comment="Items whose text contains the term")  # noqa: E501
    attempts: Mapped[int] = mapped_column(sa.Integer(), nullable=False, server_default="0", comment="Claim attempts")  # noqa: E501
    claimed_until: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True), nullable=True, comment="Lease expiry of the current claim")  # noqa: E501
    updated_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False, comment="Last status change timestamp")  # noqa: E501
    error: Mapped[str | None] = mapped_column(sa.Text(), nullable=True, comment="Exception class of the last failure")  # noqa: E501


class RawPages(Base):
    """Immutable platform responses with provenance."""

    __tablename__ = "raw_pages"
    __table_args__ = {"schema": "social", "comment": "Immutable platform responses with provenance"}  # noqa: E501

    id: Mapped[int] = mapped_column(sa.BigInteger(), sa.Identity(always=True), primary_key=True, comment="Raw page identifier")  # noqa: E501
    run_id: Mapped[uuid.UUID] = mapped_column(sa.Uuid(), sa.ForeignKey("social.runs.id"), nullable=False, comment="Originating run identifier")  # noqa: E501
    scan_id: Mapped[int | None] = mapped_column(sa.BigInteger(), sa.ForeignKey("social.scans.id"), nullable=True, comment="Originating scan identifier")  # noqa: E501
    platform: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Platform name")  # noqa: E501
    locator: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Request URL without credentials")  # noqa: E501
    status_code: Mapped[int] = mapped_column(sa.Integer(), nullable=False, comment="HTTP status code")  # noqa: E501
    fetched_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False, comment="Response timestamp")  # noqa: E501
    checksum: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Response body SHA-256 checksum")  # noqa: E501
    bytes: Mapped[int] = mapped_column(sa.Integer(), nullable=False, comment="Response body size")  # noqa: E501
    payload: Mapped[dict[str, Any] | None] = mapped_column(postgresql.JSONB(), nullable=True, comment="Parsed JSON body, null if not JSON")  # noqa: E501


class Mentions(Base):
    """Deduplicated social posts that contain at least one profile term."""

    __tablename__ = "mentions"
    __table_args__ = (
        sa.Index("mentions_created_idx", "platform", "channel", "created_at"),
        {"schema": "social", "comment": "Deduplicated social posts that contain at least one profile term"},  # noqa: E501
    )

    id: Mapped[str] = mapped_column(sa.Text(), primary_key=True, comment="Platform-qualified native identifier")  # noqa: E501
    platform: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Platform name")  # noqa: E501
    channel: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Platform channel (site, feed)")  # noqa: E501
    native_id: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Identifier on the platform")  # noqa: E501
    kind: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Item kind (story, comment, question, post)")  # noqa: E501
    url: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Public item URL")  # noqa: E501
    author_hash: Mapped[str | None] = mapped_column(sa.Text(), nullable=True, comment="SHA-256 hash of the platform author identifier")  # noqa: E501
    created_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False, comment="Item publication timestamp")  # noqa: E501
    title: Mapped[str | None] = mapped_column(sa.Text(), nullable=True, comment="Item or parent title")  # noqa: E501
    text: Mapped[str | None] = mapped_column(sa.Text(), nullable=True, comment="Plain item text, truncated")  # noqa: E501
    lang: Mapped[str | None] = mapped_column(sa.Text(), nullable=True, comment="Declared item language")  # noqa: E501
    engagement: Mapped[dict[str, Any]] = mapped_column(postgresql.JSONB(), nullable=False, comment="Platform engagement counters at first sighting")  # noqa: E501
    engagement_total: Mapped[int] = mapped_column(sa.Integer(), nullable=False, comment="Sum of engagement counters")  # noqa: E501
    trust_level: Mapped[str] = mapped_column(sa.Text(), nullable=False, server_default="low", comment="Source trust level (GPB hierarchy)")  # noqa: E501
    content_hash: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="SHA-256 hash of title and text")  # noqa: E501
    raw_page_id: Mapped[int] = mapped_column(sa.BigInteger(), sa.ForeignKey("social.raw_pages.id"), nullable=False, comment="First raw page containing the item")  # noqa: E501
    first_seen_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False, comment="First retrieval timestamp")  # noqa: E501
    last_seen_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False, comment="Latest retrieval timestamp")  # noqa: E501


class MentionTerms(Base):
    """Terms matched in each mention."""

    __tablename__ = "mention_terms"
    __table_args__ = (
        sa.Index("mention_terms_term_idx", "term_id"),
        {"schema": "social", "comment": "Terms matched in each mention"},
    )

    mention_id: Mapped[str] = mapped_column(sa.Text(), sa.ForeignKey("social.mentions.id"), primary_key=True, comment="Mention identifier")  # noqa: E501
    term_id: Mapped[str] = mapped_column(sa.Text(), sa.ForeignKey("social.terms.id"), primary_key=True, comment="Matched term identifier")  # noqa: E501
    scan_id: Mapped[int] = mapped_column(sa.BigInteger(), sa.ForeignKey("social.scans.id"), nullable=False, comment="First scan that matched the term")  # noqa: E501


class Volumes(Base):
    """Monthly platform activity baselines for share normalization."""

    __tablename__ = "volumes"
    __table_args__ = {"schema": "social", "comment": "Monthly platform activity baselines for share normalization"}  # noqa: E501

    platform: Mapped[str] = mapped_column(sa.Text(), primary_key=True, comment="Platform name")  # noqa: E501
    channel: Mapped[str] = mapped_column(sa.Text(), primary_key=True, comment="Platform channel (site, feed)")  # noqa: E501
    month: Mapped[date] = mapped_column(sa.Date(), primary_key=True, comment="First day of month")  # noqa: E501
    total: Mapped[int] = mapped_column(sa.Integer(), nullable=False, comment="Items published on the channel in the month")  # noqa: E501
    exact: Mapped[bool] = mapped_column(sa.Boolean(), nullable=False, comment="Whether the platform reported an exhaustive count")  # noqa: E501
    raw_page_id: Mapped[int] = mapped_column(sa.BigInteger(), sa.ForeignKey("social.raw_pages.id"), nullable=False, comment="Raw page with the count")  # noqa: E501
    observed_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False, comment="Observation timestamp")  # noqa: E501


class Attention(Base):
    """Monthly topic attention per platform channel with coverage."""

    __tablename__ = "attention"
    __table_args__ = (
        sa.CheckConstraint("coverage IN ('complete', 'truncated', 'partial', 'missing')", name="attention_coverage_check"),  # noqa: E501
        {"schema": "social", "comment": "Monthly topic attention per platform channel with coverage"},  # noqa: E501
    )

    profile_id: Mapped[str] = mapped_column(sa.Text(), sa.ForeignKey("social.profiles.id"), primary_key=True, comment="Profile identifier")  # noqa: E501
    topic_id: Mapped[str] = mapped_column(sa.Text(), primary_key=True, comment="OpenAlex topic identifier")  # noqa: E501
    platform: Mapped[str] = mapped_column(sa.Text(), primary_key=True, comment="Platform name")  # noqa: E501
    channel: Mapped[str] = mapped_column(sa.Text(), primary_key=True, comment="Platform channel (site, feed)")  # noqa: E501
    month: Mapped[date] = mapped_column(sa.Date(), primary_key=True, comment="First day of month")  # noqa: E501
    mentions: Mapped[int] = mapped_column(sa.Integer(), nullable=False, comment="Distinct matched mentions")  # noqa: E501
    authors: Mapped[int] = mapped_column(sa.Integer(), nullable=False, comment="Distinct mention authors")  # noqa: E501
    engagement: Mapped[int] = mapped_column(sa.BigInteger(), nullable=False, comment="Sum of mention engagement")  # noqa: E501
    estimated: Mapped[int | None] = mapped_column(sa.Integer(), nullable=True, comment="Lower-bound mentions including platform totals of truncated windows")  # noqa: E501
    volume: Mapped[int | None] = mapped_column(sa.Integer(), nullable=True, comment="Channel monthly volume")  # noqa: E501
    share: Mapped[float | None] = mapped_column(sa.Double(), nullable=True, comment="Estimated mentions divided by volume")  # noqa: E501
    coverage: Mapped[str] = mapped_column(sa.Text(), nullable=False, comment="Coverage (complete, truncated, partial, missing)")  # noqa: E501
    computed_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False, comment="Aggregation timestamp")  # noqa: E501
