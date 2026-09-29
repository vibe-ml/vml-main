"""Create social profiles, scans, raw pages, mentions, volumes, and attention."""

# Retain the repository-required E501 exemptions on single-line declarations.
# ruff: noqa: RUF100

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.dialects import postgresql

from src.common.settings import Settings

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def get_schema() -> str:
    """Resolve the social namespace for this migration invocation."""
    settings = context.config.attributes.get("settings") or Settings()
    return settings.schema_social


def upgrade() -> None:
    """Create all social tables in one namespace."""
    s = get_schema()

    # fmt: off
    op.create_table(
        "profiles",
        sa.Column("id", sa.Text(), primary_key=True, comment="SHA-256 hash of topics and terms"),  # noqa: E501
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, comment="Profile creation timestamp"),  # noqa: E501
        sa.Column("bundle_id", sa.Text(), nullable=False, comment="Source OpenAlex taxonomy bundle identifier"),  # noqa: E501
        sa.Column("definition", postgresql.JSONB(), nullable=False, comment="Selection settings used to build the profile"),  # noqa: E501
        schema=s, comment="Versioned search profiles derived from OpenAlex topics",
    )
    op.create_table(
        "profile_topics",
        sa.Column("profile_id", sa.Text(), sa.ForeignKey(f"{s}.profiles.id"), primary_key=True, comment="Profile identifier"),  # noqa: E501
        sa.Column("topic_id", sa.Text(), primary_key=True, comment="OpenAlex topic identifier"),  # noqa: E501
        sa.Column("topic_name", sa.Text(), nullable=False, comment="OpenAlex topic display name"),  # noqa: E501
        sa.Column("subfield_name", sa.Text(), nullable=True, comment="OpenAlex subfield display name"),  # noqa: E501
        sa.Column("field_name", sa.Text(), nullable=True, comment="OpenAlex field display name"),  # noqa: E501
        sa.Column("domain_name", sa.Text(), nullable=True, comment="OpenAlex domain display name"),  # noqa: E501
        sa.Column("work_count", sa.Integer(), nullable=False, comment="Current in-scope works with this primary topic"),  # noqa: E501
        schema=s, comment="OpenAlex topics selected into a profile",
    )
    op.create_table(
        "terms",
        sa.Column("id", sa.Text(), primary_key=True, comment="SHA-256 hash of the normalized phrase"),  # noqa: E501
        sa.Column("term", sa.Text(), nullable=False, comment="Search phrase as sent to platforms"),  # noqa: E501
        sa.Column("normalized", sa.Text(), nullable=False, unique=True, comment="Lowercase alphanumeric phrase used for local matching"),  # noqa: E501
        schema=s, comment="Normalized search phrases shared across profiles",
    )
    op.create_table(
        "profile_terms",
        sa.Column("profile_id", sa.Text(), primary_key=True, comment="Profile identifier"),  # noqa: E501
        sa.Column("topic_id", sa.Text(), primary_key=True, comment="OpenAlex topic identifier"),  # noqa: E501
        sa.Column("term_id", sa.Text(), sa.ForeignKey(f"{s}.terms.id"), primary_key=True, comment="Search term identifier"),  # noqa: E501
        sa.Column("origin", sa.Text(), nullable=False, comment="Term origin (display_name, keyword)"),  # noqa: E501
        sa.ForeignKeyConstraint(["profile_id", "topic_id"], [f"{s}.profile_topics.profile_id", f"{s}.profile_topics.topic_id"]),  # noqa: E501
        schema=s, comment="Mapping from profile topics to search terms",
    )
    op.create_table(
        "runs",
        sa.Column("id", sa.Uuid(), primary_key=True, comment="Run identifier"),  # noqa: E501
        sa.Column("stage", sa.Text(), nullable=False, comment="Pipeline stage (profile, collect, volume, aggregate)"),  # noqa: E501
        sa.Column("platform", sa.Text(), nullable=True, comment="Platform for collection stages"),  # noqa: E501
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, comment="Run start timestamp"),  # noqa: E501
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True, comment="Run completion timestamp"),  # noqa: E501
        sa.Column("status", sa.Text(), nullable=False, comment="Execution status (running, failed, complete)"),  # noqa: E501
        sa.Column("requests", sa.Integer(), nullable=False, server_default="0", comment="HTTP requests issued"),  # noqa: E501
        sa.Column("error", sa.Text(), nullable=True, comment="Exception class if the run failed"),  # noqa: E501
        sa.CheckConstraint("status IN ('running', 'failed', 'complete')", name="runs_status_check"),  # noqa: E501
        schema=s, comment="Collection op executions and request usage",
    )
    op.create_table(
        "scans",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), primary_key=True, comment="Scan identifier"),  # noqa: E501
        sa.Column("platform", sa.Text(), nullable=False, comment="Platform name"),  # noqa: E501
        sa.Column("channel", sa.Text(), nullable=False, comment="Platform channel (site, feed)"),  # noqa: E501
        sa.Column("term_id", sa.Text(), sa.ForeignKey(f"{s}.terms.id"), nullable=False, comment="Search term identifier"),  # noqa: E501
        sa.Column("window_start", sa.Date(), nullable=False, comment="Inclusive window start (first day of month)"),  # noqa: E501
        sa.Column("window_end", sa.Date(), nullable=False, comment="Exclusive window end (first day of month)"),  # noqa: E501
        sa.Column("parent_id", sa.BigInteger(), sa.ForeignKey(f"{s}.scans.id"), nullable=True, comment="Split parent scan identifier"),  # noqa: E501
        sa.Column("status", sa.Text(), nullable=False, comment="Scan status (pending, claimed, complete, split, truncated, failed)"),  # noqa: E501
        sa.Column("reported_total", sa.Integer(), nullable=True, comment="Hit count reported by the platform"),  # noqa: E501
        sa.Column("fetched", sa.Integer(), nullable=False, server_default="0", comment="Items fetched from the platform"),  # noqa: E501
        sa.Column("matched", sa.Integer(), nullable=False, server_default="0", comment="Items whose text contains the term"),  # noqa: E501
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0", comment="Claim attempts"),  # noqa: E501
        sa.Column("claimed_until", sa.DateTime(timezone=True), nullable=True, comment="Lease expiry of the current claim"),  # noqa: E501
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, comment="Last status change timestamp"),  # noqa: E501
        sa.Column("error", sa.Text(), nullable=True, comment="Exception class of the last failure"),  # noqa: E501
        sa.UniqueConstraint("platform", "channel", "term_id", "window_start", "window_end", name="scans_window_key"),  # noqa: E501
        sa.CheckConstraint("status IN ('pending', 'claimed', 'complete', 'split', 'truncated', 'failed')", name="scans_status_check"),  # noqa: E501
        sa.CheckConstraint("window_start < window_end", name="scans_window_check"),  # noqa: E501
        schema=s, comment="Resumable search windows per platform channel and term",
    )
    op.create_index("scans_queue_idx", "scans", ["platform", "status"], schema=s)
    op.create_table(
        "raw_pages",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), primary_key=True, comment="Raw page identifier"),  # noqa: E501
        sa.Column("run_id", sa.Uuid(), sa.ForeignKey(f"{s}.runs.id"), nullable=False, comment="Originating run identifier"),  # noqa: E501
        sa.Column("scan_id", sa.BigInteger(), sa.ForeignKey(f"{s}.scans.id"), nullable=True, comment="Originating scan identifier"),  # noqa: E501
        sa.Column("platform", sa.Text(), nullable=False, comment="Platform name"),  # noqa: E501
        sa.Column("locator", sa.Text(), nullable=False, comment="Request URL without credentials"),  # noqa: E501
        sa.Column("status_code", sa.Integer(), nullable=False, comment="HTTP status code"),  # noqa: E501
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False, comment="Response timestamp"),  # noqa: E501
        sa.Column("checksum", sa.Text(), nullable=False, comment="Response body SHA-256 checksum"),  # noqa: E501
        sa.Column("bytes", sa.Integer(), nullable=False, comment="Response body size"),  # noqa: E501
        sa.Column("payload", postgresql.JSONB(), nullable=True, comment="Parsed JSON body, null if not JSON"),  # noqa: E501
        schema=s, comment="Immutable platform responses with provenance",
    )
    op.create_table(
        "mentions",
        sa.Column("id", sa.Text(), primary_key=True, comment="Platform-qualified native identifier"),  # noqa: E501
        sa.Column("platform", sa.Text(), nullable=False, comment="Platform name"),  # noqa: E501
        sa.Column("channel", sa.Text(), nullable=False, comment="Platform channel (site, feed)"),  # noqa: E501
        sa.Column("native_id", sa.Text(), nullable=False, comment="Identifier on the platform"),  # noqa: E501
        sa.Column("kind", sa.Text(), nullable=False, comment="Item kind (story, comment, question, post)"),  # noqa: E501
        sa.Column("url", sa.Text(), nullable=False, comment="Public item URL"),  # noqa: E501
        sa.Column("author_hash", sa.Text(), nullable=True, comment="SHA-256 hash of the platform author identifier"),  # noqa: E501
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, comment="Item publication timestamp"),  # noqa: E501
        sa.Column("title", sa.Text(), nullable=True, comment="Item or parent title"),  # noqa: E501
        sa.Column("text", sa.Text(), nullable=True, comment="Plain item text, truncated"),  # noqa: E501
        sa.Column("lang", sa.Text(), nullable=True, comment="Declared item language"),  # noqa: E501
        sa.Column("engagement", postgresql.JSONB(), nullable=False, comment="Platform engagement counters at first sighting"),  # noqa: E501
        sa.Column("engagement_total", sa.Integer(), nullable=False, comment="Sum of engagement counters"),  # noqa: E501
        sa.Column("trust_level", sa.Text(), nullable=False, server_default="low", comment="Source trust level (GPB hierarchy)"),  # noqa: E501
        sa.Column("content_hash", sa.Text(), nullable=False, comment="SHA-256 hash of title and text"),  # noqa: E501
        sa.Column("raw_page_id", sa.BigInteger(), sa.ForeignKey(f"{s}.raw_pages.id"), nullable=False, comment="First raw page containing the item"),  # noqa: E501
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False, comment="First retrieval timestamp"),  # noqa: E501
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False, comment="Latest retrieval timestamp"),  # noqa: E501
        schema=s, comment="Deduplicated social posts that contain at least one profile term",  # noqa: E501
    )
    op.create_index("mentions_created_idx", "mentions", ["platform", "channel", "created_at"], schema=s)  # noqa: E501
    op.create_table(
        "mention_terms",
        sa.Column("mention_id", sa.Text(), sa.ForeignKey(f"{s}.mentions.id"), primary_key=True, comment="Mention identifier"),  # noqa: E501
        sa.Column("term_id", sa.Text(), sa.ForeignKey(f"{s}.terms.id"), primary_key=True, comment="Matched term identifier"),  # noqa: E501
        sa.Column("scan_id", sa.BigInteger(), sa.ForeignKey(f"{s}.scans.id"), nullable=False, comment="First scan that matched the term"),  # noqa: E501
        schema=s, comment="Terms matched in each mention",
    )
    op.create_index("mention_terms_term_idx", "mention_terms", ["term_id"], schema=s)
    op.create_table(
        "volumes",
        sa.Column("platform", sa.Text(), primary_key=True, comment="Platform name"),  # noqa: E501
        sa.Column("channel", sa.Text(), primary_key=True, comment="Platform channel (site, feed)"),  # noqa: E501
        sa.Column("month", sa.Date(), primary_key=True, comment="First day of month"),  # noqa: E501
        sa.Column("total", sa.Integer(), nullable=False, comment="Items published on the channel in the month"),  # noqa: E501
        sa.Column("exact", sa.Boolean(), nullable=False, comment="Whether the platform reported an exhaustive count"),  # noqa: E501
        sa.Column("raw_page_id", sa.BigInteger(), sa.ForeignKey(f"{s}.raw_pages.id"), nullable=False, comment="Raw page with the count"),  # noqa: E501
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False, comment="Observation timestamp"),  # noqa: E501
        schema=s, comment="Monthly platform activity baselines for share normalization",
    )
    op.create_table(
        "attention",
        sa.Column("profile_id", sa.Text(), sa.ForeignKey(f"{s}.profiles.id"), primary_key=True, comment="Profile identifier"),  # noqa: E501
        sa.Column("topic_id", sa.Text(), primary_key=True, comment="OpenAlex topic identifier"),  # noqa: E501
        sa.Column("platform", sa.Text(), primary_key=True, comment="Platform name"),  # noqa: E501
        sa.Column("channel", sa.Text(), primary_key=True, comment="Platform channel (site, feed)"),  # noqa: E501
        sa.Column("month", sa.Date(), primary_key=True, comment="First day of month"),  # noqa: E501
        sa.Column("mentions", sa.Integer(), nullable=False, comment="Distinct matched mentions"),  # noqa: E501
        sa.Column("authors", sa.Integer(), nullable=False, comment="Distinct mention authors"),  # noqa: E501
        sa.Column("engagement", sa.BigInteger(), nullable=False, comment="Sum of mention engagement"),  # noqa: E501
        sa.Column("estimated", sa.Integer(), nullable=True, comment="Lower-bound mentions including platform totals of truncated windows"),  # noqa: E501
        sa.Column("volume", sa.Integer(), nullable=True, comment="Channel monthly volume"),  # noqa: E501
        sa.Column("share", sa.Double(), nullable=True, comment="Estimated mentions divided by volume"),  # noqa: E501
        sa.Column("coverage", sa.Text(), nullable=False, comment="Coverage (complete, truncated, partial, missing)"),  # noqa: E501
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False, comment="Aggregation timestamp"),  # noqa: E501
        sa.CheckConstraint("coverage IN ('complete', 'truncated', 'partial', 'missing')", name="attention_coverage_check"),  # noqa: E501
        schema=s, comment="Monthly topic attention per platform channel with coverage",
    )
    # fmt: on


def downgrade() -> None:
    """Drop all social tables in dependency order."""
    s = get_schema()
    for table in (
        "attention",
        "volumes",
        "mention_terms",
        "mentions",
        "raw_pages",
        "scans",
        "runs",
        "profile_terms",
        "terms",
        "profile_topics",
        "profiles",
    ):
        op.drop_table(table, schema=s)
