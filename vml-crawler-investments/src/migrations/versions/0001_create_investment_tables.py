"""Create runs, scans, raw pages, articles, and funding events."""

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
    """Resolve the investments namespace for this migration invocation."""
    settings = context.config.attributes.get("settings") or Settings()
    return settings.schema_investments


def upgrade() -> None:
    """Create all investment tables in one namespace."""
    s = get_schema()

    # fmt: off
    op.create_table(
        "runs",
        sa.Column("id", sa.Uuid(), primary_key=True, comment="Run identifier"),  # noqa: E501
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
        sa.Column("term_id", sa.Text(), nullable=False, comment="vml-crawler-social term identifier"),  # noqa: E501
        sa.Column("term", sa.Text(), nullable=False, comment="Search phrase"),  # noqa: E501
        sa.Column("normalized", sa.Text(), nullable=False, comment="Normalized phrase for local matching"),  # noqa: E501
        sa.Column("window_start", sa.Date(), nullable=False, comment="Inclusive window start (first day of month)"),  # noqa: E501
        sa.Column("window_end", sa.Date(), nullable=False, comment="Exclusive window end (first day of month)"),  # noqa: E501
        sa.Column("parent_id", sa.BigInteger(), sa.ForeignKey(f"{s}.scans.id"), nullable=True, comment="Split parent scan identifier"),  # noqa: E501
        sa.Column("status", sa.Text(), nullable=False, comment="Scan status (pending, claimed, complete, split, truncated, failed)"),  # noqa: E501
        sa.Column("fetched", sa.Integer(), nullable=False, server_default="0", comment="Articles returned by the source"),  # noqa: E501
        sa.Column("events", sa.Integer(), nullable=False, server_default="0", comment="Articles recognized as funding events"),  # noqa: E501
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0", comment="Claim attempts"),  # noqa: E501
        sa.Column("claimed_until", sa.DateTime(timezone=True), nullable=True, comment="Lease expiry of the current claim"),  # noqa: E501
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, comment="Last status change timestamp"),  # noqa: E501
        sa.Column("error", sa.Text(), nullable=True, comment="Exception class of the last failure"),  # noqa: E501
        sa.UniqueConstraint("term_id", "window_start", "window_end", name="scans_window_key"),  # noqa: E501
        sa.CheckConstraint("status IN ('pending', 'claimed', 'complete', 'split', 'truncated', 'failed')", name="scans_status_check"),  # noqa: E501
        sa.CheckConstraint("window_start < window_end", name="scans_window_check"),  # noqa: E501
        schema=s, comment="Resumable news search windows per profile term",
    )
    op.create_index("scans_queue_idx", "scans", ["status"], schema=s)
    op.create_table(
        "raw_pages",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), primary_key=True, comment="Raw page identifier"),  # noqa: E501
        sa.Column("run_id", sa.Uuid(), sa.ForeignKey(f"{s}.runs.id"), nullable=False, comment="Originating run identifier"),  # noqa: E501
        sa.Column("scan_id", sa.BigInteger(), sa.ForeignKey(f"{s}.scans.id"), nullable=False, comment="Originating scan identifier"),  # noqa: E501
        sa.Column("locator", sa.Text(), nullable=False, comment="Request URL"),  # noqa: E501
        sa.Column("status_code", sa.Integer(), nullable=False, comment="HTTP status code"),  # noqa: E501
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False, comment="Response timestamp"),  # noqa: E501
        sa.Column("checksum", sa.Text(), nullable=False, comment="Response body SHA-256 checksum"),  # noqa: E501
        sa.Column("bytes", sa.Integer(), nullable=False, comment="Response body size"),  # noqa: E501
        sa.Column("payload", postgresql.JSONB(), nullable=True, comment="Parsed JSON body, null if not JSON"),  # noqa: E501
        schema=s, comment="Immutable source responses with provenance",
    )
    op.create_table(
        "articles",
        sa.Column("id", sa.Text(), primary_key=True, comment="SHA-256 hash of the article URL"),  # noqa: E501
        sa.Column("url", sa.Text(), nullable=False, comment="Article URL"),  # noqa: E501
        sa.Column("title", sa.Text(), nullable=False, comment="Article title"),  # noqa: E501
        sa.Column("seen_at", sa.DateTime(timezone=True), nullable=False, comment="First time the source saw the article"),  # noqa: E501
        sa.Column("domain", sa.Text(), nullable=False, comment="Publisher domain"),  # noqa: E501
        sa.Column("language", sa.Text(), nullable=True, comment="Article language"),  # noqa: E501
        sa.Column("source_country", sa.Text(), nullable=True, comment="Publisher country"),  # noqa: E501
        sa.Column("trust_level", sa.Text(), nullable=False, server_default="low", comment="Source trust level (GPB hierarchy)"),  # noqa: E501
        sa.Column("raw_page_id", sa.BigInteger(), sa.ForeignKey(f"{s}.raw_pages.id"), nullable=False, comment="First raw page containing the article"),  # noqa: E501
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False, comment="First retrieval timestamp"),  # noqa: E501
        schema=s, comment="Deduplicated news articles returned for profile terms",
    )
    op.create_table(
        "article_terms",
        sa.Column("article_id", sa.Text(), sa.ForeignKey(f"{s}.articles.id"), primary_key=True, comment="Article identifier"),  # noqa: E501
        sa.Column("term_id", sa.Text(), primary_key=True, comment="vml-crawler-social term identifier"),  # noqa: E501
        sa.Column("scan_id", sa.BigInteger(), sa.ForeignKey(f"{s}.scans.id"), nullable=False, comment="First scan that returned the article"),  # noqa: E501
        sa.Column("in_title", sa.Boolean(), nullable=False, comment="Whether the title contains the term (strong link)"),  # noqa: E501
        schema=s, comment="Profile terms that returned each article",
    )
    op.create_index("article_terms_term_idx", "article_terms", ["term_id"], schema=s)
    op.create_table(
        "events",
        sa.Column("id", sa.Text(), primary_key=True, comment="Hash of company, type, stage, amount, and month (origin group)"),  # noqa: E501
        sa.Column("event_type", sa.Text(), nullable=False, comment="Event type (vc_round, m_and_a, ipo, grant)"),  # noqa: E501
        sa.Column("company", sa.Text(), nullable=True, comment="Company named in the headline"),  # noqa: E501
        sa.Column("stage", sa.Text(), nullable=True, comment="Round stage (pre_seed, seed, series_a, ...)"),  # noqa: E501
        sa.Column("amount", sa.Double(), nullable=True, comment="Announced amount in currency units"),  # noqa: E501
        sa.Column("currency", sa.Text(), nullable=True, comment="ISO currency code"),  # noqa: E501
        sa.Column("investors", sa.Text(), nullable=True, comment="Lead investors named in the headline"),  # noqa: E501
        sa.Column("month", sa.Date(), nullable=False, comment="First day of the announcement month"),  # noqa: E501
        sa.Column("extractor", sa.Text(), nullable=False, comment="Extraction rule version"),  # noqa: E501
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False, comment="First extraction timestamp"),  # noqa: E501
        sa.CheckConstraint("event_type IN ('vc_round', 'm_and_a', 'ipo', 'grant')", name="events_type_check"),  # noqa: E501
        schema=s, comment="Funding events extracted from headlines",
    )
    op.create_index("events_month_idx", "events", ["month"], schema=s)
    op.create_table(
        "event_articles",
        sa.Column("event_id", sa.Text(), sa.ForeignKey(f"{s}.events.id"), primary_key=True, comment="Event identifier"),  # noqa: E501
        sa.Column("article_id", sa.Text(), sa.ForeignKey(f"{s}.articles.id"), primary_key=True, comment="Article identifier"),  # noqa: E501
        schema=s, comment="Articles reporting each funding event",
    )
    # fmt: on


def downgrade() -> None:
    """Drop all investment tables in dependency order."""
    s = get_schema()
    for table in (
        "event_articles",
        "events",
        "article_terms",
        "articles",
        "raw_pages",
        "scans",
        "runs",
    ):
        op.drop_table(table, schema=s)
