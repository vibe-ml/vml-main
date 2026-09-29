"""Persist API request contracts and bounded request reservations."""

# ruff: noqa: RUF100

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.dialects.postgresql import JSONB

from src.common.settings import Settings

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def get_schemas() -> tuple[str, str]:
    """Resolve the raw and tmd namespaces for this migration invocation."""
    settings = context.config.attributes.get("settings") or Settings()
    return settings.schema_raw, settings.schema_tmd


def upgrade() -> None:
    """Add durable request identity, reset eligibility, and allowance state."""
    _raw, tmd = get_schemas()

    # fmt: off
    for column in (
        sa.Column("request_parameters", JSONB(), nullable=True, comment="Serialized API query parameters"),  # noqa: E501
        sa.Column("request_fingerprint", sa.Text(), nullable=True, comment="Unique request parameter hash"),  # noqa: E501
        sa.Column("generation", sa.Integer(), nullable=False, server_default="1", comment="Request partition generation cycle"),  # noqa: E501
        sa.Column("eligible_at", sa.DateTime(timezone=True), nullable=True, comment="Earliest eligible retry timestamp"),  # noqa: E501
        sa.Column("seen_cursors", JSONB(), nullable=False, server_default="[]", comment="Array of seen pagination cursors"),  # noqa: E501
        sa.Column("received_count", sa.BigInteger(), nullable=False, server_default="0", comment="Total records received across calls"),  # noqa: E501
        sa.Column("expected_count", sa.BigInteger(), nullable=True, comment="Advertised total records count"),  # noqa: E501
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True, comment="Partition completion timestamp"),  # noqa: E501
    ):
        op.add_column("openalex_work_partitions", column, schema=tmd)

    op.create_table(
        "openalex_work_api_pages",
        sa.Column("id", sa.Uuid(), primary_key=True, comment="API page ingestion identifier"),  # noqa: E501
        sa.Column("partition_id", sa.Text(), sa.ForeignKey(f"{tmd}.openalex_work_partitions.id"), nullable=False, comment="Associated partition identifier"),  # noqa: E501
        sa.Column("run_id", sa.Uuid(), sa.ForeignKey(f"{tmd}.openalex_work_runs.id"), nullable=False, comment="Associated run identifier"),  # noqa: E501
        sa.Column("source_id", sa.Text(), sa.ForeignKey(f"{tmd}.openalex_work_sources.id"), nullable=False, comment="Ingested raw payload source identifier"),  # noqa: E501
        sa.Column("generation", sa.Integer(), nullable=False, comment="Partition generation cycle"),  # noqa: E501
        sa.Column("cursor", sa.Text(), nullable=False, comment="Pagination cursor used for request"),  # noqa: E501
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, comment="Request dispatch timestamp"),  # noqa: E501
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=False, comment="Response arrival timestamp"),  # noqa: E501
        sa.Column("status_code", sa.Integer(), nullable=False, comment="HTTP status code received"),  # noqa: E501
        sa.Column("raw_path", sa.Text(), nullable=False, comment="Relative path to stored raw response"),  # noqa: E501
        schema=tmd,
        comment="Ingested API response page logs, cursors, timestamps, and staging paths",
    )
    op.create_table(
        "openalex_work_api_allowances",
        sa.Column("day", sa.Date(), primary_key=True, comment="Rate limit quota evaluation date"),  # noqa: E501
        sa.Column("reserved", sa.Integer(), nullable=False, server_default="0", comment="Reserved or consumed request count"),  # noqa: E501
        schema=tmd,
        comment="Daily API request quotas and reserved request counters",
    )
    # fmt: on


def downgrade() -> None:
    """Remove API reservation and request metadata."""
    _raw, tmd = get_schemas()
    op.drop_table("openalex_work_api_allowances", schema=tmd)
    op.drop_table("openalex_work_api_pages", schema=tmd)
    for column in (
        "completed_at",
        "expected_count",
        "received_count",
        "seen_cursors",
        "eligible_at",
        "generation",
        "request_fingerprint",
        "request_parameters",
    ):
        op.drop_column("openalex_work_partitions", column, schema=tmd)
