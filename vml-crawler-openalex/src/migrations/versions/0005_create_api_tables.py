"""Create API partition tracking for daily refresh."""

# ruff: noqa: RUF100

import sqlalchemy as sa
from alembic import context, op

from src.common.settings import Settings

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def get_schemas() -> tuple[str, str]:
    """Resolve the raw and tmd namespaces for this migration invocation."""
    settings = context.config.attributes.get("settings") or Settings()
    return settings.schema_raw, settings.schema_tmd


def upgrade() -> None:
    """Create partition state for API retrieval."""
    _raw, tmd = get_schemas()

    # fmt: off
    op.create_table(
        "openalex_work_partitions",
        sa.Column("id", sa.Text(), primary_key=True, comment="Partition identifier hash"),  # noqa: E501
        sa.Column("scan_id", sa.Uuid(), sa.ForeignKey(f"{tmd}.openalex_work_scans.id"), nullable=False, comment="Associated scan session identifier"),  # noqa: E501
        sa.Column("publication_date", sa.Date(), nullable=False, comment="Target publication day for partition"),  # noqa: E501
        sa.Column("next_cursor", sa.Text(), nullable=True, comment="API pagination next cursor token"),  # noqa: E501
        sa.Column("status", sa.Text(), nullable=False, comment="Partition processing status (pending, running, complete)"),  # noqa: E501
        sa.Column("fencing_revision", sa.BigInteger(), nullable=False, server_default="0", comment="Fencing monotonic revision token"),  # noqa: E501
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True, comment="Worker lease expiration timestamp"),  # noqa: E501
        sa.Column("record_count", sa.BigInteger(), nullable=False, server_default="0", comment="Cumulative records acquired in partition"),  # noqa: E501
        sa.UniqueConstraint("scan_id", "publication_date", name="openalex_work_partitions_scan_id_publication_date_key"),  # noqa: E501
        schema=tmd,
        comment="Daily API refresh partition state machine, leases, and cursor tracking",
    )
    # fmt: on


def downgrade() -> None:
    """Remove partition tracking."""
    _raw, tmd = get_schemas()
    op.drop_table("openalex_work_partitions", schema=tmd)
