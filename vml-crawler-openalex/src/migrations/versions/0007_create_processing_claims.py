"""Create durable processing claims for derived batch generation."""

# ruff: noqa: RUF100

import sqlalchemy as sa
from alembic import context, op

from src.common.settings import Settings

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def get_schemas() -> tuple[str, str]:
    """Resolve the raw and tmd namespaces for this migration invocation."""
    settings = context.config.attributes.get("settings") or Settings()
    return settings.schema_raw, settings.schema_tmd


def upgrade() -> None:
    """Create processing claims for batch generation."""
    raw, tmd = get_schemas()

    # fmt: off
    op.create_table(
        "openalex_work_batch_claims",
        sa.Column("id", sa.Text(), primary_key=True, comment="Batch claim identifier"),  # noqa: E501
        sa.Column("source_id", sa.Text(), sa.ForeignKey(f"{tmd}.openalex_work_sources.id"), nullable=False, comment="Source receipt identifier"),  # noqa: E501
        sa.Column("scope_id", sa.Text(), sa.ForeignKey(f"{tmd}.openalex_work_scopes.id"), nullable=False, comment="Collection scope identifier"),  # noqa: E501
        sa.Column("transformation", sa.Text(), nullable=False, comment="Transformation name"),  # noqa: E501
        sa.Column("dependency", sa.Text(), nullable=False, comment="Transformation dependency hash"),  # noqa: E501
        sa.Column("taxonomy_id", sa.Text(), sa.ForeignKey(f"{raw}.openalex_taxonomy_bundles.id"), nullable=False, comment="Associated taxonomy bundle identifier"),  # noqa: E501
        sa.Column("status", sa.Text(), nullable=False, comment="Claim status (pending, claimed, complete, failed)"),  # noqa: E501
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True, comment="Worker claim timestamp"),  # noqa: E501
        sa.Column("batch_id", sa.Text(), sa.ForeignKey(f"{tmd}.openalex_work_batches.id"), nullable=True, comment="Derived batch identifier if completed"),  # noqa: E501
        sa.UniqueConstraint("source_id", "scope_id", "transformation", "taxonomy_id", name="openalex_work_batch_claims_unique"),  # noqa: E501
        schema=tmd,
        comment="Distributed worker claims for derived batch transformation tasks",
    )
    # fmt: on


def downgrade() -> None:
    """Remove processing claims."""
    _raw, tmd = get_schemas()
    op.drop_table("openalex_work_batch_claims", schema=tmd)
