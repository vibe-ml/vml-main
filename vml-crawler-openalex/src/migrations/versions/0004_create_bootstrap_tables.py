"""Persist release acquisition separately from scoped chunk publication."""

# ruff: noqa: RUF100

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.dialects.postgresql import JSONB

from src.common.settings import Settings

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def get_schemas() -> tuple[str, str]:
    """Resolve the raw and tmd namespaces for this migration invocation."""
    settings = context.config.attributes.get("settings") or Settings()
    return settings.schema_raw, settings.schema_tmd


def upgrade() -> None:
    """Create immutable manifest identities and resumable coverage."""
    _raw, tmd = get_schemas()

    # fmt: off
    op.create_table(
        "openalex_work_releases",
        sa.Column("id", sa.Text(), primary_key=True, comment="Snapshot release identifier"),  # noqa: E501
        sa.Column("release", sa.Text(), nullable=False, comment="Canonical release name"),  # noqa: E501
        sa.Column("manifest_path", sa.Text(), nullable=False, comment="Storage path for release manifest"),  # noqa: E501
        sa.Column("manifest_source", sa.Text(), nullable=False, comment="Origin source URL of release manifest"),  # noqa: E501
        sa.Column("retrieved_at", sa.DateTime(timezone=True), nullable=False, comment="Manifest retrieval timestamp"),  # noqa: E501
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=True, comment="Release verification timestamp"),  # noqa: E501
        sa.Column("acquisition_status", sa.Text(), nullable=False, comment="Release acquisition status"),  # noqa: E501
        schema=tmd,
        comment="Quarterly snapshot release manifest metadata and acquisition status",
    )
    op.create_table(
        "openalex_work_release_files",
        sa.Column("id", sa.Text(), primary_key=True, comment="Release file identifier"),  # noqa: E501
        sa.Column("release_id", sa.Text(), sa.ForeignKey(f"{tmd}.openalex_work_releases.id"), nullable=False, comment="Associated release identifier"),  # noqa: E501
        sa.Column("inventory", JSONB(), nullable=False, comment="Manifest file entry metadata"),  # noqa: E501
        sa.Column("source_id", sa.Text(), sa.ForeignKey(f"{tmd}.openalex_work_sources.id"), nullable=True, comment="Ingested source receipt identifier"),  # noqa: E501
        sa.Column("retrieved_at", sa.DateTime(timezone=True), nullable=True, comment="Download completion timestamp"),  # noqa: E501
        schema=tmd,
        comment="Snapshot file inventory manifests and download status",
    )
    op.create_table(
        "openalex_work_baselines",
        sa.Column("id", sa.Text(), primary_key=True, comment="Baseline configuration identifier"),  # noqa: E501
        sa.Column("release_id", sa.Text(), sa.ForeignKey(f"{tmd}.openalex_work_releases.id"), nullable=False, comment="Associated release identifier"),  # noqa: E501
        sa.Column("scope_id", sa.Text(), sa.ForeignKey(f"{tmd}.openalex_work_scopes.id"), nullable=False, comment="Target collection scope identifier"),  # noqa: E501
        sa.Column("chunk_rows", sa.Integer(), nullable=False, comment="Number of rows per processing chunk"),  # noqa: E501
        sa.Column("selection_status", sa.Text(), nullable=False, comment="Baseline processing status"),  # noqa: E501
        sa.UniqueConstraint("release_id", "scope_id", name="openalex_work_baselines_release_id_scope_id_key"),  # noqa: E501
        schema=tmd,
        comment="Scope-specific baseline selection configurations and status",
    )
    op.create_table(
        "openalex_work_chunks",
        sa.Column("id", sa.Text(), primary_key=True, comment="Chunk processing identifier"),  # noqa: E501
        sa.Column("baseline_id", sa.Text(), sa.ForeignKey(f"{tmd}.openalex_work_baselines.id"), nullable=False, comment="Associated baseline identifier"),  # noqa: E501
        sa.Column("file_id", sa.Text(), sa.ForeignKey(f"{tmd}.openalex_work_release_files.id"), nullable=False, comment="Target release file identifier"),  # noqa: E501
        sa.Column("start_row", sa.BigInteger(), nullable=False, comment="Starting row offset in release file"),  # noqa: E501
        sa.Column("row_count", sa.Integer(), nullable=False, comment="Count of rows in this chunk"),  # noqa: E501
        sa.Column("batch_id", sa.Text(), sa.ForeignKey(f"{tmd}.openalex_work_batches.id"), nullable=False, comment="Generated derived batch identifier"),  # noqa: E501
        sa.UniqueConstraint("baseline_id", "file_id", "start_row", name="openalex_work_chunks_baseline_id_file_id_start_row_key"),  # noqa: E501
        schema=tmd,
        comment="Row-chunk ranges for parallel bootstrap baseline processing",
    )
    # fmt: on


def downgrade() -> None:
    """Remove only bootstrap metadata."""
    _raw, tmd = get_schemas()
    for name in ("chunks", "baselines", "release_files", "releases"):
        op.drop_table(f"openalex_work_{name}", schema=tmd)
