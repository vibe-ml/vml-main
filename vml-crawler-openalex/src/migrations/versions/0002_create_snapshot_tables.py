"""Create bounded snapshot evidence, versions, and atomic publications."""

# Retain the repository-required E501 exemptions on single-line declarations.
# ruff: noqa: RUF100

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.dialects.postgresql import JSONB

from src.common.settings import Settings

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def get_schemas() -> tuple[str, str]:
    """Resolve the raw and tmd namespaces for this migration invocation."""
    settings = context.config.attributes.get("settings") or Settings()
    return settings.schema_raw, settings.schema_tmd


def upgrade() -> None:
    """Create work provenance and publication constraints."""
    raw, tmd = get_schemas()

    # fmt: off
    op.create_table(
        "openalex_work_runs",
        sa.Column("id", sa.Uuid(), primary_key=True, comment="Work run identifier"),  # noqa: E501
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False, comment="Observation timestamp"),  # noqa: E501
        sa.Column("lower_date", sa.Date(), nullable=False, comment="Lower publication date boundary"),  # noqa: E501
        sa.Column("upper_date", sa.Date(), nullable=False, comment="Upper publication date boundary"),  # noqa: E501
        sa.Column("status", sa.Text(), nullable=False, comment="Run status (running, failed, complete)"),  # noqa: E501
        schema=tmd,
        comment="Work collection execution lifecycle (snapshot, bootstrap, API)",
    )
    op.create_table(
        "openalex_work_sources",
        sa.Column("id", sa.Text(), primary_key=True, comment="Source receipt identifier"),  # noqa: E501
        sa.Column("release", sa.Text(), nullable=False, comment="Snapshot release identifier or ingestion tag"),  # noqa: E501
        sa.Column("source", sa.Text(), nullable=False, comment="Origin source type or URL"),  # noqa: E501
        sa.Column("path", sa.Text(), nullable=False, comment="Local or object storage relative path"),  # noqa: E501
        sa.Column("sha256", sa.Text(), nullable=False, comment="SHA-256 checksum of source file"),  # noqa: E501
        sa.Column("bytes", sa.BigInteger(), nullable=False, comment="Source file byte size"),  # noqa: E501
        sa.Column("rows", sa.BigInteger(), nullable=False, comment="Total records parsed from source"),  # noqa: E501
        schema=tmd,
        comment="Ingested snapshot Parquet files and raw API response file receipts",
    )
    op.create_table(
        "openalex_work_versions",
        sa.Column("id", sa.Text(), primary_key=True, comment="Synthetic version identity"),  # noqa: E501
        sa.Column("entity_id", sa.Text(), nullable=False, comment="OpenAlex work entity identifier"),  # noqa: E501
        sa.Column("canonicalization_version", sa.Integer(), nullable=False, comment="Version of entity canonicalization"),  # noqa: E501
        sa.Column("content_hash", sa.Text(), nullable=False, comment="SHA-256 hash of canonicalized content"),  # noqa: E501
        sa.Column("payload", JSONB(), nullable=False, comment="Parsed work record entity payload"),  # noqa: E501
        sa.UniqueConstraint("entity_id", "canonicalization_version", "content_hash", name="openalex_work_versions_entity_version_hash_key"),  # noqa: E501
        schema=raw,
        comment="Deduplicated work records and full entities",
    )
    op.create_table(
        "openalex_work_observations",
        sa.Column("run_id", sa.Uuid(), sa.ForeignKey(f"{tmd}.openalex_work_runs.id"), primary_key=True, comment="Execution run identifier"),  # noqa: E501
        sa.Column("row_number", sa.Integer(), primary_key=True, comment="Position within source file or stream"),  # noqa: E501
        sa.Column("source_id", sa.Text(), sa.ForeignKey(f"{tmd}.openalex_work_sources.id"), nullable=False, comment="Originating source receipt identifier"),  # noqa: E501
        sa.Column("version_id", sa.Text(), sa.ForeignKey(f"{raw}.openalex_work_versions.id"), nullable=True, comment="Resolved work version identifier"),  # noqa: E501
        sa.Column("disposition", sa.Text(), nullable=False, comment="Observation disposition (selected, quarantine, etc.)"),  # noqa: E501
        schema=raw,
        comment="Sighting history and disposition per work",
    )
    op.create_table(
        "openalex_work_batches",
        sa.Column("id", sa.Text(), primary_key=True, comment="Derived batch identifier"),  # noqa: E501
        sa.Column("taxonomy_id", sa.Text(), sa.ForeignKey(f"{raw}.openalex_taxonomy_bundles.id"), nullable=False, comment="Associated taxonomy bundle identifier"),  # noqa: E501
        sa.Column("manifest", JSONB(), nullable=False, comment="Batch manifest metadata"),  # noqa: E501
        schema=tmd,
        comment="Derived Parquet transform batch manifests linked to taxonomy bundles",
    )
    op.create_table(
        "openalex_work_processing",
        sa.Column("version_id", sa.Text(), sa.ForeignKey(f"{raw}.openalex_work_versions.id"), primary_key=True, comment="Work version identifier"),  # noqa: E501
        sa.Column("transformation", sa.Text(), primary_key=True, comment="Transformation name"),  # noqa: E501
        sa.Column("dependency", sa.Text(), primary_key=True, comment="Transformation dependency hash"),  # noqa: E501
        sa.Column("batch_id", sa.Text(), sa.ForeignKey(f"{tmd}.openalex_work_batches.id"), nullable=False, comment="Derived batch identifier"),  # noqa: E501
        schema=raw,
        comment="Transformation lineage connecting entity versions to derived batches",
    )
    op.create_table(
        "openalex_work_current",
        sa.Column("entity_id", sa.Text(), primary_key=True, comment="OpenAlex work entity identifier"),  # noqa: E501
        sa.Column("version_id", sa.Text(), sa.ForeignKey(f"{raw}.openalex_work_versions.id"), nullable=False, comment="Active work version identifier"),  # noqa: E501
        schema=raw,
        comment="Active reconciled version per work entity",
    )
    op.create_index("openalex_work_observations_version", "openalex_work_observations", ["version_id"], schema=raw)  # noqa: E501
    # fmt: on


def downgrade() -> None:
    """Remove snapshot tables in foreign-key order."""
    raw, tmd = get_schemas()
    op.drop_table("openalex_work_current", schema=raw)
    op.drop_table("openalex_work_processing", schema=raw)
    op.drop_table("openalex_work_batches", schema=tmd)
    op.drop_index(
        "openalex_work_observations_version",
        table_name="openalex_work_observations",
        schema=raw,
    )
    op.drop_table("openalex_work_observations", schema=raw)
    op.drop_table("openalex_work_versions", schema=raw)
    op.drop_table("openalex_work_sources", schema=tmd)
    op.drop_table("openalex_work_runs", schema=tmd)
