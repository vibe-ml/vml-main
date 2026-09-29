"""Create taxonomy bundles, provenance, and observations."""

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


def get_schemas() -> tuple[str, str]:
    """Resolve the raw and tmd namespaces for this migration invocation."""
    settings = context.config.attributes.get("settings") or Settings()
    return settings.schema_raw, settings.schema_tmd


def adopt_legacy(raw_schema: str, tmd_schema: str) -> bool:
    """Preserve a collection already initialized by the original SQL runner."""
    connection = op.get_bind()
    inspector = sa.inspect(connection)
    found_schema = None
    for s in (raw_schema, tmd_schema):
        if inspector.has_table("schema_migrations", schema=s):
            found_schema = s
            break
    if not found_schema:
        return False
    legacy = sa.Table(
        "schema_migrations",
        sa.MetaData(),
        schema=found_schema,
        autoload_with=connection,
    )
    versions = set(connection.scalars(sa.select(legacy.c.version)))
    if versions != {"001_taxonomy.sql"}:
        raise RuntimeError("Unrecognized legacy collection migration state")
    op.drop_table("schema_migrations", schema=found_schema)
    return True


def upgrade() -> None:
    """Create the taxonomy tables across raw and tmd schemas."""
    raw, tmd = get_schemas()
    if adopt_legacy(raw, tmd):
        return

    # fmt: off
    op.create_table(
        "openalex_taxonomy_runs",
        sa.Column("id", sa.Uuid(), primary_key=True, comment="Taxonomy run identifier"),  # noqa: E501
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, comment="Run start timestamp"),  # noqa: E501
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True, comment="Run completion or termination timestamp"),  # noqa: E501
        sa.Column("status", sa.Text(), nullable=False, comment="Execution status (running, failed, complete)"),  # noqa: E501
        sa.Column("error", sa.Text(), nullable=True, comment="Error message if run failed"),  # noqa: E501
        sa.CheckConstraint(
            "status IN ('running', 'failed', 'complete')", name="openalex_taxonomy_runs_status_check"
        ),
        schema=tmd,
        comment="Taxonomy run lifecycle execution status and errors",
    )
    op.create_table(
        "openalex_taxonomy_bundles",
        sa.Column("id", sa.Text(), primary_key=True, comment="Bundle identifier hash"),  # noqa: E501
        sa.Column("classification_hash", sa.Text(), nullable=False, comment="Hash of classification tree"),  # noqa: E501
        sa.Column("canonicalization_version", sa.Integer(), nullable=False, comment="Version of canonicalization logic"),  # noqa: E501
        sa.Column("records", postgresql.JSONB(), nullable=False, comment="Full classification hierarchy payload"),  # noqa: E501
        schema=raw,
        comment="Canonical taxonomy tree and classification records",
    )
    op.create_table(
        "openalex_taxonomy_observations",
        sa.Column("sequence", sa.BigInteger(), sa.Identity(always=True), primary_key=True, comment="Monotonic observation sequence"),  # noqa: E501
        sa.Column("attempt_id", sa.Uuid(), sa.ForeignKey(f"{tmd}.openalex_taxonomy_runs.id"), nullable=False, unique=True, comment="Taxonomy run identifier"),  # noqa: E501
        sa.Column("bundle_id", sa.Text(), sa.ForeignKey(f"{raw}.openalex_taxonomy_bundles.id"), nullable=False, comment="Observed taxonomy bundle identifier"),  # noqa: E501
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, comment="Observation start time"),  # noqa: E501
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=False, comment="Observation completion time"),  # noqa: E501
        schema=raw,
        comment="Sighting history per taxonomy bundle",
    )
    op.create_index("openalex_taxonomy_observations_bundle", "openalex_taxonomy_observations", ["bundle_id"], schema=raw)  # noqa: E501
    op.create_table(
        "openalex_taxonomy_raw_files",
        sa.Column("sequence", sa.BigInteger(), sa.Identity(always=True), primary_key=True, comment="Monotonic sequence number"),  # noqa: E501
        sa.Column("attempt_id", sa.Uuid(), sa.ForeignKey(f"{tmd}.openalex_taxonomy_runs.id"), nullable=False, comment="Originating taxonomy run identifier"),  # noqa: E501
        sa.Column("path", sa.Text(), nullable=False, unique=True, comment="Storage path of raw payload"),  # noqa: E501
        sa.Column("source_locator", sa.Text(), nullable=False, comment="Source URL or origin locator"),  # noqa: E501
        sa.Column("checksum", sa.Text(), nullable=False, comment="Uncompressed content SHA-256 checksum"),  # noqa: E501
        sa.Column("compressed_checksum", sa.Text(), nullable=False, comment="Compressed file SHA-256 checksum"),  # noqa: E501
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, comment="Download start timestamp"),  # noqa: E501
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=False, comment="Download completion timestamp"),  # noqa: E501
        sa.Column("bytes", sa.BigInteger(), nullable=False, comment="Byte size of raw payload"),  # noqa: E501
        schema=tmd,
        comment="Storage metadata and checksums for raw taxonomy API responses",
    )
    op.create_index("openalex_taxonomy_raw_files_attempt", "openalex_taxonomy_raw_files", ["attempt_id"], schema=tmd)  # noqa: E501
    # fmt: on


def downgrade() -> None:
    """Remove taxonomy tables in dependency order."""
    raw, tmd = get_schemas()
    op.drop_index(
        "openalex_taxonomy_raw_files_attempt",
        table_name="openalex_taxonomy_raw_files",
        schema=tmd,
    )
    op.drop_table("openalex_taxonomy_raw_files", schema=tmd)
    op.drop_index(
        "openalex_taxonomy_observations_bundle",
        table_name="openalex_taxonomy_observations",
        schema=raw,
    )
    op.drop_table("openalex_taxonomy_observations", schema=raw)
    op.drop_table("openalex_taxonomy_bundles", schema=raw)
    op.drop_table("openalex_taxonomy_runs", schema=tmd)
