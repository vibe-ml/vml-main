"""Read-only views of vml-crawler-openalex tables; never migrated here."""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# Schema name is a placeholder translated to Settings.openalex_schema_raw.
metadata = sa.MetaData(schema="openalex_raw")

taxonomy_bundles = sa.Table(
    "openalex_taxonomy_bundles",
    metadata,
    sa.Column("id", sa.Text(), primary_key=True),
    sa.Column("records", postgresql.JSONB()),
)

taxonomy_observations = sa.Table(
    "openalex_taxonomy_observations",
    metadata,
    sa.Column("sequence", sa.BigInteger(), primary_key=True),
    sa.Column("bundle_id", sa.Text()),
    sa.Column("ended_at", sa.DateTime(timezone=True)),
)

work_versions = sa.Table(
    "openalex_work_versions",
    metadata,
    sa.Column("id", sa.Text(), primary_key=True),
    sa.Column("payload", postgresql.JSONB()),
)

work_current = sa.Table(
    "openalex_work_current",
    metadata,
    sa.Column("scope_id", sa.Text(), primary_key=True),
    sa.Column("entity_id", sa.Text(), primary_key=True),
    sa.Column("version_id", sa.Text()),
)
