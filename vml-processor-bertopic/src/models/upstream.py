"""Read-only views of crawler-owned tables consumed by the processor.

The crawler `vml-crawler-openalex` owns and migrates these tables. They live in a separate
`MetaData` so processor migrations never create, alter, or drop them. Only the columns the
processor reads are declared.
"""

from sqlalchemy import Column, MetaData, Table, Text
from sqlalchemy.dialects.postgresql import JSONB

UPSTREAM_SCHEMA = "raw"

upstream_metadata = MetaData()

# fmt: off
work_versions = Table(
    "openalex_work_versions",
    upstream_metadata,
    Column("id", Text(), primary_key=True, comment="Synthetic version identity"),
    Column("entity_id", Text(), nullable=False, comment="OpenAlex work entity identifier"),
    Column("payload", JSONB(), nullable=False, comment="Parsed work record entity payload"),
    schema=UPSTREAM_SCHEMA,
    comment="Deduplicated work records and full entities",
)

work_current = Table(
    "openalex_work_current",
    upstream_metadata,
    Column("scope_id", Text(), primary_key=True, comment="Collection scope identifier"),
    Column("entity_id", Text(), primary_key=True, comment="OpenAlex work entity identifier"),
    Column("version_id", Text(), nullable=False, comment="Active work version identifier"),
    schema=UPSTREAM_SCHEMA,
    comment="Active reconciled version per work entity and collection scope",
)
# fmt: on
