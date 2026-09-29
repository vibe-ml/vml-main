"""Read-only views of vml-crawler-social profile tables; never migrated here."""

import sqlalchemy as sa

# Schema name is a placeholder translated to Settings.social_schema.
metadata = sa.MetaData(schema="social_src")

profiles = sa.Table(
    "profiles",
    metadata,
    sa.Column("id", sa.Text(), primary_key=True),
    sa.Column("created_at", sa.DateTime(timezone=True)),
)

terms = sa.Table(
    "terms",
    metadata,
    sa.Column("id", sa.Text(), primary_key=True),
    sa.Column("term", sa.Text()),
    sa.Column("normalized", sa.Text()),
)

profile_terms = sa.Table(
    "profile_terms",
    metadata,
    sa.Column("profile_id", sa.Text(), primary_key=True),
    sa.Column("topic_id", sa.Text(), primary_key=True),
    sa.Column("term_id", sa.Text(), primary_key=True),
    sa.Column("origin", sa.Text()),
)
