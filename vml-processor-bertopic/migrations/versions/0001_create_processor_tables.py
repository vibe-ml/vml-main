"""Create processor topic-run tables.

Revision ID: 0001
Revises:
Create Date: 2026-09-29
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create the four topic-run tables in the public schema."""
    op.create_table(
        "pwf_topic_runs",
        sa.Column(
            "run_id",
            sa.Uuid(),
            primary_key=True,
            comment="Stable identifier of this topic run",
        ),
        sa.Column(
            "status",
            sa.Text(),
            nullable=False,
            comment="Run outcome: running, succeeded, or failed",
        ),
        sa.Column(
            "scope_ids",
            postgresql.ARRAY(sa.Text()),
            nullable=False,
            comment="Crawler collection scopes used for the corpus",
        ),
        sa.Column(
            "work_types",
            postgresql.ARRAY(sa.Text()),
            nullable=False,
            comment="OpenAlex work types kept in the corpus",
        ),
        sa.Column(
            "published_from",
            sa.Date(),
            nullable=False,
            comment="Inclusive start of the publication interval",
        ),
        sa.Column(
            "published_to",
            sa.Date(),
            nullable=False,
            comment="Inclusive end of the publication interval",
        ),
        sa.Column(
            "excluded_domain_ids",
            postgresql.ARRAY(sa.Text()),
            nullable=False,
            comment="OpenAlex primary domain IDs excluded from the corpus",
        ),
        sa.Column(
            "embedding_model",
            sa.Text(),
            nullable=False,
            comment="Embedding model name whose vectors were fitted",
        ),
        sa.Column(
            "embedding_model_revision",
            sa.Text(),
            nullable=False,
            comment="Embedding model revision whose vectors were fitted",
        ),
        sa.Column(
            "min_cluster_size",
            sa.Integer(),
            nullable=False,
            comment="HDBSCAN minimum cluster size used for the fit",
        ),
        sa.Column(
            "min_samples",
            sa.Integer(),
            nullable=True,
            comment="HDBSCAN min_samples; null means library default",
        ),
        sa.Column(
            "umap_random_state",
            sa.Integer(),
            nullable=False,
            comment="UMAP random_state pinning reproducibility",
        ),
        sa.Column(
            "umap_metric",
            sa.Text(),
            nullable=False,
            comment="UMAP distance metric used for the fit",
        ),
        sa.Column(
            "composition_hash_algorithm",
            sa.Text(),
            nullable=False,
            comment="Hash algorithm used for composition_hash values",
        ),
        sa.Column(
            "composition_hash_encoding_version",
            sa.Text(),
            nullable=False,
            comment="Version of the membership encoding fed to the hash",
        ),
        sa.Column(
            "work_count",
            sa.Integer(),
            nullable=False,
            comment="Number of works in the fitted corpus",
        ),
        sa.Column(
            "topic_count",
            sa.Integer(),
            nullable=False,
            comment="Number of non-outlier discovery topics found",
        ),
        sa.Column(
            "outlier_count",
            sa.Integer(),
            nullable=False,
            comment="Number of works left unassigned as outliers",
        ),
        sa.Column(
            "outlier_rate",
            sa.Float(),
            nullable=False,
            comment="outlier_count divided by work_count",
        ),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
            comment="When the topic run started",
        ),
        sa.Column(
            "finished_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="When the topic run finished",
        ),
        sa.Column(
            "elapsed_seconds",
            sa.Float(),
            nullable=True,
            comment="Wall-clock seconds from start to finish",
        ),
        comment="One discovery analysis of a fixed corpus",
    )
    op.create_table(
        "pwf_discovery_topics",
        sa.Column(
            "discovery_topic_id",
            sa.Uuid(),
            primary_key=True,
            comment="Stable UUIDv4 identifier of this discovery topic",
        ),
        sa.Column(
            "topic_run_id",
            sa.Uuid(),
            sa.ForeignKey("pwf_topic_runs.run_id"),
            nullable=False,
            comment="Topic run that produced this discovery topic",
        ),
        sa.Column(
            "composition_hash",
            sa.Text(),
            nullable=False,
            comment="SHA-256 fingerprint of sorted member work IDs",
        ),
        sa.Column(
            "size",
            sa.Integer(),
            nullable=False,
            comment="Number of member works in this discovery topic",
        ),
        sa.Column(
            "keywords",
            postgresql.JSONB(),
            nullable=False,
            comment="c-TF-IDF keywords with weights as [{term, weight}]",
        ),
        sa.Column(
            "representative_work_ids",
            postgresql.ARRAY(sa.Text()),
            nullable=False,
            comment="Centroid-nearest representative OpenAlex work IDs",
        ),
        comment="Discovery topics found within a topic run",
    )
    op.create_table(
        "pwf_topic_run_mappings",
        sa.Column(
            "topic_run_id",
            sa.Uuid(),
            sa.ForeignKey("pwf_topic_runs.run_id"),
            primary_key=True,
            comment="Topic run that owns this mapping",
        ),
        sa.Column(
            "bertopic_topic_id",
            sa.Integer(),
            primary_key=True,
            comment="BERTopic run-local topic integer identifier",
        ),
        sa.Column(
            "discovery_topic_id",
            sa.Uuid(),
            sa.ForeignKey("pwf_discovery_topics.discovery_topic_id"),
            nullable=False,
            comment="Persisted discovery topic UUID for this integer ID",
        ),
        comment="BERTopic integer IDs mapped to discovery topic UUIDs per run",
    )
    op.create_table(
        "pwf_work_topic_assignments",
        sa.Column(
            "topic_run_id",
            sa.Uuid(),
            sa.ForeignKey("pwf_topic_runs.run_id"),
            primary_key=True,
            comment="Topic run that produced this assignment",
        ),
        sa.Column(
            "work_id",
            sa.Text(),
            primary_key=True,
            comment="OpenAlex work entity identifier",
        ),
        sa.Column(
            "work_version_id",
            sa.Text(),
            nullable=False,
            comment="Immutable work version consumed by the run",
        ),
        sa.Column(
            "discovery_topic_id",
            sa.Uuid(),
            sa.ForeignKey("pwf_discovery_topics.discovery_topic_id"),
            nullable=True,
            comment="Assigned discovery topic; null when outlier",
        ),
        sa.Column(
            "is_outlier",
            sa.Boolean(),
            nullable=False,
            comment="True when HDBSCAN left the work unassigned",
        ),
        sa.Column(
            "bertopic_topic_id",
            sa.Integer(),
            nullable=False,
            comment="BERTopic run-local topic id; -1 for outliers",
        ),
        comment="Per-work topic assignment or outlier flag within a run",
    )


def downgrade() -> None:
    """Drop the four topic-run tables."""
    op.drop_table("pwf_work_topic_assignments")
    op.drop_table("pwf_topic_run_mappings")
    op.drop_table("pwf_discovery_topics")
    op.drop_table("pwf_topic_runs")
