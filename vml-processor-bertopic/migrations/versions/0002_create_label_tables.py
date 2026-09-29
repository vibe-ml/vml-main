"""Create processor work-summary and topic-label tables.

Revision ID: 0002
Revises: 0001
Create Date: 2026-09-29
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create the work-summary and topic-label tables in the public schema."""
    op.create_table(
        "pwf_work_summaries",
        sa.Column(
            "topic_run_id",
            sa.Uuid(),
            sa.ForeignKey("pwf_topic_runs.run_id"),
            primary_key=True,
            comment="Topic run that owns this summary",
        ),
        sa.Column(
            "discovery_topic_id",
            sa.Uuid(),
            sa.ForeignKey("pwf_discovery_topics.discovery_topic_id"),
            primary_key=True,
            comment="Discovery topic this work was sampled for",
        ),
        sa.Column(
            "work_id",
            sa.Text(),
            primary_key=True,
            comment="OpenAlex work entity identifier that was summarized",
        ),
        sa.Column(
            "summary_text",
            sa.Text(),
            nullable=False,
            comment="Generated English summary of the work title and abstract",
        ),
        sa.Column(
            "model",
            sa.Text(),
            nullable=False,
            comment="Labeling model name that produced this summary",
        ),
        sa.Column(
            "model_revision",
            sa.Text(),
            nullable=False,
            comment="Labeling model revision that produced this summary",
        ),
        sa.Column(
            "prompt_version",
            sa.Text(),
            nullable=False,
            comment="Version of the summarization prompt constants used",
        ),
        sa.Column(
            "prompt_tokens",
            sa.Integer(),
            nullable=False,
            comment="Prompt token count reported for this summary call",
        ),
        sa.Column(
            "completion_tokens",
            sa.Integer(),
            nullable=False,
            comment="Completion token count reported for this summary call",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
            comment="When this summary row was inserted",
        ),
        comment="Per-work summaries that feed discovery topic headlines",
    )
    op.create_table(
        "pwf_topic_labels",
        sa.Column(
            "topic_run_id",
            sa.Uuid(),
            sa.ForeignKey("pwf_topic_runs.run_id"),
            primary_key=True,
            comment="Topic run that owns this label",
        ),
        sa.Column(
            "discovery_topic_id",
            sa.Uuid(),
            sa.ForeignKey("pwf_discovery_topics.discovery_topic_id"),
            primary_key=True,
            comment="Discovery topic this headline describes",
        ),
        sa.Column(
            "headline",
            sa.Text(),
            nullable=True,
            comment="Generated English headline; null when labeling failed",
        ),
        sa.Column(
            "concatenated_summary_text",
            sa.Text(),
            nullable=True,
            comment="Concatenated per-work summaries that fed the headline",
        ),
        sa.Column(
            "chunk_count",
            sa.Integer(),
            nullable=False,
            comment="Number of summary chunks used; 1 for single-pass labeling",
        ),
        sa.Column(
            "sampling_method",
            sa.Text(),
            nullable=False,
            comment="How works were selected: representative_docs or all_members",
        ),
        sa.Column(
            "sampled_work_ids",
            postgresql.ARRAY(sa.Text()),
            nullable=False,
            comment="OpenAlex work IDs whose summaries fed this headline",
        ),
        sa.Column(
            "sample_size",
            sa.Integer(),
            nullable=False,
            comment="Number of works sampled for this label",
        ),
        sa.Column(
            "model",
            sa.Text(),
            nullable=False,
            comment="Labeling model name that produced this headline",
        ),
        sa.Column(
            "model_revision",
            sa.Text(),
            nullable=False,
            comment="Labeling model revision that produced this headline",
        ),
        sa.Column(
            "prompt_version",
            sa.Text(),
            nullable=False,
            comment="Version of the labeling prompt constants used",
        ),
        sa.Column(
            "status",
            sa.Text(),
            nullable=False,
            comment="Label outcome: succeeded or failed",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
            comment="When this label row was inserted",
        ),
        comment="Generated headlines and sampling metadata for discovery topics",
    )


def downgrade() -> None:
    """Drop the work-summary and topic-label tables."""
    op.drop_table("pwf_topic_labels")
    op.drop_table("pwf_work_summaries")
