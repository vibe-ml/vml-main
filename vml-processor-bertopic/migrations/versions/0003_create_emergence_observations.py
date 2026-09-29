"""Create processor emergence observation table.

Revision ID: 0003
Revises: 0002
Create Date: 2026-09-29
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create the emergence observation table in the public schema."""
    op.create_table(
        "pwf_emergence_observations",
        sa.Column(
            "topic_run_id",
            sa.Uuid(),
            sa.ForeignKey("pwf_topic_runs.run_id"),
            primary_key=True,
            comment="Topic run that this observation scores",
        ),
        sa.Column(
            "discovery_topic_id",
            sa.Uuid(),
            sa.ForeignKey("pwf_discovery_topics.discovery_topic_id"),
            primary_key=True,
            comment="Discovery topic whose share series is stored",
        ),
        sa.Column(
            "monthly_series",
            postgresql.JSONB(),
            nullable=False,
            comment="Per-month topic_count, corpus_count, and share for the run interval",
        ),
        sa.Column(
            "monthly_share_change",
            sa.Float(),
            nullable=True,
            comment="Geometric month-to-month share change; null when unset",
        ),
        sa.Column(
            "complete_years",
            postgresql.JSONB(),
            nullable=False,
            comment="Complete calendar years used for annual figures",
        ),
        sa.Column(
            "mean_share",
            sa.Float(),
            nullable=True,
            comment="Mean share over complete years; null under insufficient coverage",
        ),
        sa.Column(
            "annual_growth",
            sa.Float(),
            nullable=True,
            comment="Annual share growth over complete years; null when unset",
        ),
        sa.Column(
            "valid_year_pair_count",
            sa.Integer(),
            nullable=True,
            comment="Count of valid adjacent year pairs used for annual growth",
        ),
        sa.Column(
            "coverage_status",
            sa.Text(),
            nullable=False,
            comment="Coverage class: insufficient_historical_coverage, newly_observed, or scored",
        ),
        sa.Column(
            "emergence_quadrant",
            sa.Text(),
            nullable=True,
            comment="WISDOM quadrant when scored; null when coverage withholds it",
        ),
        sa.Column(
            "metric_version",
            sa.Text(),
            nullable=False,
            comment="Formula version that produced this observation",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
            comment="When this observation row was inserted",
        ),
        comment="Publication-share series and emergence class per discovery topic",
    )


def downgrade() -> None:
    """Drop the emergence observation table."""
    op.drop_table("pwf_emergence_observations")
