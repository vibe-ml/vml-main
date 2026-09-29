"""Create the smoke probe table.

Revision ID: 0001
Revises:
Create Date: 2026-09-29
"""

import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create the smoke probe table and insert a known literal."""
    op.create_table(
        "smoke_probe",
        sa.Column(
            "probe_value", sa.Integer(), nullable=False, comment="Smoke literal."
        ),
        comment="Throwaway proof that Alembic can migrate the test database.",
    )
    smoke_probe = sa.table("smoke_probe", sa.column("probe_value", sa.Integer()))
    op.bulk_insert(smoke_probe, [{"probe_value": 1}])


def downgrade() -> None:
    """Drop the smoke probe table."""
    op.drop_table("smoke_probe")
