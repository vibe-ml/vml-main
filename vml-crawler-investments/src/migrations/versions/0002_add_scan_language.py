"""Add source language to scans for the Russian news segment."""

# Retain the repository-required E501 exemptions on single-line declarations.
# ruff: noqa: RUF100

import sqlalchemy as sa
from alembic import context, op

from src.common.settings import Settings

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def get_schema() -> str:
    """Resolve the investments namespace for this migration invocation."""
    settings = context.config.attributes.get("settings") or Settings()
    return settings.schema_investments


def upgrade() -> None:
    """Key scans by term, language, and window; existing scans are English."""
    s = get_schema()
    op.add_column("scans", sa.Column("language", sa.Text(), nullable=False, server_default="english", comment="GDELT source language"), schema=s)  # noqa: E501
    op.drop_constraint("scans_window_key", "scans", schema=s, type_="unique")
    op.create_unique_constraint("scans_language_window_key", "scans", ["term_id", "language", "window_start", "window_end"], schema=s)  # noqa: E501


def downgrade() -> None:
    """Refuse: Russian scans and their articles would lose their key."""
    raise NotImplementedError("Downgrade would drop non-English scans")
