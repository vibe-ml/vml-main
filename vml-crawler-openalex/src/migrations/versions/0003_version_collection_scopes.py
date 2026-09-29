"""Separate collection policies and preserve existing publication history."""

# ruff: noqa: RUF100
import hashlib
import json

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.dialects.postgresql import JSONB

from src.common.settings import Settings

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def get_schemas() -> tuple[str, str]:
    """Resolve the raw and tmd namespaces for this migration invocation."""
    settings = context.config.attributes.get("settings") or Settings()
    return settings.schema_raw, settings.schema_tmd


def upgrade() -> None:
    """Attach legacy runs and selected works to immutable evaluated scopes."""
    raw, tmd = get_schemas()

    # fmt: off
    op.create_table(
        "openalex_work_scopes",
        sa.Column("id", sa.Text(), primary_key=True, comment="Scope SHA-256 definition hash"),  # noqa: E501
        sa.Column("definition", JSONB(), nullable=False, comment="Immutable collection scope criteria JSON"),  # noqa: E501
        schema=tmd,
        comment="Immutable ingestion query definitions and publication date boundaries",
    )
    op.add_column("openalex_work_runs", sa.Column("scope_id", sa.Text(), sa.ForeignKey(f"{tmd}.openalex_work_scopes.id"), comment="Collection scope identifier"), schema=tmd)  # noqa: E501
    op.add_column("openalex_work_current", sa.Column("scope_id", sa.Text(), sa.ForeignKey(f"{tmd}.openalex_work_scopes.id"), comment="Collection scope identifier"), schema=raw)  # noqa: E501
    # fmt: on

    connection = op.get_bind()
    runs_table = sa.Table(
        "openalex_work_runs", sa.MetaData(), schema=tmd, autoload_with=connection
    )
    scopes_table = sa.Table(
        "openalex_work_scopes", sa.MetaData(), schema=tmd, autoload_with=connection
    )

    for lower, upper in connection.execute(
        sa.select(runs_table.c.lower_date, runs_table.c.upper_date).distinct()
    ):
        definition = json.dumps(
            {
                "corpus": "core",
                "publication_from": str(lower),
                "publication_through": str(upper),
                "domain_ids": [],
                "field_ids": [],
                "matching": "primary-topic-or-within-and-between-v1",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        identity = hashlib.sha256(definition.encode()).hexdigest()
        connection.execute(
            sa.insert(scopes_table).values(
                id=identity, definition=json.loads(definition)
            )
        )
        connection.execute(
            sa.update(runs_table)
            .where(
                sa.and_(
                    runs_table.c.lower_date == lower,
                    runs_table.c.upper_date == upper,
                )
            )
            .values(scope_id=identity)
        )

    connection.execute(
        sa.text(f"""
        UPDATE "{tmd}".openalex_work_batches b
        SET manifest = b.manifest || jsonb_build_object('scope_id', s.id)
        FROM "{tmd}".openalex_work_scopes s
        WHERE b.manifest->>'lower_date' = s.definition->>'publication_from'
          AND b.manifest->>'upper_date' = s.definition->>'publication_through'
    """)
    )

    connection.execute(sa.text(f'DELETE FROM "{raw}".openalex_work_current'))
    op.drop_constraint(
        "openalex_work_current_pkey",
        "openalex_work_current",
        schema=raw,
        type_="primary",
    )
    op.alter_column("openalex_work_current", "scope_id", nullable=False, schema=raw)
    op.alter_column("openalex_work_runs", "scope_id", nullable=False, schema=tmd)
    op.create_primary_key(
        "openalex_work_current_pkey",
        "openalex_work_current",
        ["scope_id", "entity_id"],
        schema=raw,
    )

    connection.execute(
        sa.text(f"""
        INSERT INTO "{raw}".openalex_work_current (entity_id, version_id, scope_id)
        SELECT DISTINCT ON (r.scope_id, v.entity_id) v.entity_id, v.id, r.scope_id
        FROM "{raw}".openalex_work_observations o
        JOIN "{tmd}".openalex_work_runs r ON r.id = o.run_id
        JOIN "{raw}".openalex_work_versions v ON v.id = o.version_id
        WHERE r.status = 'complete' AND o.disposition = 'selected'
        ORDER BY r.scope_id, v.entity_id, r.observed_at DESC, r.id DESC, o.row_number DESC
    """)
    )

    # fmt: off
    op.create_table(
        "openalex_work_scans",
        sa.Column("id", sa.Uuid(), primary_key=True, comment="Scan session identifier"),  # noqa: E501
        sa.Column("scope_id", sa.Text(), sa.ForeignKey(f"{tmd}.openalex_work_scopes.id"), nullable=False, comment="Associated collection scope identifier"),  # noqa: E501
        sa.Column("request_key", sa.Text(), nullable=False, comment="Scan partition or request key"),  # noqa: E501
        sa.Column("source_request", JSONB(), nullable=False, comment="Serialized request context or arguments"),  # noqa: E501
        sa.Column("status", sa.Text(), nullable=False, comment="Scan status (active, complete, failed)"),  # noqa: E501
        sa.Column("next_row", sa.BigInteger(), nullable=False, server_default="0", comment="Next record offset or cursor position"),  # noqa: E501
        schema=tmd,
        comment="Resumable scan progress checkpoints, request keys, and next row cursors",
    )
    op.create_index("openalex_work_scans_request", "openalex_work_scans", ["request_key", "status"], schema=tmd)  # noqa: E501
    op.add_column("openalex_work_runs", sa.Column("scan_id", sa.Uuid(), sa.ForeignKey(f"{tmd}.openalex_work_scans.id"), comment="Scan checkpoint identifier"), schema=tmd)  # noqa: E501
    # fmt: on

    connection.execute(
        sa.text(f"""
        INSERT INTO "{tmd}".openalex_work_scans (id, scope_id, request_key, source_request, status, next_row)
        SELECT r.id, r.scope_id, 'legacy:' || r.id::text, '{{}}'::jsonb, r.status,
            CASE WHEN r.status='complete' THEN (SELECT count(*) FROM "{raw}".openalex_work_observations o WHERE o.run_id=r.id) ELSE 0 END
        FROM "{tmd}".openalex_work_runs r
    """)
    )
    connection.execute(sa.text(f'UPDATE "{tmd}".openalex_work_runs SET scan_id=id'))

    op.alter_column("openalex_work_runs", "scan_id", nullable=False, schema=tmd)

    connection.execute(
        sa.text(f"""
        CREATE OR REPLACE FUNCTION "{tmd}".protect_scope() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'Cannot modify or delete an immutable collection scope';
        END;
        $$ LANGUAGE plpgsql;
    """)
    )
    connection.execute(
        sa.text(f"""
        CREATE TRIGGER immutable_scope BEFORE UPDATE OR DELETE ON "{tmd}".openalex_work_scopes
        FOR EACH ROW EXECUTE FUNCTION "{tmd}".protect_scope()
    """)
    )


def downgrade() -> None:
    """Reject lossy merging of scope-specific selection history."""
    raw, tmd = get_schemas()
    connection = op.get_bind()
    if connection.scalar(sa.text(f'SELECT count(*) FROM "{tmd}".openalex_work_scopes')):
        raise NotImplementedError("Scope history cannot be downgraded safely")
    connection.execute(
        sa.text(
            f'DROP TRIGGER IF EXISTS immutable_scope ON "{tmd}".openalex_work_scopes'
        )
    )
    connection.execute(sa.text(f'DROP FUNCTION IF EXISTS "{tmd}".protect_scope()'))
    op.drop_column("openalex_work_runs", "scan_id", schema=tmd)
    op.drop_table("openalex_work_scans", schema=tmd)
    op.drop_constraint(
        "openalex_work_current_pkey",
        "openalex_work_current",
        schema=raw,
        type_="primary",
    )
    op.drop_column("openalex_work_current", "scope_id", schema=raw)
    op.create_primary_key(
        "openalex_work_current_pkey", "openalex_work_current", ["entity_id"], schema=raw
    )
    op.drop_column("openalex_work_runs", "scope_id", schema=tmd)
    op.drop_table("openalex_work_scopes", schema=tmd)
