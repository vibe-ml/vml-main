"""Read-only measurements for the blanco pilot; run with access to runtime files."""

import gzip
import hashlib
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

import httpx
import sqlalchemy as sa
from dotenv import dotenv_values
from pyarrow import parquet
from sqlalchemy.engine import make_url


def measure() -> dict:
    """Collect deployment evidence without printing credentials or work payloads."""
    values = dotenv_values("/opt/dagster/secrets/openalex.env")
    database_url = values["OPENALEX_DATABASE_URL"]
    if not database_url:
        raise ValueError("Missing database URL")
    url = make_url(database_url).set(host="127.0.0.1")
    root = Path("/data/vml/dev/openalex")
    result = {"observed_at": datetime.now(UTC).isoformat(), "database": url.database}
    engine = sa.create_engine(url, hide_parameters=True)
    try:
        with engine.connect().execution_options(postgresql_readonly=True) as conn:
            metadata = sa.MetaData()
            for schema in ("raw", "tmd"):
                metadata.reflect(
                    bind=conn,
                    schema=schema,
                    only=lambda name, _: name.startswith("openalex_"),
                )
            tables = metadata.tables
            scopes = tables["tmd.openalex_work_scopes"]
            result["scopes"] = [
                dict(row) for row in conn.execute(sa.select(scopes)).mappings()
            ]
            revision = sa.Table(
                "alembic_openalex", metadata, schema="migrations", autoload_with=conn
            )
            result["migration_revision"] = conn.scalar(
                sa.select(revision.c.version_num)
            )
            result["tables"] = {
                name: conn.scalar(sa.select(sa.func.count()).select_from(table))
                for name, table in sorted(tables.items())
            }
            result["database_bytes"] = conn.scalar(
                sa.select(sa.func.pg_database_size(sa.func.current_database()))
            )
            relations = sa.table(
                "pg_stat_user_tables",
                sa.column("schemaname"),
                sa.column("relname"),
                sa.column("relid"),
                schema="pg_catalog",
            )
            result["relations"] = [
                list(row)
                for row in conn.execute(
                    sa.select(
                        relations.c.schemaname,
                        relations.c.relname,
                        sa.func.pg_table_size(relations.c.relid),
                        sa.func.pg_indexes_size(relations.c.relid),
                    ).order_by(relations.c.schemaname, relations.c.relname)
                )
            ]
            bundles = tables["raw.openalex_taxonomy_bundles"]
            result["taxonomy"] = [
                [
                    identity,
                    *(
                        len(records[kind])
                        for kind in ("domains", "fields", "subfields", "topics")
                    ),
                ]
                for identity, records in conn.execute(
                    sa.select(bundles.c.id, bundles.c.records)
                )
            ]
            raw = tables["tmd.openalex_taxonomy_raw_files"]
            result["taxonomy_raw_checksums_valid"] = all(
                hashlib.sha256((root / path).read_bytes()).hexdigest() == compressed
                and hashlib.sha256(
                    gzip.decompress((root / path).read_bytes())
                ).hexdigest()
                == checksum
                for path, checksum, compressed in conn.execute(
                    sa.select(raw.c.path, raw.c.checksum, raw.c.compressed_checksum)
                )
            )
            partitions = tables["tmd.openalex_work_partitions"]
            result["partition_statuses"] = [
                list(row)
                for row in conn.execute(
                    sa.select(
                        partitions.c.status,
                        sa.func.count(),
                        sa.func.sum(partitions.c.record_count),
                    ).group_by(partitions.c.status)
                )
            ]
            allowances = tables["tmd.openalex_work_api_allowances"]
            result["allowances"] = [
                dict(row)
                for row in conn.execute(
                    sa.select(allowances).order_by(allowances.c.day)
                ).mappings()
            ]
            observations = tables["raw.openalex_work_observations"]
            result["work_dispositions"] = [
                list(row)
                for row in conn.execute(
                    sa.select(observations.c.disposition, sa.func.count()).group_by(
                        observations.c.disposition
                    )
                )
            ]
            claims = tables["tmd.openalex_work_batch_claims"]
            result["processing_claims"] = [
                list(row)
                for row in conn.execute(
                    sa.select(claims.c.status, sa.func.count()).group_by(
                        claims.c.status
                    )
                )
            ]
            sources = tables["tmd.openalex_work_sources"]
            result["work_source_checksums_valid"] = all(
                hashlib.sha256((root / path).read_bytes()).hexdigest() == checksum
                for path, checksum in conn.execute(
                    sa.select(sources.c.path, sources.c.sha256)
                )
            )
            versions = tables["raw.openalex_work_versions"]
            corpus = versions.c.payload["is_xpac"].astext
            publication = versions.c.payload["publication_date"].astext
            result["retained_corpus"] = [
                list(row)
                for row in conn.execute(
                    sa.select(
                        corpus,
                        sa.func.count(),
                        sa.func.min(publication),
                        sa.func.max(publication),
                    ).group_by(corpus)
                )
            ]
            batches = tables["tmd.openalex_work_batches"]
            manifests = list(conn.scalars(sa.select(batches.c.manifest)))
            result["derived_rows"] = sum(
                parquet.ParquetFile(root / m["path"]).metadata.num_rows
                for m in manifests
            )
            result["derived_checksums_valid"] = all(
                hashlib.sha256((root / m["path"]).read_bytes()).hexdigest()
                == m["sha256"]
                and parquet.ParquetFile(root / m["path"]).metadata.num_rows
                == m["count"]
                for m in manifests
            )
    finally:
        engine.dispose()
    result["storage"] = {
        name: {
            "files": len(files),
            "bytes": sum(p.stat().st_size for p in files),
        }
        for name in ("raw", "derived", "staging")
        for files in [[p for p in (root / name).rglob("*") if p.is_file()]]
    }
    result["filesystem_free_bytes"] = shutil.disk_usage(root).free
    query = """{ pipelineRunsOrError(limit: 100) { ... on Runs { results {
        runId jobName status startTime endTime tags { key value }
    } } } }"""
    response = httpx.post(
        "http://localhost:3002/graphql", json={"query": query}, timeout=30
    )
    response.raise_for_status()
    result["dagster"] = response.json()
    response = httpx.get(
        "https://api.openalex.org/rate-limit",
        params={"api_key": values["OPENALEX_API_KEY"]},
        timeout=30,
    )
    if response.is_error:
        raise RuntimeError(f"Rate-limit request failed: HTTP {response.status_code}")
    result["rate_limit"] = response.json()["rate_limit"]
    return result


if __name__ == "__main__":
    try:
        evidence = measure()
    except Exception as error:  # noqa: BLE001 - sanitize external failures
        # External exceptions may include credential-bearing URLs.
        print(json.dumps({"error_type": type(error).__name__}))
        raise SystemExit(1) from None
    print(json.dumps(evidence, indent=2, default=str))
