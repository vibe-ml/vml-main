# Retrieve a work from VML raw data

Run the commands below from your local terminal. SSH runs queries on `vml`, beside the data, and returns JSON to a local file. No DuckDB server is required.

## Prerequisites and paths

- SSH access through the `vml` alias and permission to run `docker exec`.
- Running container: `dagster-code_openalex-1`. Check with `ssh vml 'docker ps --format "{{.Names}}"'`.
- The container already provides Python, DuckDB, SQLAlchemy, and database configuration. Credentials stay on VML.
- Storage root: `/data/openalex` inside the container, mapped to `/data/demo/openalex` on the host.
- Snapshot inventory: `/data/openalex/raw/snapshot/inventory/*.parquet` inside the container.

These names and paths follow the [VML deployment](deploy/openalex-deployment-vml.md). The examples use its default `raw` and `tmd` database schemas.

Use the full identifier `https://openalex.org/W4407679583` in queries. Replace the work ID and local output filename for another work. Redirection with `>` overwrites the local output file.

## 1. Check ingested records and source paths

PostgreSQL stores complete payloads for ingested work versions. Query it first to avoid searching the entire snapshot. This read-only command returns all stored versions and their source receipts:

```bash
ssh vml 'docker exec -i dagster-code_openalex-1 python -' <<'PY' > work-w4407679583-postgres.json
import json
import os
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

work_id = "https://openalex.org/W4407679583"
engine = create_engine(os.environ["OPENALEX_DATABASE_URL"])
with engine.connect() as connection:
    connection.execute(text("SET TRANSACTION READ ONLY"))
    connection.execute(text("SET statement_timeout = '30s'"))
    versions = connection.execute(text("""
        SELECT id, payload
        FROM raw.openalex_work_versions
        WHERE entity_id = :work_id
    """), {"work_id": work_id}).mappings().all()
    sources = connection.execute(text("""
        SELECT DISTINCT o.version_id, s.id AS source_id,
               s.path, s.release, s.source, s.sha256
        FROM raw.openalex_work_versions AS v
        JOIN raw.openalex_work_observations AS o ON o.version_id = v.id
        JOIN tmd.openalex_work_sources AS s ON s.id = o.source_id
        WHERE v.entity_id = :work_id
    """), {"work_id": work_id}).mappings().all()

print(json.dumps({
    "work_id": work_id,
    "retrieved_at": datetime.now(timezone.utc).isoformat(),
    "versions": [dict(row) for row in versions],
    "sources": [dict(row) for row in sources],
}, ensure_ascii=False, indent=2, default=str))
PY
```

If `versions` contains the needed payload, retrieval is complete. Multiple versions represent stored content variants; their order does not identify the latest version. For the reconciled current version, use `raw.openalex_work_current` with the desired `scope_id` and `entity_id`.

Source paths are relative to the storage root. A source can be a Parquet snapshot or a JSON API response: inspect its path before choosing a reader. An empty `versions` array does not mean the work is absent from downloaded snapshots; ingestion may not have selected or processed it.

## 2. Query a known Parquet file, or locate it first

Set `known_file` below to the absolute **container path** obtained above or from an earlier lookup. Leave it as `None` to search downloaded inventory files. The fallback searches only the `id` column and returns matching filenames before reading complete records.

```bash
ssh vml 'docker exec -i dagster-code_openalex-1 python -' <<'PY' > work-w4407679583-parquet.json
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

work_id = "https://openalex.org/W4407679583"
known_file = None  # Or "/data/openalex/raw/snapshot/inventory/<file>.parquet"
root = Path(os.environ["OPENALEX_STORAGE_ROOT"])
connection = duckdb.connect(config={"threads": 2, "memory_limit": "512MB"})
started = time.monotonic()

if known_file is not None:
    paths = [known_file]
else:
    inventory = sorted(str(path) for path in
                       (root / "raw/snapshot/inventory").glob("*.parquet"))
    if not inventory:
        raise SystemExit("No downloaded inventory Parquet files found")
    paths = [row[0] for row in connection.execute("""
        SELECT DISTINCT filename
        FROM read_parquet(?, filename=true)
        WHERE id = ?
    """, [inventory, work_id]).fetchall()]

locate_seconds = time.monotonic() - started
sources = []
for path in paths:
    rows = connection.execute("""
        SELECT to_json(work)
        FROM read_parquet(?) AS work
        WHERE id = ?
    """, [path, work_id]).fetchall()
    receipt_path = Path(path).with_suffix(".json")
    sources.append({
        "container_path": path,
        "receipt": json.loads(receipt_path.read_text())
                   if receipt_path.exists() else None,
        "records": [json.loads(row[0]) for row in rows],
    })

print(json.dumps({
    "work_id": work_id,
    "retrieved_at": datetime.now(timezone.utc).isoformat(),
    "duckdb_version": duckdb.__version__,
    "locate_seconds": locate_seconds,
    "total_seconds": time.monotonic() - started,
    "sources": sources,
}, ensure_ascii=False, indent=2))
PY
```

Check the command's exit status before using its output. A failed command may leave an empty or incomplete local file. Successful JSON contains the complete matching Parquet rows under `sources[].records[]`. Keep all matches: different files may contain different observations of the same work.

An empty `sources` array means the ID was not found in the downloaded inventory searched. With `known_file` set, an empty `records` array means that file had no matching ID. Neither result establishes absence from OpenAlex or from other local sources, such as API responses.

## Performance and repeat lookups

On September 26, 2026, searching for `W4407679583` across 2,040 inventory files (707 GB on disk) took **260.863 seconds**. Reading the matching record and receipt then took **1.703 seconds**. Another broad search was active on VML, so these are observed timings, not a performance guarantee. Inventory size is not the number of bytes scanned.

The matching container file was:

```text
/data/openalex/raw/snapshot/inventory/7bd6ab32de86596dda784ce0631b111edaae484a9c4859eeef27b1782bd21aeb.parquet
```

Set `known_file` to this path to retrieve this work again without repeating the inventory search, while the file remains available. The saved [work record](agents/research/technologies/work-w4407679583.codex.md) includes its abstract, full JSON, and provenance.

Use exact equality on `id`. Avoid casting every column to text and applying `LIKE '%W4407679583%'`: that reads unrelated data and defeats the narrow lookup. Keep concurrent scans limited because ingestion shares the host. DuckDB's memory setting limits its buffer manager, not total process memory.

## Interpreting and saving the result

- Snapshot values, including citation counts, reflect the stored data, not a live API response.
- Preserve the retrieval timestamp, work ID, source path, and receipt when saving research notes. A receipt checksum is recorded provenance unless you separately recompute it.
- In this snapshot, `abstract_inverted_index` is a JSON-encoded string. Decode it, sort its words by their listed positions, and join them to reconstruct the abstract. Handle a missing or null index as an unavailable abstract.
- Preserve all original fields in the exported JSON. Distinguish reconstructed text and your own summary from the source record.
