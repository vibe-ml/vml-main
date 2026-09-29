# OpenAlex ingestion

```mermaid
flowchart TD
    Settings[Collection scope and settings] --> Snapshot
    Settings --> Refresh
    TaxonomyAPI[OpenAlex taxonomy API] --> Taxonomy[taxonomy_job]
    Taxonomy --> Bundle[Committed taxonomy bundle]
    Parquet[OpenAlex Parquet snapshot] --> Snapshot[snapshot_job: bounded sample<br/>bootstrap_job: full inventory]
    WorksAPI[OpenAlex works API] --> Refresh[daily_refresh_job<br/>Publication-date partitions]
    Snapshot --> Raw[Retained raw files and checksums]
    Refresh --> Raw
    Raw --> Select[Validate and select core works within scope]
    Bundle --> Select
    Select --> Catalog[PostgreSQL catalog<br/>Selected payloads and progress]
    Catalog --> Process[rebuild_job / process_batches<br/>One source claim per invocation]
    Bundle --> Process
    Process --> Output[Derived Parquet and committed manifests]
    Output --> Consumers[Downstream consumers]
```

Ingestion collects taxonomy and scientific works, retains original source files, and publishes selected works as derived Parquet. PostgreSQL tracks collection state; Dagster runs and schedules the jobs.

## How it works

1. **Freeze the collection scope.** A new scan uses inclusive publication dates, defaulting to January 1, 2026 through the current UTC date. Optional domain and field filters apply to the primary topic. Resumed scans keep their original scope.
2. **Collect taxonomy.** `taxonomy_job` fetches domains, fields, subfields, and topics. Only a complete, validated bundle becomes current. Selection and derived publication require valid taxonomy.
3. **Acquire works.** Choose a bounded sample with `snapshot_job`, the full snapshot with `bootstrap_job`, or API updates with `daily_refresh_job`. Full bootstrap downloads the complete inventory before selecting records; scope filters do not reduce its downloads.
4. **Select records.** Retain core works (`is_xpac=false`) within scope. Invalid publication dates and unknown corpus flags are quarantined. Preserve original source files and checksums for inspection and reprocessing.
5. **Process outputs.** `process_batches` claims one source batch, reads selected payloads from PostgreSQL, reconstructs abstracts, and writes immutable Parquet plus a committed manifest. `snapshot_job` invokes processing after collection; bootstrap and daily refresh rely on separate `rebuild_job` runs to drain the queue.
6. **Consume committed data.** Use `WorksCatalog(settings).read_works(scope_id)` and its `batches` manifests. Output columns are `entity_id`, `version_id`, and JSON `payload`. Manifests carry paths, checksums, counts, scope, source, and taxonomy dependencies. Directory listings do not establish publication.

## Dagster

Dagster orchestrates the data pipeline, managing schedules, dependencies, execution retries, and decoupled batch processing.

```mermaid
flowchart TD
    subgraph Daily Schedules
        T_Sched[daily_taxonomy_schedule<br/>00:00 UTC] --> T_Job[taxonomy_job]
        R_Sched[daily_refresh_schedule<br/>00:15 UTC] --> R_Job[daily_refresh_job]
    end

    subgraph Continuous Processing
        B_Sched[process_batches_schedule<br/>every 5 min] --> Reb_Job[rebuild_job]
    end

    subgraph Bulk Acquisition
        Manual[Manual / Resumed] --> Boot_Job[bootstrap_job]
        Sample[Manual] --> Snap_Job[snapshot_job]
    end

    T_Job -->|Publishes| TaxBundle[(Taxonomy Bundle)]
    TaxBundle -.->|Required dependency| Boot_Job
    TaxBundle -.->|Required dependency| R_Job
    TaxBundle -.->|Required dependency| Reb_Job

    Boot_Job -->|Downloads Parquet & Creates| Claims[(Batch Claims)]
    R_Job -->|Fetches API & Creates| Claims
    Snap_Job -->|Direct Processing| DerivedParquet[Derived Parquet & Manifests]

    Reb_Job -->|Claims & Transforms| Claims
    Reb_Job -->|Writes| DerivedParquet
```

### Job workflows

1. **`taxonomy_job` (Daily, 00:00 UTC):**
   - Fetches domains, fields, subfields, and topics from the OpenAlex taxonomy API.
   - Computes canonical entity hashes, validates hierarchy references, and atomically commits a new taxonomy snapshot bundle to `raw.openalex_taxonomy_bundles`.
   - Establishes the taxonomy classification hash required for downstream work filtering and batch claims.

2. **`bootstrap_job` (Manual / Long-Running Resume):**
   - Fetches the quarterly works manifest from S3 (`manifest.json`, 2,040 files, ~707 GB).
   - Iterates through the inventory, downloading part files to local storage with exponential backoff retries.
   - Reads Parquet rows into DuckDB, evaluates scope criteria (`publication_from`, topics, `is_xpac=false`), and writes selected versions to `raw.openalex_work_versions`.
   - Divides accepted records into discrete chunks (`tmd.openalex_work_chunks`) and registers pending claims in `tmd.openalex_work_batch_claims`.

3. **`snapshot_job` (Manual Sample):**
   - Downloads a single, bounded Parquet sample (up to 64 MB / 10,000 rows).
   - Filters records by scope and immediately runs `process_batches` to validate end-to-end ingestion and derived Parquet generation in a single run.

4. **`daily_refresh_job` (Daily, 00:15 UTC):**
   - Queries OpenAlex `/rate-limit` to check credit balance and calculate today's request capacity against `OPENALEX_API_REQUEST_LIMIT`.
   - Divides the budget between recent dates (70%) and older rotating publication dates (30%).
   - Requests pages via cursor pagination, logs raw HTTP responses in `tmd.openalex_work_api_pages`, selects scoped works, and issues batch claims. Pauses if the daily budget is exhausted.

5. **`rebuild_job` (Every 5 minutes, `*/5 * * * *`):**
   - Continuously drains the processing queue generated by `bootstrap_job` and `daily_refresh_job`.
   - Claims pending batches from `tmd.openalex_work_batch_claims`.
   - Reconstructs inverted abstracts into plain text and writes immutable Zstandard-compressed Parquet files to `data/openalex/derived/`.
   - Emits a manifest recording input source IDs, counts, SHA-256 hashes, and taxonomy dependencies to `tmd.openalex_work_batches`.

### Deployment and schedules

Schedules are defined in [`src/ingestion/defs.py`](../src/ingestion/defs.py):

| Schedule | Job | Cron | Purpose |
| --- | --- | --- | --- |
| `daily_taxonomy_schedule` | `taxonomy_job` | `0 0 * * *` UTC | Refresh subject taxonomy |
| `daily_refresh_schedule` | `daily_refresh_job` | `15 0 * * *` UTC | Incremental daily API sync |
| `process_batches_schedule` | `rebuild_job` | `*/5 * * * *` UTC | Drain batch claims into derived Parquet |

Set `OPENALEX_INGESTION_CADENCE` for refresh and shared defaults; `OPENALEX_TAXONOMY_CADENCE` and `OPENALEX_BATCH_CADENCE` override individual schedules. In production, Dagster daemon executes runs in isolated Docker containers via `DockerRunLauncher`.

## Configuration and running

Install with `uv sync`, copy `.env.example` to `.env`, and configure:

- `OPENALEX_DATABASE_URL`: PostgreSQL URL using `postgresql+psycopg`.
- `OPENALEX_API_KEY`: API credentials.
- `OPENALEX_STORAGE_ROOT`: raw and derived storage; defaults to `data/openalex`.
- `OPENALEX_PUBLICATION_FROM` / `OPENALEX_PUBLICATION_THROUGH`: inclusive date bounds; omit the latter to use the new scan's UTC date.
- `OPENALEX_DOMAIN_IDS` / `OPENALEX_FIELD_IDS`: JSON lists; empty lists impose no restriction.

See [settings.py](../src/common/settings.py) for all settings and defaults. PostgreSQL uses `raw` for collected data and `tmd` for technical metadata, with `openalex_` table prefixes. The legacy `OPENALEX_COLLECTION_SCHEMA` setting in `.env.example` overrides these schema names. Migration state is stored in `migrations.alembic_<OPENALEX_ALEMBIC_NAME>`.

```bash
uv run alembic upgrade head
uv run dagster job execute -m src.ingestion.job -j taxonomy_job
uv run dagster job execute -m src.ingestion.scheduler -j daily_refresh_job
uv run dagster job execute -m src.ingestion.rebuild -j rebuild_job
```

For a bounded Parquet run, follow the [sample configuration](agents/tasks/ingestion/snapshot-sample.md). Full bootstrap requires storage for the entire source inventory plus derived files and database growth:

```bash
uv run dagster job execute -m src.ingestion.bootstrap -j bootstrap_job
```

For persistent Dagster run/event storage, provision a separate database, export `DAGSTER_POSTGRES_URL=postgresql://USER:PASSWORD@HOST:5432/DAGSTER_DATABASE`, and set `DAGSTER_HOME="$PWD/config"`. [config/dagster.yaml](../config/dagster.yaml) defines the instance storage.

## Recovery and progress

- **Taxonomy:** failed or incomplete attempts leave the previous valid bundle available. Rerunning reacquires all taxonomy pages.
- **Bootstrap:** reruns reuse registered downloads and committed selection chunks. Transient downloads retry with configurable backoff. An explicit `collect_bootstrap.config.scan_id` resumes a frozen scan. A changed upstream manifest can supersede the inventory.
- **API refresh:** committed pages retain cursors and request parameters. Leases fence concurrent workers; budget pauses retain progress. Rejected cursors restart pagination for that partition.
- **Processing:** pending or expired claims can be reclaimed. Run `rebuild_job` repeatedly or schedule it frequently to drain a backlog; one invocation does not process the entire queue.

Inspect Dagster runs and catalog state separately for acquisition, selection, partition completion, and processing. Download completion does not imply that selected outputs are ready.

## Current readiness

As documented through September 25, 2026:

- Taxonomy, bounded API collection, and derived Parquet publication have live validation. The bounded snapshot sample validated retention and exclusion, but contained no selected core works.
- Full bootstrap has been launched and resumed after a network failure. Completion and sustained bulk-processing capacity are not yet established by the recorded evidence.
- Budget allocation/fairness repairs and multi-day capacity validation remain open. See [budget status](agents/tasks/ingestion/issues/07-allocate-refresh-budget.md) and [capacity validation](agents/tasks/ingestion/issues/11-validate-live-capacity.md).

These are recorded validation limits, not a live deployment status. Deployment evidence is in the [deployment report](agents/research/deploy-03-validation.md) and [bootstrap recovery report](agents/tasks/fetch-snapshot/investigate-01-report.md).

## Development checks

```bash
uv run python -m pytest
uv run ty check src tests
uv run ruff check src tests
uv run ruff format --check src tests
```

Database tests use disposable PostgreSQL schemas and temporary storage. Set `TEST_DATABASE_URL` to a `postgresql+psycopg` URL; the local fixture can also use `local-db-pg`. Live acquisition and capacity checks are separate from the deterministic test suite.
