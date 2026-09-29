# PostgreSQL Database Schema & Model Architecture

Overview of schemas, tables, and SQLAlchemy models across OpenAlex data and technical metadata.

---

## Architecture Principles

* **Two PostgreSQL Schemas**:
  * `raw`: Stores ingested OpenAlex entities, payload JSONB, current active states, and observation histories.
  * `tmd`: Stores Technical Metadata — pipeline execution lifecycles, checkpoints, raw file receipts, rate limit allowances, and worker claims.
* **Table Naming**: All PostgreSQL tables carry the source prefix `openalex_`.
* **SQLAlchemy Models**:
  * Defined in `src/models/raw.py` and `src/models/tmd.py`.
  * Class names are in PascalCase **without** prefix (e.g. `TaxonomyBundles`, `TaxonomyRuns`).
  * Inherit from `Base(DeclarativeBase)` in `src/models/base.py`.

---

## 1. Schema `raw` — Data & Lineage (`src/models/raw.py`)

Total: 6 tables / models.

| SQLAlchemy Model | PostgreSQL Table | Legacy Table | Purpose |
|---|---|---|---|
| `TaxonomyBundles` | `raw.openalex_taxonomy_bundles` | `bundles` | Canonical taxonomy tree (`records` JSONB: domains, fields, subfields, topics) |
| `TaxonomyObservations` | `raw.openalex_taxonomy_observations` | `observations` | Sighting history per taxonomy bundle |
| `WorkVersions` | `raw.openalex_work_versions` | `work_versions` | Deduplicated work records and full entities (`payload` JSONB) |
| `WorkCurrent` | `raw.openalex_work_current` | `work_current` | Active reconciled version per work entity and collection scope |
| `WorkObservations` | `raw.openalex_work_observations` | `work_observations` | Sighting history and disposition (`selected`, quarantine) per work |
| `WorkProcessing` | `raw.openalex_work_processing` | `work_processing` | Transformation lineage connecting entity versions to derived batches |

---

## 2. Schema `tmd` — Technical Metadata & Orchestration (`src/models/tmd.py`)

Total: 15 tables / models.

| SQLAlchemy Model | PostgreSQL Table | Legacy Table | Purpose |
|---|---|---|---|
| `TaxonomyRuns` | `tmd.openalex_taxonomy_runs` | `attempts` | Taxonomy run lifecycle execution status and errors |
| `TaxonomyRawFiles` | `tmd.openalex_taxonomy_raw_files` | `raw_objects` | Storage metadata and checksums for raw taxonomy API responses |
| `WorkRuns` | `tmd.openalex_work_runs` | `work_runs` | Work collection execution lifecycle (snapshot, bootstrap, API) |
| `WorkSources` | `tmd.openalex_work_sources` | `work_sources` | Ingested snapshot Parquet files and raw API response file receipts |
| `WorkScopes` | `tmd.openalex_work_scopes` | `work_scopes` | Immutable ingestion query definitions and publication date boundaries |
| `WorkScans` | `tmd.openalex_work_scans` | `work_scans` | Resumable scan progress checkpoints, request keys, and next row cursors |
| `WorkReleases` | `tmd.openalex_work_releases` | `work_releases` | Quarterly snapshot release manifest metadata and acquisition status |
| `WorkReleaseFiles` | `tmd.openalex_work_release_files` | `work_release_files` | Snapshot file inventory manifests and download status |
| `WorkBaselines` | `tmd.openalex_work_baselines` | `work_baselines` | Scope-specific baseline selection configurations and status |
| `WorkChunks` | `tmd.openalex_work_chunks` | `work_chunks` | Row-chunk ranges for parallel bootstrap baseline processing |
| `WorkPartitions` | `tmd.openalex_work_partitions` | `work_partitions` | Daily API refresh partition state machine, leases, and cursor tracking |
| `WorkApiPages` | `tmd.openalex_work_api_pages` | `work_api_pages` | Ingested API response page logs, cursors, timestamps, and staging paths |
| `WorkApiAllowances` | `tmd.openalex_work_api_allowances` | `work_api_allowances` | Daily API request quotas and reserved request counters |
| `WorkBatches` | `tmd.openalex_work_batches` | `work_batches` | Derived Parquet transform batch manifests linked to taxonomy bundles |
| `WorkBatchClaims` | `tmd.openalex_work_batch_claims` | `work_batch_claims` | Distributed worker claims for derived batch transformation tasks |
