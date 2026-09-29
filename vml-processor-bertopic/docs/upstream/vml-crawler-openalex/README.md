# OpenAlex Crawler

Collects OpenAlex taxonomy and scientific works. Supports snapshot ingestion, API refreshes, and version history, with PostgreSQL metadata and local raw-file storage. Dagster orchestrates collection jobs.

## Quick start

Requires Python 3.14+, [uv](https://docs.astral.sh/uv/), PostgreSQL, and an OpenAlex API key.

```bash
uv sync
cp .env.example .env
```

Set database credentials, API key, storage path, and collection scope in `.env`. Run the taxonomy collector:

```bash
uv run dagster job execute -m src.ingestion.job -j taxonomy_job
```

The job applies database migrations automatically. See the [ingestion guide](docs/ingestion.md) for snapshot collection, recovery, and persistent Dagster storage.

## Ingestion

1. **Configure scope:** Select publication dates and optional domain or field filters in `.env`.
2. **Collect taxonomy:** `taxonomy_job` fetches domains, fields, subfields, and topics.
3. **Load works:** `snapshot_job` imports a bounded Parquet sample; `bootstrap_job` ingests the complete works snapshot and selects records within scope.
4. **Refresh works:** `daily_refresh_job` queries API partitions by publication date within the available API budget.
5. **Process batches:** `rebuild_job` processes or rebuilds derived batches from retained inputs.

Raw files remain on disk; PostgreSQL tracks versions, observations, checkpoints, and current selections. Interrupted bootstrap and refresh work can resume. Dagster schedules taxonomy, refresh, and batch processing daily by default.

## Development

```bash
uv run python -m pytest
uv run ruff check src tests
```

Database tests require PostgreSQL; set `TEST_DATABASE_URL` as described in the ingestion guide.

- [Domain glossary](CONTEXT.md)
- [Project settings](src/common/settings.py)
- [Database models](src/models/)
