# Investments Crawler

Collects English and Russian funding news (VC rounds, M&A, IPOs, grants) since 2020 for the technology topics of [vml-crawler-social](../vml-crawler-social) (OpenAlex topics and terms) and extracts funding events from headlines. Results are written to the `investments` schema of the shared PostgreSQL database; social tables are read-only. The funding status and score multiplier are computed in [vml-signals](../vml-signals).

Task: [trends-05](../vml-trends/docs/agents/tasks/trends-05-investments-task.md). Design and source decision: [docs/collection.md](docs/collection.md).

## Quick start

Requires the database populated by vml-crawler-social (search profile).

```bash
uv sync
cp .env.example .env
uv run dagster job execute -m src.collection.defs -j investments_job
```

Runs are resumable. The request budget and the 10-second spacing keep GDELT from blocking the IP.

## Development

```bash
scripts/dev.sh uv sync
scripts/dev.sh uv run python -m pytest
scripts/dev.sh uv run ruff check src tests
scripts/dev.sh uv run ty check src tests
```
