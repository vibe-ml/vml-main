# Social Crawler

Collects social-media mentions of OpenAlex topics since 2024 and aggregates monthly **mass attention** per topic. Topics and search terms come from the database of [vml-crawler-openalex](../vml-crawler-openalex); results are written to the same PostgreSQL database in a separate `social` schema. OpenAlex tables are read-only. Dagster orchestrates the daily job.

Task: [trends-04](../vml-trends/docs/agents/tasks/trends-04-social-media-task.md). Rationale: [collection design](docs/collection.md).

## Sources

All sources are free. Trust level for all of them is **Low** (GPB hierarchy): social media is an attention indicator, never the sole evidence.

| Platform | API | Channels | Monthly volume |
| --- | --- | --- | --- |
| Hacker News | Algolia search, no key | `hn` (stories + comments) | yes |
| Stack Exchange | API 2.3, optional free app key | one per site (`SOCIAL_STACKEXCHANGE_SITES`) | yes |
| Bluesky | `searchPosts`, free account + app password | `all` | no |

Without Bluesky credentials the platform is skipped and its coverage stays `missing`, not zero.

## Quick start

Requires Python 3.14+, [uv](https://docs.astral.sh/uv/), and the PostgreSQL database populated by vml-crawler-openalex (taxonomy and works).

```bash
uv sync
cp .env.example .env   # database URL, OpenAlex schema, optional keys
uv run dagster job execute -m src.collection.defs -j social_job
```

The job applies migrations, builds the search profile, collects every enabled platform within its request budget, and rebuilds `social.attention`. Repeated runs resume pending scans; a new month adds new windows.

## Development

No local Python? Run everything in Docker; the script also starts a disposable PostgreSQL:

```bash
scripts/dev.sh uv sync
scripts/dev.sh uv run python -m pytest
scripts/dev.sh uv run ruff check src tests
scripts/dev.sh uv run ty check src tests
```

- [Domain glossary](CONTEXT.md)
- [Project settings](src/common/settings.py)
- [Database models](src/models/)
- [Deployment](deploy/README.md)
