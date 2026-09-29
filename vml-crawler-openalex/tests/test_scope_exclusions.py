"""Verify domain exclusions and descending inventory sorting."""

from datetime import date

from src.common.settings import Settings
from src.ingestion.bootstrap import inventory
from src.ingestion.scope import CollectionScope


def test_settings_parses_exclude_ids() -> None:
    settings = Settings(
        database_url="postgresql+psycopg://user:pass@localhost:5432/db",
        exclude_domain_ids=["2"],
        exclude_field_ids=["11", "12"],
    )
    assert settings.exclude_domain_ids == ("2",)
    assert settings.exclude_field_ids == ("11", "12")


def test_scope_excludes_social_sciences() -> None:
    scope = CollectionScope(
        publication_from=date(2024, 1, 1),
        publication_through=date(2026, 9, 28),
        exclude_domain_ids=("2",),
    )
    record_social = {
        "id": "https://openalex.org/W1",
        "publication_date": "2026-05-01",
        "is_xpac": False,
        "primary_topic": {
            "id": "https://openalex.org/T1",
            "domain": {"id": "https://openalex.org/domains/2"},
        },
    }
    record_physical = {
        "id": "https://openalex.org/W2",
        "publication_date": "2026-05-01",
        "is_xpac": False,
        "primary_topic": {
            "id": "https://openalex.org/T2",
            "domain": {"id": "https://openalex.org/domains/3"},
        },
    }
    assert scope.disposition(record_social) == "excluded:classification"
    assert scope.disposition(record_physical) == "selected"


def test_inventory_sorts_descending() -> None:
    import json

    manifest = json.dumps(
        {
            "format": "parquet",
            "entity": "works",
            "date": "2026-09-24",
            "content_length": 600,
            "record_count": 3,
            "files": [
                {
                    "url": "s3://openalex/data/parquet/works/updated_date=2025-01-01/part_000.parquet",
                    "meta": {"content_length": 100, "record_count": 1},
                },
                {
                    "url": "s3://openalex/data/parquet/works/updated_date=2026-09-01/part_000.parquet",
                    "meta": {"content_length": 200, "record_count": 1},
                },
                {
                    "url": "s3://openalex/data/parquet/works/updated_date=2026-08-01/part_000.parquet",
                    "meta": {"content_length": 300, "record_count": 1},
                },
            ],
        }
    ).encode()
    _, files = inventory(manifest)
    urls = [f["url"] for f in files]
    assert "updated_date=2026-09-01" in urls[0]
    assert "updated_date=2026-08-01" in urls[1]
    assert "updated_date=2025-01-01" in urls[2]
