"""Bounded snapshot acceptance through ingestion jobs and consumer reads."""

import hashlib
import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import httpx
from dagster import ExecuteInProcessResult

from src.ingestion.job import Runtime
from src.ingestion.snapshot import snapshot_job
from src.ingestion.works_store import WorksCatalog
from tests.test_taxonomy import Database, run


def sample(tmp_path: Path, rows: list[dict]) -> bytes:
    """Encode realistic structured source rows with compressed Parquet."""
    source = tmp_path / "fixture.json"
    source.write_text(json.dumps(rows))
    output = tmp_path / "fixture.parquet"
    with duckdb.connect() as connection:
        connection.execute(
            "CREATE TABLE fixture AS SELECT * FROM read_json_auto(?)", [str(source)]
        )
        connection.execute(
            "COPY fixture TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(output)]
        )
    return output.read_bytes()


def execute(
    catalog: WorksCatalog,
    body: bytes,
    row_count: int,
    failure: Callable[[str], None] = lambda stage: None,
    now: Callable[[], datetime] | None = None,
    overrides: Mapping[str, object] | None = None,
) -> ExecuteInProcessResult:
    """Run the ingestion job with a fixture transport and frozen clock."""
    config = {
        "release": "2026-Q3",
        "source": "https://fixture.test/works.parquet",
        "size": len(body),
        "rows": row_count,
        "sha256": hashlib.sha256(body).hexdigest(),
    } | dict(overrides or {})
    runtime = Runtime(
        catalog.settings,
        httpx.MockTransport(lambda request: httpx.Response(200, content=body)),
        now or (lambda: datetime(2026, 9, 23, tzinfo=UTC)),
        failure,
    )
    return snapshot_job.execute_in_process(
        resources={"runtime": runtime},
        run_config={"ops": {"collect_snapshot": {"config": config}}},
        raise_on_error=False,
    )


def test_bounded_selection_fidelity_and_replay(
    database: Database, tmp_path: Path
) -> None:
    """Retain raw evidence while selecting both UTC date boundaries."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    rows = [
        {
            "id": f"https://openalex.org/W{i}",
            "publication_date": date,
            "is_xpac": flag,
            "title": "A work",
            "doi": "https://doi.org/10.1234/example",
            "ids": {"openalex": f"https://openalex.org/W{i}", "pmid": None},
            "publication_year": 2026,
            "type": "article",
            "language": "en",
            "is_retracted": False,
            "primary_topic": {
                "id": "https://openalex.org/T99999",
                "display_name": "Unrestricted",
            },
            "topics": [{"id": "https://openalex.org/T99999", "score": 0.75}],
            "primary_location": {
                "landing_page_url": "https://example.test/work",
                "source": {"id": "https://openalex.org/S1"},
            },
            "best_oa_location": None,
            "updated_date": "2026-09-01",
            "created_date": "2026-01-01",
            "abstract_inverted_index": None,
            "unknown": {"null": None},
            "authorships": [
                {
                    "author": {"id": "https://openalex.org/A1", "display_name": "Ada"},
                    "institutions": [
                        {
                            "id": "https://openalex.org/I1",
                            "country_code": "ES",
                            "display_name": "Institute",
                        }
                    ],
                }
            ],
        }
        for i, (date, flag) in enumerate(
            [
                ("2026-01-01", False),
                ("2026-09-23", False),
                ("2026-09-24", False),
                ("2025-12-31", False),
                ("2026-05-01", True),
                (None, False),
                ("invalid", False),
                ("2026-04-01", None),
            ],
            1,
        )
    ]
    rows[0]["abstract_inverted_index"] = {"World": [1], "Hello": [0]}
    rows[1]["abstract_inverted_index"] = json.dumps({"Second": [1], "The": [0]})
    body = sample(tmp_path, rows)
    assert execute(catalog, body, len(rows)).success
    state = catalog.read_works()
    assert {row["entity_id"] for row in state["current"]} == {
        "https://openalex.org/W1",
        "https://openalex.org/W2",
    }
    assert len(state["quarantine"]) == 3
    assert len(state["observations"]) == 8
    assert (tmp_path / state["sources"][0]["path"]).read_bytes() == body
    manifest = state["batches"][0]["manifest"]
    assert manifest["taxonomy_snapshot_id"] == taxonomy.read()["current_bundle_id"]
    assert (
        manifest["sha256"]
        == hashlib.sha256((tmp_path / manifest["path"]).read_bytes()).hexdigest()
    )
    assert manifest["source_sha256"] == hashlib.sha256(body).hexdigest()
    assert manifest["count"] == 2 and manifest["source_count"] == 8
    assert manifest["lower_date"] == "2026-01-01"
    assert manifest["upper_date"] == "2026-09-23"
    with duckdb.connect() as connection:
        derived = [
            json.loads(row[0])
            for row in connection.execute(
                "SELECT payload FROM read_parquet(?) ORDER BY entity_id",
                [str(tmp_path / manifest["path"])],
            ).fetchall()
        ]
    assert derived[0] == rows[0] | {"abstract": "Hello World"}
    assert derived[1] == rows[1] | {"abstract": "The Second"}
    assert execute(catalog, body, len(rows)).success
    again = catalog.read_works()
    assert len(again["versions"]) == len(again["processing"]) == 2
    assert len(again["batches"]) == 1


def test_acquires_evidence_while_taxonomy_publication_waits(
    database: Database, tmp_path: Path
) -> None:
    """Missing taxonomy withholds output without discarding work observations."""
    from src.common.settings import Settings

    catalog = WorksCatalog(
        Settings(
            database_url=database[0],
            collection_schema=database[1],
            storage_root=tmp_path,
        )
    )
    body = sample(
        tmp_path,
        [
            {
                "id": "https://openalex.org/W1",
                "publication_date": "2026-01-01",
                "is_xpac": False,
            }
        ],
    )
    result = execute(catalog, body, 1)
    assert result.success
    state = catalog.read_works()
    assert state["runs"][0]["status"] == "waiting_taxonomy"
    assert len(state["observations"]) == len(state["versions"]) == 1
    assert state["batches"] == state["current"] == []
    run(database, tmp_path)
    assert execute(catalog, body, 1).success
    assert len(catalog.read_works()["batches"]) == 1


def test_empty_selection_publishes_valid_zero_count_manifest(
    database: Database, tmp_path: Path
) -> None:
    """A validated sample with no selected works still records completion."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    body = sample(
        tmp_path,
        [
            {
                "id": "https://openalex.org/W1",
                "publication_date": "2027-01-01",
                "is_xpac": False,
            }
        ],
    )
    assert execute(catalog, body, 1).success
    state = catalog.read_works()
    assert len(state["batches"]) == 1
    assert state["batches"][0]["manifest"]["count"] == 0
    assert state["current"] == []


import pytest


@pytest.mark.parametrize(
    "stage",
    [
        "after_snapshot_raw_publish",
        "after_snapshot_output_publish",
        "before_snapshot_commit",
        "after_snapshot_commit",
    ],
)
def test_failures_expose_only_committed_outputs(
    database: Database, tmp_path: Path, stage: str
) -> None:
    """Faults never expose a manifest or selection before database commit."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    body = sample(
        tmp_path,
        [
            {
                "id": "https://openalex.org/W1",
                "publication_date": "2026-01-01",
                "is_xpac": False,
            }
        ],
    )

    def interrupt(point: str) -> None:
        """Inspect the consumer view while the publication is in progress."""
        if point == stage:
            state = catalog.read_works()
            expected = 1 if stage == "after_snapshot_commit" else 0
            assert len(state["batches"]) == len(state["current"]) == expected
            raise OSError("injected failure")

    assert not execute(catalog, body, 1, failure=interrupt).success
    assert execute(catalog, body, 1).success
    state = catalog.read_works()
    assert (
        len(state["batches"]) == len(state["versions"]) == len(state["processing"]) == 1
    )


@pytest.mark.parametrize("defect", ["size", "checksum", "rows", "unreadable"])
def test_invalid_source_never_publishes(
    database: Database, tmp_path: Path, defect: str
) -> None:
    """Reject source integrity failures before publishing selected data."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    body = sample(
        tmp_path,
        [
            {
                "id": "https://openalex.org/W1",
                "publication_date": "2026-01-01",
                "is_xpac": False,
            }
        ],
    )
    overrides = {}
    if defect == "size":
        overrides["size"] = len(body) + 1
    elif defect == "checksum":
        overrides["sha256"] = "0" * 64
    elif defect == "rows":
        overrides["rows"] = 2
    else:
        body = b"not parquet"
    assert not execute(catalog, body, 1, overrides=overrides).success
    state = catalog.read_works()
    assert state["batches"] == state["versions"] == state["processing"] == []
    assert state["runs"][0]["status"] == "failed"


def test_upper_date_is_evaluated_in_utc_each_run(
    database: Database, tmp_path: Path
) -> None:
    """A new UTC date admits newly in-scope records without a code change."""
    from datetime import timedelta, timezone

    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    body = sample(
        tmp_path,
        [
            {
                "id": "https://openalex.org/W1",
                "publication_date": "2026-09-24",
                "is_xpac": False,
            }
        ],
    )
    assert execute(
        catalog,
        body,
        1,
        now=lambda: datetime(2026, 9, 24, 1, tzinfo=timezone(timedelta(hours=2))),
    ).success
    assert catalog.read_works()["current"] == []
    assert execute(
        catalog, body, 1, now=lambda: datetime(2026, 9, 24, tzinfo=UTC)
    ).success
    assert len(catalog.read_works()["current"]) == 1


def test_concurrent_consumer_reads_keep_publication_consistent(
    database: Database, tmp_path: Path
) -> None:
    """Every consumer view resolves current versions during concurrent commits."""
    from concurrent.futures import ThreadPoolExecutor

    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    bodies = [
        sample(
            tmp_path,
            [
                {
                    "id": f"https://openalex.org/W{index}",
                    "publication_date": "2026-01-01",
                    "is_xpac": False,
                }
            ],
        )
        for index in range(12)
    ]

    def publish() -> None:
        """Run independent snapshot publications while consumers poll."""
        for body in bodies:
            assert execute(catalog, body, 1).success

    with ThreadPoolExecutor(max_workers=1) as pool:
        writer = pool.submit(publish)
        while not writer.done():
            state = catalog.read_works()
            versions = {row["id"] for row in state["versions"]}
            assert all(row["version_id"] in versions for row in state["current"])
        writer.result()


@pytest.mark.parametrize("kind", ["record", "sample"])
def test_expanded_source_limits_preserve_raw_without_publication(
    database: Database, tmp_path: Path, kind: str
) -> None:
    """Highly compressed source objects cannot bypass expanded data limits."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    count, length = (1, 1024 * 1024 + 1) if kind == "record" else (20, 900000)
    body = sample(
        tmp_path,
        [
            {
                "id": f"https://openalex.org/W{index}",
                "publication_date": "2026-01-01",
                "is_xpac": False,
                "title": "x" * length,
            }
            for index in range(count)
        ],
    )
    assert len(body) < 1024 * 1024
    assert not execute(catalog, body, count).success
    assert catalog.read_works()["batches"] == []
    assert next((tmp_path / "raw" / "snapshot").glob("*.parquet")).read_bytes() == body


def test_explicit_scope_includes_future_dates(
    database: Database, tmp_path: Path
) -> None:
    """Operators can select future evidence with inclusive explicit bounds."""
    _, taxonomy, _ = run(database, tmp_path)
    settings = taxonomy.settings.model_copy(
        update={"publication_from": "2026-12-01", "publication_through": "2026-12-31"}
    )
    catalog = WorksCatalog(settings)
    rows = [
        {"id": f"W{i}", "publication_date": day, "is_xpac": False}
        for i, day in enumerate(
            ["2026-11-30", "2026-12-01", "2026-12-31", "2027-01-01"]
        )
    ]
    assert execute(catalog, sample(tmp_path, rows), len(rows)).success
    state = catalog.read_works()
    assert {row["entity_id"] for row in state["current"]} == {"W1", "W2"}
    assert state["scopes"][0]["definition"]["publication_through"] == "2026-12-31"


@pytest.mark.parametrize(
    ("domains", "fields", "expected"),
    [
        ([], [], {"W0", "W1", "W2", "W3", "W4"}),
        (["1", "2"], [], {"W0", "W1", "W2"}),
        ([], ["11", "12"], {"W0", "W1", "W3"}),
        (["1", "2"], ["11", "12"], {"W0", "W1"}),
    ],
)
def test_primary_classification_restrictions(
    database: Database,
    tmp_path: Path,
    domains: list[str],
    fields: list[str],
    expected: set[str],
) -> None:
    """OR within lists, AND between lists; other topics never grant membership."""
    _, taxonomy, _ = run(database, tmp_path)
    settings = taxonomy.settings.model_copy(
        update={"domain_ids": domains, "field_ids": fields}
    )
    catalog = WorksCatalog(settings)
    rows = [
        {
            "id": f"W{i}",
            "publication_date": "2026-05-01",
            "is_xpac": False,
            "primary_topic": {
                "domain": {"id": f"https://openalex.org/domains/{domain}"},
                "field": {"id": f"https://openalex.org/fields/{field}"},
            }
            if domain
            else None,
            "topics": [
                {
                    "domain": {"id": "https://openalex.org/domains/1"},
                    "field": {"id": "https://openalex.org/fields/11"},
                }
            ],
        }
        for i, (domain, field) in enumerate(
            [(1, 11), (2, 12), (1, 13), (3, 11), (None, None)]
        )
    ]
    assert execute(catalog, sample(tmp_path, rows), len(rows)).success
    state = catalog.read_works()
    assert {row["entity_id"] for row in state["current"]} == expected
    definition = state["scopes"][0]["definition"]
    assert definition["domain_ids"] == domains
    assert definition["field_ids"] == fields


def test_unfinished_scan_freezes_scope_across_dates_and_configuration(
    database: Database, tmp_path: Path
) -> None:
    """An interrupted scan resumes its original policy under changed settings."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    rows = [
        {"id": f"W{i}", "publication_date": day, "is_xpac": False}
        for i, day in enumerate(["2026-01-01", "2026-09-24"])
    ]
    body = sample(tmp_path, rows)

    def stop(stage: str) -> None:
        if stage == "before_snapshot_commit":
            raise OSError("stop before commit")

    assert not execute(catalog, body, 2, failure=stop).success
    before = catalog.read_works()
    scan = before["scans"][0]
    assert scan["status"] == "failed"
    assert scan["next_row"] == 0
    changed = WorksCatalog(
        taxonomy.settings.model_copy(
            update={
                "publication_from": "2026-09-24",
                "publication_through": "2026-12-31",
                "domain_ids": ["1"],
            }
        )
    )
    assert execute(
        changed,
        body,
        2,
        now=lambda: datetime(2026, 9, 25, tzinfo=UTC),
        overrides={"scan_id": str(scan["id"])},
    ).success
    after = catalog.read_works()
    assert len(after["scopes"]) == len(after["scans"]) == 1
    assert after["scans"][0]["scope_id"] == scan["scope_id"]
    assert after["scans"][0]["next_row"] == 2
    assert after["scans"][0]["status"] == "complete"
    assert {row["entity_id"] for row in after["current"]} == {"W0"}
    assert after["batches"][0]["manifest"]["upper_date"] == "2026-09-23"


def test_changed_scope_has_independent_coverage_and_retains_history(
    database: Database, tmp_path: Path
) -> None:
    """Changing restrictions never borrows old scope completion or membership."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    rows = [
        {
            "id": f"W{i}",
            "publication_date": "2026-01-01",
            "is_xpac": False,
            "primary_topic": {
                "domain": {"id": f"https://openalex.org/domains/{i + 1}"},
                "field": {"id": "https://openalex.org/fields/11"},
            },
        }
        for i in range(2)
    ]
    body = sample(tmp_path, rows)
    assert execute(catalog, body, 2).success
    initial = catalog.read_works()
    original_scope = initial["scopes"][0]["id"]
    changed = WorksCatalog(
        taxonomy.settings.model_copy(
            update={
                "domain_ids": ["https://openalex.org/domains/1"],
                "field_ids": ["11", "12"],
            }
        )
    )

    def stop(stage: str) -> None:
        if stage == "before_snapshot_commit":
            raise OSError("stop new scope")

    assert not execute(changed, body, 2, failure=stop).success
    unfinished = changed.read_works()
    new_scope = next(
        row["id"] for row in unfinished["scopes"] if row["id"] != original_scope
    )
    scoped = changed.read_works(scope_id=new_scope)
    assert scoped["batches"] == scoped["current"] == []
    assert scoped["scans"][0]["status"] == "failed"
    assert execute(changed, body, 2).success
    scoped = changed.read_works(scope_id=new_scope)
    assert {row["entity_id"] for row in scoped["current"]} == {"W0"}
    assert scoped["scans"][0]["status"] == "complete"
    manifest = scoped["batches"][0]["manifest"]
    assert manifest["api_parameters"] == {
        "corpus": "core",
        "filter": "from_publication_date:2026-01-01,to_publication_date:2026-09-23,primary_topic.domain.id:1,primary_topic.field.id:11|12",
    }
    assert manifest["selected_count"] == 1
    assert (
        manifest["count"] == 0
    )  # Existing content does not need another transformation.
    retained = changed.read_works(scope_id=original_scope)
    assert {row["entity_id"] for row in retained["current"]} == {"W0", "W1"}
    final = changed.read_works()
    assert len(final["versions"]) == len(final["processing"]) == 2
    assert len(final["observations"]) == 4


def test_default_resume_does_not_expand_at_midnight(
    database: Database, tmp_path: Path
) -> None:
    """Automatic retry resumes the unfinished default scan before newer dates."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    rows = [{"id": "W1", "publication_date": "2026-09-24", "is_xpac": False}]
    body = sample(tmp_path, rows)

    def stop(stage: str) -> None:
        if stage == "after_snapshot_raw_publish":
            raise OSError("interrupted acquisition")

    assert not execute(catalog, body, 1, failure=stop).success
    assert execute(
        catalog, body, 1, now=lambda: datetime(2026, 9, 24, tzinfo=UTC)
    ).success
    state = catalog.read_works()
    assert state["current"] == []
    assert len(state["scopes"]) == len(state["scans"]) == 1
    assert state["scopes"][0]["definition"]["publication_through"] == "2026-09-23"
    assert execute(
        catalog, body, 1, now=lambda: datetime(2026, 9, 24, tzinfo=UTC)
    ).success
    assert len(catalog.read_works()["current"]) == 1


@pytest.mark.parametrize(
    "policy",
    [
        {"publication_from": "2026-12-31", "publication_through": "2026-01-01"},
        {"domain_ids": ["1|2"]},
        {"field_ids": ["https://openalex.org/domains/1"]},
    ],
)
def test_invalid_scope_never_publishes(
    database: Database, tmp_path: Path, policy: dict
) -> None:
    """Invalid bounds and identifiers cannot broaden source or local selection."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings.model_copy(update=policy))
    rows = [{"id": "W1", "publication_date": "2026-01-01", "is_xpac": False}]
    assert not execute(catalog, sample(tmp_path, rows), 1).success
    state = catalog.read_works()
    assert state["scopes"] == state["scans"] == state["batches"] == []


def test_migrated_unfinished_scan_can_bind_source_without_expanding_scope(
    database: Database, tmp_path: Path
) -> None:
    """Operators can resume pre-scope runs whose original request was not stored."""
    from uuid import uuid4

    from alembic import command
    from sqlalchemy import text

    from src.common.settings import Settings
    from src.migrations import configuration

    settings = Settings(
        database_url=database[0], collection_schema=database[1], storage_root=tmp_path
    )
    catalog = WorksCatalog(settings)
    command.upgrade(configuration(settings), "0002")
    original_run = str(uuid4())
    # A revision-0002 fixture: failed acquisition retained dates but no source request.
    with catalog.transaction() as connection:
        connection.execute(
            text(
                f"INSERT INTO \"{settings.schema_tmd}\".openalex_work_runs (id, observed_at, lower_date, upper_date, status) VALUES (:id, '2026-09-23T00:00:00Z', '2026-01-01', '2026-09-23', 'failed')"
            ),
            {"id": original_run},
        )
    run(database, tmp_path)
    rows = [{"id": "W1", "publication_date": "2026-09-24", "is_xpac": False}]
    body = sample(tmp_path, rows)
    later = lambda: datetime(2026, 9, 24, tzinfo=UTC)
    # Missing legacy source metadata must require an explicit binding, not a new scope.
    assert not execute(catalog, body, 1, now=later).success
    assert execute(
        catalog, body, 1, now=later, overrides={"scan_id": original_run}
    ).success
    state = catalog.read_works()
    assert len(state["scopes"]) == len(state["scans"]) == 1
    assert state["scopes"][0]["definition"]["publication_through"] == "2026-09-23"
    assert state["scans"][0]["status"] == "complete"
    assert state["current"] == []
