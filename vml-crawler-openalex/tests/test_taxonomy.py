"""Acceptance tests through the Dagster ingestion boundary."""

import gzip
import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
from dagster import ExecuteInProcessResult
from pydantic import SecretStr

from src.common.settings import Settings
from src.ingestion.job import Runtime, taxonomy_job
from src.ingestion.store import Catalog

type Database = tuple[SecretStr, str]
type Taxonomy = dict[str, list[dict[str, Any]]]


def records() -> Taxonomy:
    """Return a complete small source hierarchy."""
    domain = {"id": "https://openalex.org/domains/1", "display_name": "Life Sciences"}
    field = {
        "id": "https://openalex.org/fields/11",
        "display_name": "Science",
        "domain": domain,
    }
    subfield = {
        "id": "https://openalex.org/subfields/1101",
        "display_name": "Biology",
        "domain": domain,
        "field": field,
    }
    topic = {
        "id": "https://openalex.org/T10001",
        "display_name": "Cells",
        "domain": domain,
        "field": field,
        "subfield": subfield,
        "description": "Cell study",
        "keywords": ["cells"],
        "works_count": 3,
        "updated_date": "2026-09-01",
        "unknown": None,
    }
    return {
        "domains": [domain],
        "fields": [field],
        "subfields": [subfield],
        "topics": [topic],
    }


def fixture_page(rows: list[dict[str, Any]], first: bool) -> dict[str, Any]:
    """Return a short data page or its empty terminal page."""
    return {
        "meta": {"count": len(rows), "next_cursor": "end" if first else None},
        "results": rows if first else [],
    }


def run(
    database: Database,
    tmp_path: Path,
    data: Taxonomy | None = None,
    failure: Callable[[str], None] = lambda stage: None,
    handler: Callable[[httpx.Request], httpx.Response] | None = None,
) -> tuple[ExecuteInProcessResult, Catalog, list[bytes]]:
    """Execute the public job with controlled source, clock, and failures."""
    settings = Settings(
        database_url=database[0], collection_schema=database[1], storage_root=tmp_path
    )
    bodies = []

    def respond(request: httpx.Request) -> httpx.Response:
        """Return and retain fixture response bytes."""
        rows = (data or records())[request.url.path.strip("/")]
        cursor = request.url.params["cursor"]
        body = json.dumps(
            fixture_page(rows, cursor == "*"),
            indent=2,
        ).encode()
        bodies.append(body)
        return httpx.Response(200, content=body)

    runtime = Runtime(
        settings,
        httpx.MockTransport(handler or respond),
        lambda: datetime(2026, 9, 23, tzinfo=UTC),
        failure,
    )
    result = taxonomy_job.execute_in_process(
        resources={"runtime": runtime}, raise_on_error=False
    )
    return result, Catalog(settings), bodies


def test_publishes_complete_bundle_and_original_bytes(
    database: Database, tmp_path: Path
) -> None:
    """Verify publishes complete bundle and original bytes."""
    result, catalog, bodies = run(database, tmp_path)
    assert result.success
    state = catalog.read()
    assert len(state["bundles"]) == 1
    assert len(state["observations"]) == 1
    assert state["attempts"][0]["status"] == "complete"
    bundle = state["bundles"][0]
    assert bundle["records"]["topics"][0]["keywords"] == ["cells"]
    assert (
        len(state["raw_objects"]) == 8
    )  # Short pages followed through terminal cursor.
    assert [
        gzip.decompress((tmp_path / row["path"]).read_bytes())
        for row in state["raw_objects"]
    ] == bodies


def test_replay_changes_and_reversions_keep_observation_history(
    database: Database, tmp_path: Path
) -> None:
    """Verify replay changes and reversions keep observation history."""
    first, catalog, _ = run(database, tmp_path)
    initial = first.output_for_node("collect_taxonomy")
    again, _, _ = run(database, tmp_path)
    assert again.output_for_node("collect_taxonomy") == initial
    changed = records()
    changed["topics"][0]["works_count"] = 4
    count_run, _, _ = run(database, tmp_path, changed)
    count_id = count_run.output_for_node("collect_taxonomy")
    assert count_id != initial
    state = catalog.read()
    assert len({bundle["classification_hash"] for bundle in state["bundles"]}) == 1
    changed["topics"][0]["description"] = "Changed classification"
    classification, _, _ = run(database, tmp_path, changed)
    assert classification.success
    state = catalog.read()
    assert len({bundle["classification_hash"] for bundle in state["bundles"]}) == 2
    reverted, _, _ = run(database, tmp_path)
    assert reverted.output_for_node("collect_taxonomy") == initial
    state = catalog.read()
    assert len(state["bundles"]) == 3
    assert len(state["observations"]) == 5
    assert state["current_bundle_id"] == initial


@pytest.mark.parametrize(
    "defect", ["duplicate", "wrong_type", "missing_parent", "inconsistent", "empty"]
)
def test_invalid_hierarchy_preserves_last_valid_bundle(
    database: Database, tmp_path: Path, defect: str
) -> None:
    """Verify invalid hierarchy preserves last valid bundle."""
    valid, catalog, _ = run(database, tmp_path)
    data = records()
    if defect == "duplicate":
        data["topics"] *= 2
    elif defect == "wrong_type":
        data["topics"][0]["id"] = "https://openalex.org/W10001"
    elif defect == "missing_parent":
        data["topics"][0]["subfield"] = {"id": "https://openalex.org/subfields/9999"}
    elif defect == "inconsistent":
        data["domains"].append(
            {"id": "https://openalex.org/domains/2", "display_name": "Other"}
        )
        data["topics"][0]["domain"] = data["domains"][1]
    else:
        data["topics"] = []
    failed, _, _ = run(database, tmp_path, data)
    assert not failed.success
    state = catalog.read()
    assert state["current_bundle_id"] == valid.output_for_node("collect_taxonomy")
    assert len(state["observations"]) == 1
    assert sorted(row["status"] for row in state["attempts"]) == ["complete", "failed"]


@pytest.mark.parametrize(
    "page",
    [
        {"results": [], "meta": {"count": 1}},
        {"results": [], "meta": {"count": 1, "next_cursor": None}},
        {"results": [], "meta": {"count": 0, "next_cursor": "*"}},
        {"results": [], "meta": {"count": 0, "next_cursor": 12}},
        {
            "results": [records()["domains"][0]],
            "meta": {"count": 2, "next_cursor": None},
        },
    ],
)
def test_malformed_pagination_never_publishes(
    database: Database, tmp_path: Path, page: dict[str, Any]
) -> None:
    """Verify malformed pagination never publishes."""
    result, catalog, _ = run(
        database, tmp_path, handler=lambda request: httpx.Response(200, json=page)
    )
    assert not result.success
    state = catalog.read()
    assert state["current_bundle_id"] is None
    assert state["attempts"][0]["status"] == "failed"
    assert len(state["raw_objects"]) == 1


@pytest.mark.parametrize(
    "stage",
    [
        "before_raw_publish",
        "after_raw_publish",
        "after_raw_commit",
        "before_bundle_commit",
        "after_bundle_commit",
    ],
)
def test_interrupted_persistence_reacquires_without_duplicate_bundles(
    database: Database, tmp_path: Path, stage: str
) -> None:
    """Verify interrupted persistence reacquires without duplicate bundles."""

    def interrupt(point: str) -> None:
        """Inject a persistence failure at the requested boundary."""
        if point == stage:
            raise OSError("injected failure")

    failed, catalog, _ = run(database, tmp_path, failure=interrupt)
    assert not failed.success
    state = catalog.read()
    assert len(state["bundles"]) == (1 if stage == "after_bundle_commit" else 0)
    recovered, _, _ = run(database, tmp_path)
    assert recovered.success
    state = catalog.read()
    assert len(state["bundles"]) == 1
    assert len(state["observations"]) == (2 if stage == "after_bundle_commit" else 1)
    for raw in state["raw_objects"]:
        assert gzip.decompress((tmp_path / raw["path"]).read_bytes())


def test_exact_numeric_identity_and_stable_record_order(
    database: Database, tmp_path: Path
) -> None:
    """Verify exact numeric identity and stable record order."""
    data = records()
    data["domains"].append(
        {"id": "https://openalex.org/domains/2", "display_name": "Other"}
    )
    first, _, _ = run(database, tmp_path, data)
    data["domains"].reverse()
    data["topics"][0]["works_count"] = 3.0
    second, _, _ = run(database, tmp_path, data)
    assert first.output_for_node("collect_taxonomy") == second.output_for_node(
        "collect_taxonomy"
    )

    def response(request: httpx.Request) -> httpx.Response:
        """Return the controlled source response."""
        rows = data[request.url.path.strip("/")]
        cursor = request.url.params["cursor"]
        body = json.dumps(fixture_page(rows, cursor == "*")).replace(
            "3.0", "3.0000000000000000001"
        )
        return httpx.Response(200, content=body.encode())

    precise, _, _ = run(database, tmp_path, handler=response)
    assert precise.success
    assert precise.output_for_node("collect_taxonomy") != first.output_for_node(
        "collect_taxonomy"
    )


@pytest.mark.parametrize(
    "stage",
    [
        "before_raw_publish",
        "after_raw_publish",
        "after_raw_commit",
        "before_bundle_commit",
        "after_bundle_commit",
    ],
)
def test_fresh_process_recovers_after_abrupt_exit(
    database: Database, tmp_path: Path, stage: str
) -> None:
    """Verify fresh process recovers after abrupt exit."""
    import os
    import subprocess
    import sys

    env = os.environ | {
        "TEST_DATABASE_URL": database[0].get_secret_value(),
        "TEST_SCHEMA": database[1],
        "TEST_STORAGE": str(tmp_path),
        "CRASH_STAGE": stage,
    }
    crashed = subprocess.run(
        [sys.executable, "-m", "tests.restart_worker"],
        env=env,
        capture_output=True,
        check=False,
    )
    assert crashed.returncode == 73
    catalog = Catalog(
        Settings(
            database_url=database[0],
            collection_schema=database[1],
            storage_root=tmp_path,
        )
    )
    state = catalog.read()
    assert len(state["bundles"]) == (1 if stage == "after_bundle_commit" else 0)
    env.pop("CRASH_STAGE")
    recovered = subprocess.run(
        [sys.executable, "-m", "tests.restart_worker"],
        env=env,
        capture_output=True,
        check=False,
    )
    assert recovered.returncode == 0
    state = catalog.read()
    assert len(state["bundles"]) == 1
    assert len(state["observations"]) == (2 if stage == "after_bundle_commit" else 1)
    assert len(state["attempts"]) == 2
    assert all((tmp_path / raw["path"]).is_file() for raw in state["raw_objects"])


def test_observation_intervals_checksums_and_credentials(
    database: Database, tmp_path: Path, capfd: pytest.CaptureFixture[str]
) -> None:
    """Verify observation intervals checksums and credentials."""
    import hashlib
    from datetime import timedelta
    from urllib.parse import parse_qs, urlparse

    tick = datetime(2026, 9, 23, tzinfo=UTC)

    def now() -> datetime:
        """Advance the deterministic observation clock."""
        nonlocal tick
        tick += timedelta(seconds=1)
        return tick

    def response(request: httpx.Request) -> httpx.Response:
        """Return the controlled source response."""
        assert request.url.params["api_key"] == "fixture-secret-key"
        rows = records()[request.url.path.strip("/")]
        first = request.url.params["cursor"] == "*"
        return httpx.Response(
            200,
            json=fixture_page(rows, first),
        )

    settings = Settings(
        database_url=database[0],
        collection_schema=database[1],
        storage_root=tmp_path,
        api_key="fixture-secret-key",
    )
    runtime = Runtime(settings, httpx.MockTransport(response), now)
    result = taxonomy_job.execute_in_process(resources={"runtime": runtime})
    assert result.success
    state = Catalog(settings).read()
    observation = state["observations"][0]
    assert observation["started_at"] < observation["ended_at"]
    for raw in state["raw_objects"]:
        compressed = (tmp_path / raw["path"]).read_bytes()
        body = gzip.decompress(compressed)
        assert hashlib.sha256(body).hexdigest() == raw["checksum"]
        assert hashlib.sha256(compressed).hexdigest() == raw["compressed_checksum"]
        assert raw["bytes"] == len(body)
        assert (
            observation["started_at"]
            <= raw["started_at"]
            < raw["ended_at"]
            <= observation["ended_at"]
        )
        assert set(parse_qs(urlparse(raw["source_locator"]).query)) == {
            "cursor",
            "per_page",
        }
    captured = capfd.readouterr()
    assert "fixture-secret-key" not in captured.out + captured.err + repr(state) + repr(
        settings
    )


def test_http_failure_preserves_evidence_without_leaking_key(
    database: Database, tmp_path: Path, capfd: pytest.CaptureFixture[str]
) -> None:
    """Verify http failure preserves evidence without leaking key."""
    settings = Settings(
        database_url=database[0],
        collection_schema=database[1],
        storage_root=tmp_path,
        api_key="fixture-secret-key",
    )
    runtime = Runtime(
        settings,
        httpx.MockTransport(
            lambda request: httpx.Response(401, content=b'{"error":"unauthorized"}')
        ),
    )
    result = taxonomy_job.execute_in_process(
        resources={"runtime": runtime}, raise_on_error=False
    )
    assert not result.success
    state = Catalog(settings).read()
    assert state["attempts"][0]["error"] == "HTTPStatusError"
    assert len(state["raw_objects"]) == 1
    assert state["current_bundle_id"] is None
    captured = capfd.readouterr()
    assert "fixture-secret-key" not in captured.out + captured.err + repr(state)


def test_dagster_postgres_instance_runs_job(database: Database, tmp_path: Path) -> None:
    """Verify dagster postgres instance runs job."""
    from dagster import DagsterInstance
    from sqlalchemy.engine import make_url

    settings = Settings(
        database_url=database[0],
        collection_schema=database[1],
        storage_root=tmp_path / "raw",
    )
    catalog = Catalog(settings)
    catalog.migrate()
    url = (
        make_url(database[0].get_secret_value())
        .set(drivername="postgresql")
        .update_query_dict({"options": "-csearch_path=" + database[1]})
    )
    instance_dir = tmp_path / "dagster"
    instance_dir.mkdir()
    (instance_dir / "dagster.yaml").write_text(
        "storage:\n  postgres:\n    postgres_url: "
        + json.dumps(url.render_as_string(hide_password=False))
        + "\ntelemetry:\n  enabled: false\n"
    )

    def response(request: httpx.Request) -> httpx.Response:
        """Return the controlled source response."""
        rows = records()[request.url.path.strip("/")]
        first = request.url.params["cursor"] == "*"
        return httpx.Response(
            200,
            json=fixture_page(rows, first),
        )

    with DagsterInstance.from_config(str(instance_dir)) as instance:
        result = taxonomy_job.execute_in_process(
            instance=instance,
            resources={"runtime": Runtime(settings, httpx.MockTransport(response))},
        )
        assert result.success
        persisted_run = instance.get_run_by_id(result.run_id)
        assert persisted_run is not None and persisted_run.is_success
    assert catalog.read()["current_bundle_id"] == result.output_for_node(
        "collect_taxonomy"
    )


@pytest.mark.parametrize("legacy", [False, True])
def test_job_tracks_alembic_revision_and_preserves_existing_data(
    database: Database,
    tmp_path: Path,
    legacy: bool,
) -> None:
    """Verify migration tracking and adoption through repeated job execution."""
    import sqlalchemy as sa

    first, catalog, _ = run(database, tmp_path)
    assert first.success
    name = "alembic_" + catalog.settings.alembic_name
    if legacy:
        from alembic import command

        from src.migrations import configuration

        command.downgrade(configuration(catalog.settings), "0001")
    with catalog.engine.begin() as connection:
        version = sa.Table(
            name, sa.MetaData(), schema="migrations", autoload_with=connection
        )
        assert connection.scalar(sa.select(version.c.version_num)) == (
            "0001" if legacy else "0008"
        )
        if legacy:
            # Reproduce the previous runner's committed marker without changing data.
            version.drop(connection)
            marker = sa.Table(
                "schema_migrations",
                sa.MetaData(),
                sa.Column("version", sa.Text(), primary_key=True),
                schema=database[1],
            )
            marker.create(connection)
            connection.execute(marker.insert().values(version="001_taxonomy.sql"))
    second, _, _ = run(database, tmp_path)
    assert second.success
    assert second.output_for_node("collect_taxonomy") == first.output_for_node(
        "collect_taxonomy"
    )
    assert len(catalog.read()["observations"]) == 2
    with catalog.engine.connect() as connection:
        version = sa.Table(
            name, sa.MetaData(), schema="migrations", autoload_with=connection
        )
        assert connection.scalar(sa.select(version.c.version_num)) == "0008"
        assert not sa.inspect(connection).has_table(
            "schema_migrations", schema=database[1]
        )


def test_alembic_generates_sequential_revisions_and_round_trips(
    database: Database,
    tmp_path: Path,
) -> None:
    """Verify operator revision generation, upgrade, and downgrade commands."""
    import shutil

    import sqlalchemy as sa
    from alembic import command

    from src.migrations import configuration

    settings = Settings(database_url=database[0], collection_schema=database[1])
    config = configuration(settings)
    scripts = tmp_path / "migrations"
    shutil.copytree(Path("src/migrations"), scripts)
    config.set_main_option("script_location", str(scripts))
    revision = command.revision(config, message="add example")
    assert revision is not None and not isinstance(revision, list)
    assert revision.revision == "0009"
    assert Path(revision.path).name == "0009_add_example.py"
    command.upgrade(config, "head")
    catalog = Catalog(settings)
    with catalog.engine.connect() as connection:
        version = sa.Table(
            "alembic_" + settings.alembic_name,
            sa.MetaData(),
            schema="migrations",
            autoload_with=connection,
        )
        assert connection.scalar(sa.select(version.c.version_num)) == "0009"
    command.downgrade(config, "base")
    with catalog.engine.connect() as connection:
        assert sa.inspect(connection).get_table_names(schema=database[1]) == []
    command.upgrade(config, "head")
    assert catalog.read()["current_bundle_id"] is None


def test_nonempty_terminal_pages_publish_complete_taxonomy(
    database: Database,
    tmp_path: Path,
) -> None:
    """Accept the live taxonomy contract: null cursor on the last data page."""

    def response(request: httpx.Request) -> httpx.Response:
        """Reproduce terminal metadata observed on the live domains endpoint."""
        rows = records()[request.url.path.strip("/")]
        return httpx.Response(
            200,
            json={"meta": {"count": len(rows), "next_cursor": None}, "results": rows},
        )

    result, catalog, _ = run(database, tmp_path, handler=response)
    assert result.success
    state = catalog.read()
    assert len(state["bundles"]) == 1
    assert len(state["raw_objects"]) == 4
