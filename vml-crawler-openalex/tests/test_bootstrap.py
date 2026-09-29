"""Full snapshot behavior through jobs, filesystem, and PostgreSQL."""

import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import httpx
from dagster import ExecuteInProcessResult

from src.ingestion.job import Runtime
from src.ingestion.works_store import WorksCatalog
from tests.test_snapshot import sample
from tests.test_taxonomy import Database, run


def fixture(tmp_path: Path) -> tuple[bytes, dict[str, bytes]]:
    """Use the official entity manifest shape and a multi-chunk works file."""
    body = sample(
        tmp_path,
        [
            {
                "id": f"https://openalex.org/W{i}",
                "publication_date": "2026-01-01",
                "is_xpac": False,
            }
            for i in range(5)
        ],
    )
    url = "s3://openalex/data/parquet/works/updated_date=2026-09-01/part_0000.parquet"
    manifest = {
        "date": "2026-09-23",
        "format": "parquet",
        "entity": "works",
        "record_count": 5,
        "content_length": len(body),
        "files": [
            {
                "url": url,
                "meta": {
                    "content_length": len(body),
                    "record_count": 5,
                    "sha256": hashlib.sha256(body).hexdigest(),
                },
            }
        ],
    }
    return json.dumps(manifest).encode(), {
        url.replace("s3://openalex/", "https://openalex.s3.amazonaws.com/"): body
    }


def execute(
    catalog: WorksCatalog,
    manifest: bytes,
    bodies: dict[str, bytes],
    calls: list[str],
    failure: Callable[[str], None] = lambda stage: None,
    chunk_rows: int = 2,
) -> ExecuteInProcessResult:
    """Run discovery using fixture network responses."""
    from src.ingestion.bootstrap import bootstrap_job

    def response(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(
            200,
            content=manifest
            if str(request.url).endswith("manifest.json")
            else bodies[str(request.url)],
        )

    return bootstrap_job.execute_in_process(
        resources={
            "runtime": Runtime(
                catalog.settings,
                httpx.MockTransport(response),
                lambda: datetime(2026, 9, 23, tzinfo=UTC),
                failure,
            )
        },
        run_config={
            "ops": {"collect_bootstrap": {"config": {"chunk_rows": chunk_rows}}}
        },
        raise_on_error=False,
    )


def test_complete_inventory_chunks_and_noop(database: Database, tmp_path: Path) -> None:
    """Completion requires all chunks; replay performs only manifest checks."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    manifest, bodies = fixture(tmp_path)
    calls = []
    assert execute(catalog, manifest, bodies, calls).success
    state = catalog.read_works()
    assert len(state["current"]) == len(state["versions"]) == 5
    assert len(state["batches"]) == 3
    assert state["file_coverage"][0]["processed"]
    release = state["releases"][0]
    assert release["acquisition_status"] == "complete"
    assert state["baselines"][0]["selection_status"] == "complete"
    assert (tmp_path / release["manifest_path"]).read_bytes() == manifest
    calls.clear()
    assert execute(catalog, manifest, bodies, calls).success
    assert all(url.endswith("manifest.json") for url in calls)
    again = catalog.read_works()
    assert len(again["batches"]) == 3
    assert len(again["observations"]) == 5


def test_changed_manifest_reconciles_new_inventory(
    database: Database, tmp_path: Path
) -> None:
    """Changed inventory cannot mark the old release acquired or selected."""
    from src.ingestion.bootstrap import bootstrap_job

    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    manifest, bodies = fixture(tmp_path)
    changed = json.loads(manifest)
    changed["date"] = "2026-09-24"
    changed = json.dumps(changed).encode()
    checks = []

    def response(request: httpx.Request) -> httpx.Response:
        if str(request.url).endswith("manifest.json"):
            checks.append(1)
            return httpx.Response(
                200, content=manifest if len(checks) == 1 else changed
            )
        return httpx.Response(200, content=bodies[str(request.url)])

    result = bootstrap_job.execute_in_process(
        resources={"runtime": Runtime(catalog.settings, httpx.MockTransport(response))},
        raise_on_error=False,
    )
    assert not result.success
    state = catalog.read_works()
    assert state["releases"][0]["acquisition_status"] == "superseded"
    assert not state["current"]
    assert execute(catalog, changed, bodies, []).success
    state = catalog.read_works()
    assert len(state["current"]) == 5
    assert {r["acquisition_status"] for r in state["releases"]} == {
        "complete",
        "superseded",
    }


def test_missing_taxonomy_keeps_acquisition_complete(
    database: Database, tmp_path: Path
) -> None:
    """Downloads can complete before selected publication is possible."""
    from src.common.settings import Settings

    catalog = WorksCatalog(
        Settings(
            database_url=database[0],
            collection_schema=database[1],
            storage_root=tmp_path,
        )
    )
    manifest, bodies = fixture(tmp_path)
    assert execute(catalog, manifest, bodies, []).success
    state = catalog.read_works()
    assert state["releases"][0]["acquisition_status"] == "complete"
    assert state["baselines"][0]["selection_status"] == "waiting_taxonomy"
    assert not state["current"]
    run(database, tmp_path)
    calls = []
    assert execute(catalog, manifest, bodies, calls).success
    assert all(url.endswith("manifest.json") for url in calls)
    assert catalog.read_works()["baselines"][0]["selection_status"] == "complete"


import pytest


@pytest.mark.parametrize(
    "invalid", ["truncated", "unreadable", "row_count", "checksum"]
)
def test_invalid_source_never_completes(
    database: Database, tmp_path: Path, invalid: str
) -> None:
    """Size, checksums, full Parquet decoding and row totals guard completion."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    manifest, bodies = fixture(tmp_path)
    broken = json.loads(manifest)
    url = next(iter(bodies))
    if invalid == "truncated":
        bodies[url] = bodies[url][:-20]
    elif invalid == "unreadable":
        bodies[url] = b"x" * len(bodies[url])
        broken["files"][0]["meta"].pop("sha256")
    elif invalid == "row_count":
        broken["record_count"] = broken["files"][0]["meta"]["record_count"] = 6
    else:
        broken["files"][0]["meta"]["sha256"] = "0" * 64
    assert not execute(catalog, json.dumps(broken).encode(), bodies, []).success
    state = catalog.read_works()
    assert state["releases"][0]["acquisition_status"] != "complete"
    assert state["baselines"][0]["selection_status"] != "complete"
    assert not state["chunks"] and not state["current"]


@pytest.mark.parametrize(
    "stage",
    [
        "before_bootstrap_raw_publish",
        "after_bootstrap_raw_link",
        "after_bootstrap_raw_publish",
        "after_bootstrap_raw_commit",
        "after_bootstrap_output_publish",
        "before_bootstrap_chunk_commit",
        "after_bootstrap_chunk_commit",
    ],
)
def test_fresh_process_resume(database: Database, tmp_path: Path, stage: str) -> None:
    """Abrupt exits never skip input or duplicate logical chunk publications."""
    import os
    import subprocess
    import sys

    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    manifest, bodies = fixture(tmp_path)
    (tmp_path / "source_manifest.json").write_bytes(manifest)
    (tmp_path / "source_files.json").write_text(
        json.dumps({url: body.hex() for url, body in bodies.items()})
    )
    env = os.environ | {
        "TEST_DATABASE_URL": database[0].get_secret_value(),
        "TEST_SCHEMA": database[1],
        "TEST_STORAGE": str(tmp_path),
        "CRASH_STAGE": stage,
    }
    command = [sys.executable, "-m", "tests.bootstrap_worker"]
    first = subprocess.run(command, env=env, capture_output=True, check=False)
    assert first.returncode == 73, first.stderr.decode()[-2000:]
    partial = catalog.read_works()
    assert partial["baselines"][0]["selection_status"] != "complete"
    if stage == "after_bootstrap_chunk_commit":
        assert len(partial["chunks"]) == 1
        assert not partial["file_coverage"][0]["processed"]
    resumed = subprocess.run(
        command, env=env | {"CRASH_STAGE": ""}, capture_output=True, check=False
    )
    assert resumed.returncode == 0, resumed.stderr.decode()[-2000:]
    state = catalog.read_works()
    assert state["baselines"][0]["selection_status"] == "complete"
    assert (
        len(state["current"])
        == len(state["versions"])
        == len(state["observations"])
        == 5
    )
    assert len(state["chunks"]) == len(state["batches"]) == 3
    if stage not in {"before_bootstrap_raw_publish", "after_bootstrap_raw_link"}:
        calls = json.loads((tmp_path / "resume_calls.json").read_text())
        assert all(url.endswith("manifest.json") for url in calls)


def test_expanded_chunks_split_without_exceeding_memory_budget(
    database: Database, tmp_path: Path
) -> None:
    """Highly compressed records remain processable with bounded expanded chunks."""
    from src.ingestion.bootstrap import bootstrap_job

    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    manifest, _bodies = fixture(tmp_path)
    (tmp_path / "fixture.parquet").unlink()
    body = sample(
        tmp_path,
        [
            {
                "id": f"https://openalex.org/W{i}",
                "publication_date": "2026-01-01",
                "is_xpac": False,
                "title": "a" * 900000,
            }
            for i in range(40)
        ],
    )
    changed = json.loads(manifest)
    changed["record_count"] = changed["files"][0]["meta"]["record_count"] = 40
    changed["content_length"] = changed["files"][0]["meta"]["content_length"] = len(
        body
    )
    changed["files"][0]["meta"]["sha256"] = hashlib.sha256(body).hexdigest()
    payload = json.dumps(changed).encode()
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200, content=payload if str(request.url).endswith("manifest.json") else body
        )
    )
    result = bootstrap_job.execute_in_process(
        resources={"runtime": Runtime(catalog.settings, transport)},
        raise_on_error=False,
    )
    assert result.success
    state = catalog.read_works()
    assert len(state["current"]) == 40
    assert len(state["chunks"]) == 2


def test_database_failure_preserves_committed_chunks(
    database: Database, tmp_path: Path
) -> None:
    """A persistence failure rolls back its whole chunk; a retry fills the gap."""
    from sqlalchemy.exc import OperationalError

    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    manifest, bodies = fixture(tmp_path)
    committed = 0

    def fail(stage: str) -> None:
        nonlocal committed
        if stage == "after_bootstrap_chunk_commit":
            committed += 1
        if stage == "before_bootstrap_chunk_commit" and committed == 1:
            raise OperationalError("injected database unavailable", {}, Exception())

    assert not execute(catalog, manifest, bodies, [], fail).success
    state = catalog.read_works()
    assert len(state["chunks"]) == 1 and len(state["current"]) == 2
    assert state["releases"][0]["acquisition_status"] == "complete"
    assert state["baselines"][0]["selection_status"] == "failed"
    assert execute(catalog, manifest, bodies, []).success
    state = catalog.read_works()
    assert len(state["chunks"]) == 3 and len(state["current"]) == 5
    assert len(state["observations"]) == 5


def test_multiple_files_resume_in_fresh_process(
    database: Database, tmp_path: Path
) -> None:
    """Resume only the missing files of a partially acquired release."""
    import os
    import subprocess
    import sys

    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    manifest, bodies = fixture(tmp_path)
    document = json.loads(manifest)
    second = json.loads(json.dumps(document["files"][0]))
    second["url"] = second["url"].replace("part_0000", "part_0001")
    document["files"].append(second)
    document["record_count"] *= 2
    document["content_length"] *= 2
    second_url = second["url"].replace(
        "s3://openalex/", "https://openalex.s3.amazonaws.com/"
    )
    bodies[second_url] = next(iter(bodies.values()))
    (tmp_path / "source_manifest.json").write_text(json.dumps(document))
    (tmp_path / "source_files.json").write_text(
        json.dumps({url: body.hex() for url, body in bodies.items()})
    )
    env = os.environ | {
        "TEST_DATABASE_URL": database[0].get_secret_value(),
        "TEST_SCHEMA": database[1],
        "TEST_STORAGE": str(tmp_path),
        "CRASH_STAGE": "after_bootstrap_raw_commit",
    }
    command = [sys.executable, "-m", "tests.bootstrap_worker"]
    first = subprocess.run(command, env=env, capture_output=True, check=False)
    assert first.returncode == 73
    state = catalog.read_works()
    assert state["releases"][0]["acquisition_status"] == "partial"
    assert sum(row["source_id"] is not None for row in state["release_files"]) == 1
    second_run = subprocess.run(
        command, env=env | {"CRASH_STAGE": ""}, capture_output=True, check=False
    )
    assert second_run.returncode == 0, second_run.stderr.decode()[-2000:]
    calls = json.loads((tmp_path / "resume_calls.json").read_text())
    assert [url for url in calls if not url.endswith("manifest.json")] == [second_url]
    state = catalog.read_works()
    assert len(state["chunks"]) == 6
    assert len(state["versions"]) == len(state["processing"]) == 5
    assert len(state["observations"]) == 10


def test_large_file_streams_before_first_chunk(
    database: Database, tmp_path: Path
) -> None:
    """Publishing the first chunk must not materialize a 470 MiB source result."""
    import os
    import subprocess
    import sys

    import duckdb

    run(database, tmp_path)
    manifest, _bodies = fixture(tmp_path)
    output = tmp_path / "large.parquet"
    with duckdb.connect() as duck:
        duck.execute(
            "COPY (SELECT 'https://openalex.org/W' || i AS id, '2026-01-01' AS publication_date, true AS is_xpac, repeat('a',16384) AS title FROM range(30000) t(i)) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
            [str(output)],
        )
    body = output.read_bytes()
    document = json.loads(manifest)
    document["record_count"] = document["files"][0]["meta"]["record_count"] = 30000
    document["content_length"] = document["files"][0]["meta"]["content_length"] = len(
        body
    )
    document["files"][0]["meta"]["sha256"] = hashlib.sha256(body).hexdigest()
    url = document["files"][0]["url"].replace(
        "s3://openalex/", "https://openalex.s3.amazonaws.com/"
    )
    (tmp_path / "source_manifest.json").write_text(json.dumps(document))
    (tmp_path / "source_files.json").write_text(json.dumps({url: body.hex()}))
    env = os.environ | {
        "TEST_DATABASE_URL": database[0].get_secret_value(),
        "TEST_SCHEMA": database[1],
        "TEST_STORAGE": str(tmp_path),
        "CRASH_STAGE": "after_bootstrap_chunk_commit",
    }
    result = subprocess.run(
        [sys.executable, "-m", "tests.bootstrap_worker"],
        env=env,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 73, (
        result.stdout.decode()[-1000:] + result.stderr.decode()[-2000:]
    )
    print("STDOUT:", result.stdout.decode())
    assert int((tmp_path / "peak_rss_kib").read_text()) < 700 * 1024


def test_transient_connection_error_retried(database: Database, tmp_path: Path) -> None:
    """Transient network errors during file download are retried and succeed."""
    from src.ingestion.bootstrap import bootstrap_job

    _, taxonomy, _ = run(database, tmp_path)
    settings = taxonomy.settings.model_copy(
        update={"download_max_retries": 3, "download_retry_seconds": 0}
    )
    catalog = WorksCatalog(settings)
    manifest, bodies = fixture(tmp_path)
    file_attempts = 0

    def response(request: httpx.Request) -> httpx.Response:
        nonlocal file_attempts
        url = str(request.url)
        if url.endswith("manifest.json"):
            return httpx.Response(200, content=manifest)
        file_attempts += 1
        if file_attempts == 1:
            raise httpx.ConnectError("Connection reset by peer", request=request)
        return httpx.Response(200, content=bodies[url])

    result = bootstrap_job.execute_in_process(
        resources={
            "runtime": Runtime(
                settings,
                httpx.MockTransport(response),
                lambda: datetime(2026, 9, 23, tzinfo=UTC),
            )
        },
        run_config={"ops": {"collect_bootstrap": {"config": {"chunk_rows": 2}}}},
        raise_on_error=False,
    )
    assert result.success
    assert file_attempts == 2
    state = catalog.read_works()
    assert state["releases"][0]["acquisition_status"] == "complete"
    assert state["baselines"][0]["selection_status"] == "complete"
    assert len(state["current"]) == 5
    staging = tmp_path / "staging"
    assert not list(staging.glob("*.parquet"))


def test_transient_manifest_fetch_retried(database: Database, tmp_path: Path) -> None:
    """Transient network errors during manifest retrieval are retried and succeed."""
    from src.ingestion.bootstrap import bootstrap_job

    _, taxonomy, _ = run(database, tmp_path)
    settings = taxonomy.settings.model_copy(
        update={"download_max_retries": 3, "download_retry_seconds": 0}
    )
    catalog = WorksCatalog(settings)
    manifest, bodies = fixture(tmp_path)
    manifest_attempts = 0

    def response(request: httpx.Request) -> httpx.Response:
        nonlocal manifest_attempts
        url = str(request.url)
        if url.endswith("manifest.json"):
            manifest_attempts += 1
            if manifest_attempts == 1:
                raise httpx.ConnectError("Network unreachable", request=request)
            return httpx.Response(200, content=manifest)
        return httpx.Response(200, content=bodies[url])

    result = bootstrap_job.execute_in_process(
        resources={
            "runtime": Runtime(
                settings,
                httpx.MockTransport(response),
                lambda: datetime(2026, 9, 23, tzinfo=UTC),
            )
        },
        run_config={"ops": {"collect_bootstrap": {"config": {"chunk_rows": 2}}}},
        raise_on_error=False,
    )
    assert result.success
    assert manifest_attempts >= 2
    state = catalog.read_works()
    assert state["releases"][0]["acquisition_status"] == "complete"


def test_bootstrap_emits_asset_observations(database: Database, tmp_path: Path) -> None:
    """Bootstrap job emits AssetObservation events for download and selection progress."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    manifest, bodies = fixture(tmp_path)
    calls = []
    result = execute(catalog, manifest, bodies, calls)
    assert result.success

    observations = [
        event.event_specific_data.asset_observation
        for event in result.all_events
        if event.is_step_event and event.event_type_value == "ASSET_OBSERVATION"
    ]
    assert len(observations) >= 2

    download_obs = [
        obs
        for obs in observations
        if obs.asset_key.path == ["openalex", "bootstrap_download"]
    ]
    assert len(download_obs) >= 1
    assert download_obs[-1].metadata["phase"].value == "acquisition"
    assert download_obs[-1].metadata["total_files"].value == 1
    assert download_obs[-1].metadata["acquired_files"].value == 1
    assert download_obs[-1].metadata["pct_downloaded"].value == 100.0
    assert "acquired_gib" in download_obs[-1].metadata
    assert isinstance(download_obs[-1].metadata["acquired_gib"].value, float)
    assert "total_gib" in download_obs[-1].metadata
    assert isinstance(download_obs[-1].metadata["total_gib"].value, float)
    assert "acquired_bytes_str" in download_obs[-1].metadata
    assert isinstance(download_obs[-1].metadata["acquired_bytes_str"].value, str)
    assert "total_bytes_str" in download_obs[-1].metadata
    assert isinstance(download_obs[-1].metadata["total_bytes_str"].value, str)
    assert "acquired_bytes" not in download_obs[-1].metadata
    assert "total_bytes" not in download_obs[-1].metadata

    selection_obs = [
        obs
        for obs in observations
        if obs.asset_key.path == ["openalex", "bootstrap_selection"]
    ]
    assert len(selection_obs) >= 1
    assert selection_obs[-1].metadata["phase"].value == "selection"
    assert selection_obs[-1].metadata["processed_rows"].value == 5
    assert selection_obs[-1].metadata["total_rows"].value == 5
    assert selection_obs[-1].metadata["pct_processed"].value == 100.0
