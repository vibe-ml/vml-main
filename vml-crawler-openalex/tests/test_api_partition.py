"""API partition acceptance through ingestion jobs."""

import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from dagster import ExecuteInProcessResult

from src.ingestion.job import Runtime
from src.ingestion.refresh import refresh_job
from src.ingestion.works_store import WorksCatalog
from tests.test_taxonomy import Database, run


def execute(
    catalog: WorksCatalog,
    responses: list[bytes],
    failure: Callable[[str], None] = lambda stage: None,
    now: Callable[[], datetime] | None = None,
    overrides: Mapping[str, object] | None = None,
) -> ExecuteInProcessResult:
    """Run the ingestion job with a fixture transport and frozen clock."""
    config = {
        "publication_date": "2026-01-01",
    } | dict(overrides or {})

    response_iterator = iter(responses)

    def handle_request(request: httpx.Request) -> httpx.Response:
        try:
            return httpx.Response(200, content=next(response_iterator))
        except StopIteration:
            return httpx.Response(404, content=b"{}")

    runtime = Runtime(
        catalog.settings,
        httpx.MockTransport(handle_request),
        now or (lambda: datetime(2026, 9, 23, tzinfo=UTC)),
        failure,
    )
    return refresh_job.execute_in_process(
        resources={"runtime": runtime},
        run_config={"ops": {"refresh_partition": {"config": config}}},
        raise_on_error=False,
    )


def test_empty_scan_coverage(database: Database, tmp_path: Path) -> None:
    """An exhausted empty query records a completed coverage check."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings.model_copy(update={"api_key": None}))

    # 1. Create a scan for the test to use
    run_id = "11111111-1111-1111-1111-111111111111"
    scan_id, _ = catalog.start_scan(
        {"scan_id": None}, run_id, datetime(2026, 9, 23, tzinfo=UTC)
    )

    empty_page = json.dumps(
        {"meta": {"count": 0, "next_cursor": None}, "results": []}
    ).encode()

    result = execute(catalog, [empty_page], overrides={"scan_id": scan_id})
    assert result.success

    # Verify partition state
    data = catalog.read_works()
    partitions = data["partitions"]
    assert len(partitions) == 1
    assert partitions[0]["status"] == "complete"
    assert partitions[0]["record_count"] == 0


def test_scope_selection_and_short_pages(database: Database, tmp_path: Path) -> None:
    """Apply frozen filters, retain raw pages, and publish only scope members."""
    import gzip

    import duckdb

    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings.model_copy(update={"api_key": None}))
    scan_id, _ = catalog.start_scan(
        {}, "22222222-2222-2222-2222-222222222222", datetime(2026, 9, 23, tzinfo=UTC)
    )
    good = {
        "id": "W1",
        "publication_date": "2026-01-01",
        "is_xpac": False,
        "unknown": {"x": None},
    }
    pages = [
        json.dumps(
            {"meta": {"count": 2, "next_cursor": "a+/="}, "results": [good]}
        ).encode(),
        json.dumps(
            {
                "meta": {"count": 2, "next_cursor": None},
                "results": [good | {"id": "W2", "is_xpac": True}],
            }
        ).encode(),
    ]
    requests = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=pages[len(requests) - 1])

    result = refresh_job.execute_in_process(
        resources={
            "runtime": Runtime(
                catalog.settings,
                httpx.MockTransport(handle),
                lambda: datetime(2026, 9, 23, tzinfo=UTC),
            )
        },
        run_config={
            "ops": {
                "refresh_partition": {
                    "config": {"scan_id": scan_id, "publication_date": "2026-01-01"}
                }
            }
        },
        raise_on_error=False,
    )
    assert result.success
    assert requests[0].url.params["corpus"] == "core"
    assert requests[1].url.params["cursor"] == "a+/="
    state = catalog.read_works()
    assert [v["entity_id"] for v in state["versions"]] == ["W1"]
    assert len(state["observations"]) == 2
    assert {
        gzip.decompress((tmp_path / s["path"]).read_bytes()) for s in state["sources"]
    } == set(pages)
    manifest = next(b["manifest"] for b in state["batches"] if b["manifest"]["count"])
    with duckdb.connect() as db:
        assert (
            db.execute(
                "SELECT count(*) FROM read_parquet(?)",
                [str(tmp_path / manifest["path"])],
            ).fetchall()[0][0]
            == 1
        )


def test_budget_preserves_cursor_and_frozen_request(
    database: Database, tmp_path: Path
) -> None:
    """A later run resumes the same cursor after a persisted allowance reset."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(
        taxonomy.settings.model_copy(update={"api_request_limit": 1, "api_key": None})
    )
    scan, _ = catalog.start_scan(
        {}, "33333333-3333-3333-3333-333333333333", datetime(2026, 9, 23, tzinfo=UTC)
    )
    row = {"id": "W1", "publication_date": "2026-01-01", "is_xpac": False}
    first = json.dumps(
        {"meta": {"count": 1, "next_cursor": "next"}, "results": [row]}
    ).encode()
    result = execute(catalog, [first], overrides={"scan_id": scan})
    assert result.success
    state = catalog.read_works()
    assert state["partitions"][0]["status"] == "paused_budget"
    assert state["partitions"][0]["next_cursor"] == "next"
    assert execute(catalog, [], overrides={"scan_id": scan}).success
    requests = []
    catalog = WorksCatalog(catalog.settings.model_copy(update={"page_size": 5}))

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200, json={"meta": {"count": 1, "next_cursor": None}, "results": []}
        )

    result = refresh_job.execute_in_process(
        resources={
            "runtime": Runtime(
                catalog.settings,
                httpx.MockTransport(handle),
                lambda: datetime(2026, 9, 24, tzinfo=UTC),
            )
        },
        run_config={
            "ops": {
                "refresh_partition": {
                    "config": {"scan_id": scan, "publication_date": "2026-01-01"}
                }
            }
        },
        raise_on_error=False,
    )
    assert result.success
    assert requests[0].url.params["cursor"] == "next"
    assert requests[0].url.params["per_page"] == "100"


def test_expired_worker_cannot_commit_or_steal_live_lease(
    database: Database, tmp_path: Path
) -> None:
    """Overlapping jobs cannot commit a stale page or steal a live lease."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(
        taxonomy.settings.model_copy(update={"api_key": None, "api_lease_seconds": 2})
    )
    scan, _ = catalog.start_scan(
        {}, "44444444-4444-4444-4444-444444444444", datetime(2026, 9, 23, tzinfo=UTC)
    )
    current = [datetime(2026, 9, 23, tzinfo=UTC)]
    page = json.dumps(
        {"meta": {"count": 0, "next_cursor": None}, "results": []}
    ).encode()

    def handle(request: httpx.Request) -> httpx.Response:
        nested = execute(
            catalog, [], now=lambda: current[0], overrides={"scan_id": scan}
        )
        assert (
            nested.success and nested.output_for_node("refresh_partition") == "leased"
        )
        # A second contender must still be denied after the first returned.
        nested = execute(
            catalog, [], now=lambda: current[0], overrides={"scan_id": scan}
        )
        assert (
            nested.success and nested.output_for_node("refresh_partition") == "leased"
        )
        current[0] = datetime(2026, 9, 23, 0, 0, 3, tzinfo=UTC)
        nested = execute(
            catalog, [page], now=lambda: current[0], overrides={"scan_id": scan}
        )
        assert nested.success
        return httpx.Response(200, content=page)

    result = refresh_job.execute_in_process(
        resources={
            "runtime": Runtime(
                catalog.settings, httpx.MockTransport(handle), lambda: current[0]
            )
        },
        run_config={
            "ops": {
                "refresh_partition": {
                    "config": {"scan_id": scan, "publication_date": "2026-01-01"}
                }
            }
        },
        raise_on_error=False,
    )
    assert not result.success
    assert catalog.read_works()["partitions"][0]["status"] == "complete"


def test_invalid_terminal_page_never_completes(
    database: Database, tmp_path: Path
) -> None:
    """A malformed terminal page retains its raw evidence without checkpointing."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings.model_copy(update={"api_key": None}))
    scan, _ = catalog.start_scan(
        {}, "55555555-5555-5555-5555-555555555555", datetime(2026, 9, 23, tzinfo=UTC)
    )
    malformed = json.dumps(
        {"meta": {"count": -1, "next_cursor": None}, "results": []}
    ).encode()
    assert not execute(catalog, [malformed], overrides={"scan_id": scan}).success
    state = catalog.read_works()
    assert state["partitions"][0]["status"] != "complete"
    assert state["partitions"][0]["next_cursor"] is None
    assert len(state["sources"]) == 1
    assert not state["observations"]


def test_transient_retry_and_cursor_rejection(
    database: Database, tmp_path: Path
) -> None:
    """Retry transient failures; rejected cursors restart only this partition."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(
        taxonomy.settings.model_copy(update={"api_key": None, "api_retry_seconds": 0})
    )
    scan, _ = catalog.start_scan(
        {}, "66666666-6666-6666-6666-666666666666", datetime(2026, 9, 23, tzinfo=UTC)
    )
    row = {"id": "W1", "publication_date": "2026-01-01", "is_xpac": False}
    replies = iter(
        [
            httpx.Response(503, content=b"busy"),
            httpx.Response(
                200,
                json={"meta": {"count": 1, "next_cursor": "expired"}, "results": [row]},
            ),
            httpx.Response(400, json={"error": "Invalid cursor"}),
            httpx.Response(
                200, json={"meta": {"count": 1, "next_cursor": None}, "results": [row]}
            ),
        ]
    )
    cursors = []

    def handle(request: httpx.Request) -> httpx.Response:
        cursors.append(request.url.params["cursor"])
        return next(replies)

    result = refresh_job.execute_in_process(
        resources={
            "runtime": Runtime(
                catalog.settings,
                httpx.MockTransport(handle),
                lambda: datetime(2026, 9, 23, tzinfo=UTC),
            )
        },
        run_config={
            "ops": {
                "refresh_partition": {
                    "config": {"scan_id": scan, "publication_date": "2026-01-01"}
                }
            }
        },
        raise_on_error=False,
    )
    assert result.success
    assert cursors == ["*", "*", "expired", "*"]
    state = catalog.read_works()
    assert state["partitions"][0]["generation"] == 2
    assert len(state["versions"]) == len(state["processing"]) == 1
    assert len(state["observations"]) == 2


def test_reset_header_blocks_early_requests(database: Database, tmp_path: Path) -> None:
    """A supplied reset deadline survives a new runtime before allowance returns."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(
        taxonomy.settings.model_copy(update={"api_key": None, "api_retry_seconds": 0})
    )
    scan, _ = catalog.start_scan(
        {}, "77777777-7777-7777-7777-777777777777", datetime(2026, 9, 23, tzinfo=UTC)
    )

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "3600"},
            content=b"limited",
        )

    result = refresh_job.execute_in_process(
        resources={
            "runtime": Runtime(
                catalog.settings,
                httpx.MockTransport(handle),
                lambda: datetime(2026, 9, 23, tzinfo=UTC),
            )
        },
        run_config={
            "ops": {
                "refresh_partition": {
                    "config": {"scan_id": scan, "publication_date": "2026-01-01"}
                }
            }
        },
        raise_on_error=False,
    )
    assert (
        result.success
        and result.output_for_node("refresh_partition") == "paused_budget"
    )
    assert execute(catalog, [], overrides={"scan_id": scan}).success
    assert catalog.read_works()["partitions"][0]["eligible_at"] == datetime(
        2026, 9, 23, 1, tzinfo=UTC
    )


def test_invalid_credentials_require_intervention(
    database: Database, tmp_path: Path
) -> None:
    """Credentials failures remain explicit and never appear in stored metadata."""
    from pydantic import SecretStr

    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(
        taxonomy.settings.model_copy(update={"api_key": SecretStr("fixture-secret")})
    )
    scan, _ = catalog.start_scan(
        {}, "88888888-8888-8888-8888-888888888888", datetime(2026, 9, 23, tzinfo=UTC)
    )
    result = refresh_job.execute_in_process(
        resources={
            "runtime": Runtime(
                catalog.settings,
                httpx.MockTransport(
                    lambda request: httpx.Response(401, content=b"unauthorized")
                ),
                lambda: datetime(2026, 9, 23, tzinfo=UTC),
            )
        },
        run_config={
            "ops": {
                "refresh_partition": {
                    "config": {"scan_id": scan, "publication_date": "2026-01-01"}
                }
            }
        },
        raise_on_error=False,
    )
    assert not result.success
    state = catalog.read_works()
    assert state["partitions"][0]["status"] == "intervention_required"
    assert "fixture-secret" not in str(state)


@pytest.mark.parametrize(
    "stage",
    [
        "before_raw_publish",
        "after_api_raw_commit",
        "after_snapshot_output_publish",
        "before_api_commit",
        "after_api_commit",
    ],
)
def test_fresh_process_recovery(database: Database, tmp_path: Path, stage: str) -> None:
    """Crash boundaries never lose pages or duplicate logical processing."""
    import os
    import subprocess
    import sys

    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings.model_copy(update={"api_key": None}))
    scan, _ = catalog.start_scan(
        {}, "99999999-9999-9999-9999-999999999999", datetime(2026, 9, 23, tzinfo=UTC)
    )
    env = os.environ | {
        "TEST_DATABASE_URL": database[0].get_secret_value(),
        "TEST_SCHEMA": database[1],
        "TEST_STORAGE": str(tmp_path),
        "TEST_SCAN": scan,
        "TEST_NOW": "2026-09-23T00:00:00+00:00",
        "CRASH_STAGE": stage,
    }
    first = subprocess.run(
        [sys.executable, "-m", "tests.api_worker"],
        env=env,
        capture_output=True,
        check=False,
    )
    assert first.returncode == 73
    env["CRASH_STAGE"] = ""
    # A fresh process cannot steal the lease left by the killed worker.
    second = subprocess.run(
        [sys.executable, "-m", "tests.api_worker"],
        env=env,
        capture_output=True,
        check=False,
    )
    assert second.returncode == 0
    if stage == "after_snapshot_output_publish":
        assert (tmp_path / "requested-cursors").read_text().splitlines() == [
            "*",
            "next",
        ]
    else:
        assert (tmp_path / "requested-cursors").read_text().splitlines() == ["*"]
    env["TEST_NOW"] = "2026-09-23T00:06:00+00:00"
    third = subprocess.run(
        [sys.executable, "-m", "tests.api_worker"],
        env=env,
        capture_output=True,
        check=False,
    )
    assert third.returncode == 0
    state = catalog.read_works()
    assert state["partitions"][0]["status"] == "complete"
    assert state["partitions"][0]["generation"] == 1
    expected = (
        ["*", "next"]
        if stage in ("after_api_commit", "after_snapshot_output_publish")
        else ["*", "*", "next"]
    )
    assert (tmp_path / "requested-cursors").read_text().splitlines() == expected
    assert len(state["versions"]) == len(state["processing"]) == 1


def test_mutable_count_does_not_prevent_cursor_completion(
    database: Database, tmp_path: Path
) -> None:
    """Source count changes are recorded, not treated as snapshot consistency."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings.model_copy(update={"api_key": None}))
    scan, _ = catalog.start_scan(
        {}, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", datetime(2026, 9, 23, tzinfo=UTC)
    )
    row = {"id": "W1", "publication_date": "2026-01-01", "is_xpac": False}
    pages = [
        json.dumps(
            {"meta": {"count": 2, "next_cursor": "next"}, "results": [row]}
        ).encode(),
        json.dumps({"meta": {"count": 3, "next_cursor": None}, "results": []}).encode(),
    ]
    assert execute(catalog, pages, overrides={"scan_id": scan}).success
    partition = catalog.read_works()["partitions"][0]
    assert partition["status"] == "complete"
    assert partition["received_count"] == 1 and partition["record_count"] == 3


def test_failed_page_keeps_observation_provenance(
    database: Database, tmp_path: Path
) -> None:
    """A failed page retains its request, generation, timestamps, and raw location."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings.model_copy(update={"api_key": None}))
    scan, _ = catalog.start_scan(
        {}, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", datetime(2026, 9, 23, tzinfo=UTC)
    )
    assert not execute(catalog, [b"not-json"], overrides={"scan_id": scan}).success
    page = catalog.read_works()["api_pages"][0]
    assert page["status_code"] == 200
    assert page["generation"] == 1 and page["cursor"] == "*"
    assert page["started_at"] == page["ended_at"] == datetime(2026, 9, 23, tzinfo=UTC)
    assert (tmp_path / page["raw_path"]).is_file()


def test_commit_failure_retains_cursor_without_false_running_status(
    database: Database, tmp_path: Path
) -> None:
    """Failed commits retain evidence but publish no observations or outputs."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings.model_copy(update={"api_key": None}))
    scan, _ = catalog.start_scan(
        {}, "cccccccc-cccc-cccc-cccc-cccccccccccc", datetime(2026, 9, 23, tzinfo=UTC)
    )

    def fail(stage: str) -> None:
        if stage == "before_api_commit":
            raise RuntimeError("fixture database failure")

    row = {"id": "W1", "publication_date": "2026-01-01", "is_xpac": False}
    page = json.dumps(
        {"meta": {"count": 1, "next_cursor": None}, "results": [row]}
    ).encode()
    assert not execute(
        catalog, [page], failure=fail, overrides={"scan_id": scan}
    ).success
    state = catalog.read_works()
    assert state["partitions"][0]["status"] == "failed"
    assert not state["observations"] and not state["batches"]
    assert execute(catalog, [page], overrides={"scan_id": scan}).success
    assert len(catalog.read_works()["processing"]) == 1


def test_successful_page_with_exhausted_allowance_pauses(
    database: Database, tmp_path: Path
) -> None:
    """The last allowed page commits before its reset deadline prevents another call."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings.model_copy(update={"api_key": None}))
    scan, _ = catalog.start_scan(
        {}, "dddddddd-dddd-dddd-dddd-dddddddddddd", datetime(2026, 9, 23, tzinfo=UTC)
    )
    requests = []
    row = {"id": "W1", "publication_date": "2026-01-01", "is_xpac": False}

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "3600"},
            json={"meta": {"count": 1, "next_cursor": "next"}, "results": [row]},
        )

    result = refresh_job.execute_in_process(
        resources={
            "runtime": Runtime(
                catalog.settings,
                httpx.MockTransport(handle),
                lambda: datetime(2026, 9, 23, tzinfo=UTC),
            )
        },
        run_config={
            "ops": {
                "refresh_partition": {
                    "config": {"scan_id": scan, "publication_date": "2026-01-01"}
                }
            }
        },
        raise_on_error=False,
    )
    assert (
        result.success
        and result.output_for_node("refresh_partition") == "paused_budget"
    )
    assert len(requests) == 1
    assert catalog.read_works()["partitions"][0]["next_cursor"] == "next"
