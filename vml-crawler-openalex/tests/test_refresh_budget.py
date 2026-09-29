"""Fair refresh and free allowance acceptance through ingestion jobs."""

from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr

from src.ingestion.job import Runtime
from src.ingestion.scheduler import daily_refresh_job
from src.ingestion.works_store import WorksCatalog
from tests.test_taxonomy import Database, run


def test_partial_allowance_keeps_recent_and_rotating_reservations(
    database: Database, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Twenty available calls fund eighteen works calls split across both queues."""
    _, taxonomy, _ = run(database, tmp_path)
    settings = taxonomy.settings.model_copy(update={"api_key": SecretStr("fixture")})
    requests = []

    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/rate-limit":
            return httpx.Response(
                200,
                json={
                    "rate_limit": {
                        "credits_limit": 1000,
                        "credits_remaining": 200,
                        "credits_used": 800,
                        "resets_in_seconds": 86400,
                        "credit_costs": {"list": 10},
                    }
                },
            )
        requests.append(request)
        return httpx.Response(
            200, json={"meta": {"count": 0, "next_cursor": None}, "results": []}
        )

    # Scheduling must not materialize the corpus, which exceeds worker memory.
    original_read = WorksCatalog.read_works

    def reject_corpus_read(*args, **kwargs):
        raise AssertionError("Refresh scheduling must not load the full corpus")

    monkeypatch.setattr(WorksCatalog, "read_works", reject_corpus_read)
    result = daily_refresh_job.execute_in_process(
        resources={
            "runtime": Runtime(
                settings,
                httpx.MockTransport(handle),
                lambda: datetime(2026, 9, 24, tzinfo=UTC),
            )
        },
        raise_on_error=False,
    )
    assert result.success
    state = original_read(WorksCatalog(settings))
    completed = [p for p in state["partitions"] if p["status"] == "complete"]
    assert len(requests) == len(completed) == 18
    recent = [p for p in completed if p["publication_date"] >= date(2026, 8, 26)]
    assert len(recent) == 13
    assert date(2026, 9, 24) in [p["publication_date"] for p in recent]
    assert min(p["publication_date"] for p in completed) == date(2026, 1, 1)
    report = result.output_for_node("schedule_refresh")
    assert report["completed_partitions"] == 18
    assert report["reserved_requests"] == 18
    assert report["whole_corpus_fresh"] is False

    observations = [
        e.event_specific_data.asset_observation
        for e in result.all_events
        if e.is_asset_observation
        and e.event_specific_data.asset_observation.asset_key.path
        == ["openalex", "daily_refresh"]
    ]
    assert len(observations) == 18
    first_obs = observations[0]
    assert "queue_name" in first_obs.metadata
    assert "current_partition" in first_obs.metadata
    assert "queue_total" in first_obs.metadata
    assert "queue_processed" in first_obs.metadata
    assert "queue_remaining" in first_obs.metadata
    assert "status" in first_obs.metadata
    assert "budget_reserved_used" in first_obs.metadata
    assert "completed_count" in first_obs.metadata
