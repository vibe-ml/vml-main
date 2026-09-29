"""Tests for batch claims Dagster sensor."""

from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import sqlalchemy as sa
from dagster import build_sensor_context

from src.ingestion.sensor import batch_claims_sensor, bootstrap_progress_sensor
from src.ingestion.works_store import WorksCatalog
from src.models.tmd import (
    WorkBaselines,
    WorkBatchClaims,
    WorkBatches,
    WorkChunks,
    WorkReleaseFiles,
    WorkReleases,
    WorkScopes,
    WorkSources,
)
from tests.test_bootstrap import execute as bootstrap_execute
from tests.test_bootstrap import fixture as bootstrap_fixture
from tests.test_snapshot import execute, sample
from tests.test_taxonomy import Database, run


def test_batch_claims_sensor_empty_queue(database: Database, tmp_path: Path) -> None:
    """Sensor yields observation and skips run when no batch claims exist."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)

    context = build_sensor_context(resources={"catalog": catalog})
    result = batch_claims_sensor(context)

    assert len(result.run_requests) == 0
    assert result.skip_reason is not None
    assert (
        "Pending: 0 | Running: 0 | Complete: 0 | Failed: 0"
        in result.skip_reason.skip_message
    )
    assert len(result.asset_events) == 1

    event = result.asset_events[0]
    assert event.asset_key.path == ["openalex", "batch_claims"]
    assert event.metadata["pending"].value == 0
    assert event.metadata["total"].value == 0


def test_batch_claims_sensor_triggers_pending_and_manages_cursor(
    database: Database, tmp_path: Path
) -> None:
    """Sensor emits run request for pending claim set and updates cursor to prevent duplicates."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)

    # Populate baseline source and taxonomy records via sample execute
    body = sample(
        tmp_path,
        [
            {
                "id": "https://openalex.org/W100",
                "title": "Observability Test",
                "publication_date": "2026-01-01",
                "is_xpac": False,
            }
        ],
    )
    assert execute(catalog, body, 1).success

    # Read existing claim to obtain valid foreign keys
    with catalog.transaction() as conn:
        existing_claim = (
            conn.execute(sa.select(WorkBatchClaims).limit(1)).mappings().one()
        )

        claim_id_1 = str(uuid4())
        claim_id_2 = str(uuid4())
        sorted_ids = sorted([claim_id_1, claim_id_2])
        conn.execute(
            sa.insert(WorkBatchClaims).values(
                [
                    {
                        "id": sorted_ids[0],
                        "source_id": existing_claim["source_id"],
                        "scope_id": existing_claim["scope_id"],
                        "transformation": "test_transform_1",
                        "dependency": "dep_1",
                        "taxonomy_id": existing_claim["taxonomy_id"],
                        "status": "pending",
                    },
                    {
                        "id": sorted_ids[1],
                        "source_id": existing_claim["source_id"],
                        "scope_id": existing_claim["scope_id"],
                        "transformation": "test_transform_2",
                        "dependency": "dep_2",
                        "taxonomy_id": existing_claim["taxonomy_id"],
                        "status": "pending",
                    },
                ]
            )
        )

    expected_key = f"2:{sorted_ids[0]}:{sorted_ids[1]}"

    # First evaluation: detects 2 pending claims and requests run for the claim set
    context1 = build_sensor_context(resources={"catalog": catalog})
    result1 = batch_claims_sensor(context1)

    assert len(result1.run_requests) == 1
    assert result1.run_requests[0].run_key == f"batch_claims_{expected_key}"
    assert result1.cursor == expected_key

    obs1 = result1.asset_events[0]
    assert obs1.metadata["pending"].value == 2
    assert obs1.metadata["complete"].value >= 1

    # Second evaluation with the previous cursor: duplicate run requests suppressed
    context2 = build_sensor_context(
        cursor=result1.cursor, resources={"catalog": catalog}
    )
    result2 = batch_claims_sensor(context2)

    assert len(result2.run_requests) == 0
    assert result2.skip_reason is not None
    assert result2.cursor == expected_key
    assert result2.asset_events[0].metadata["pending"].value == 2

    # Transition first claim to running, second claim to complete, insert one failed
    with catalog.transaction() as conn:
        conn.execute(
            sa.update(WorkBatchClaims)
            .where(WorkBatchClaims.id == sorted_ids[0])
            .values(status="running", claimed_at=datetime.now(UTC))
        )
        conn.execute(
            sa.update(WorkBatchClaims)
            .where(WorkBatchClaims.id == sorted_ids[1])
            .values(status="complete")
        )
        claim_id_3 = str(uuid4())
        conn.execute(
            sa.insert(WorkBatchClaims).values(
                id=claim_id_3,
                source_id=existing_claim["source_id"],
                scope_id=existing_claim["scope_id"],
                transformation="test_transform_3",
                dependency="dep_3",
                taxonomy_id=existing_claim["taxonomy_id"],
                status="failed",
            )
        )

    context3 = build_sensor_context(
        cursor=result2.cursor, resources={"catalog": catalog}
    )
    result3 = batch_claims_sensor(context3)

    assert len(result3.run_requests) == 0
    assert result3.cursor == ""
    obs3 = result3.asset_events[0]
    assert obs3.metadata["pending"].value == 0
    assert obs3.metadata["running"].value == 1
    assert obs3.metadata["failed"].value == 1
    assert obs3.metadata["complete"].value >= 2


def test_bootstrap_progress_sensor_no_baseline(
    database: Database, tmp_path: Path
) -> None:
    """Sensor skips gracefully when no snapshot baseline exists."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)

    context = build_sensor_context(resources={"catalog": catalog})
    result = bootstrap_progress_sensor(context)

    assert len(result.run_requests) == 0
    assert result.skip_reason is not None
    assert "No snapshot baseline found" in result.skip_reason.skip_message
    assert len(result.asset_events) == 0


def test_bootstrap_progress_sensor_tracks_baseline_progress(
    database: Database, tmp_path: Path
) -> None:
    """Sensor emits AssetObservation tracking file acquisition and row selection."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)

    manifest, bodies = bootstrap_fixture(tmp_path)
    calls = []
    exec_result = bootstrap_execute(catalog, manifest, bodies, calls)
    assert exec_result.success

    context = build_sensor_context(resources={"catalog": catalog})
    result = bootstrap_progress_sensor(context)

    assert len(result.run_requests) == 0
    assert result.skip_reason is not None
    assert "complete" in result.skip_reason.skip_message
    assert len(result.asset_events) == 1

    event = result.asset_events[0]
    assert event.asset_key.path == ["openalex", "bootstrap_progress"]
    assert event.metadata["total_files"].value == 1
    assert event.metadata["acquired_files"].value == 1
    assert event.metadata["pct_downloaded"].value == 100.0
    assert "acquired_gib" in event.metadata
    assert isinstance(event.metadata["acquired_gib"].value, float)
    assert "acquired_bytes_str" in event.metadata
    assert isinstance(event.metadata["acquired_bytes_str"].value, str)
    assert "acquired_bytes" not in event.metadata
    assert event.metadata["processed_rows"].value == 5
    assert event.metadata["total_rows"].value == 5
    assert event.metadata["pct_processed"].value == 100.0
    assert event.metadata["committed_chunks"].value >= 1


def test_bootstrap_progress_sensor_selects_active_baseline_on_tie(
    database: Database, tmp_path: Path
) -> None:
    """Sensor prioritizes active baseline when multiple baselines share release retrieved_at."""
    _, taxonomy, _ = run(database, tmp_path)
    bundle_id = taxonomy.read()["bundles"][0]["id"]

    # Configure active runtime settings with publication_from 2024-01-01
    active_settings = taxonomy.settings.model_copy(
        update={"publication_from": date(2024, 1, 1)}
    )
    catalog = WorksCatalog(active_settings)

    # Seed shared release and release file
    retrieved_time = datetime(2026, 9, 25, 14, 4, 46, tzinfo=UTC)
    with catalog.transaction() as conn:
        conn.execute(
            sa.insert(WorkReleases).values(
                id="release_tie_test",
                release="2026-09-23",
                manifest_path="manifest.json",
                manifest_source="https://example.com/manifest.json",
                retrieved_at=retrieved_time,
                checked_at=None,
                acquisition_status="acquired",
            )
        )
        conn.execute(
            sa.insert(WorkSources).values(
                id="source_tie_1",
                release="2026-09-23",
                source="https://example.com/part_0000.parquet",
                path="part_0000.parquet",
                sha256="0" * 64,
                bytes=10 * 1024 * 1024,
                rows=1000,
            )
        )
        conn.execute(
            sa.insert(WorkReleaseFiles).values(
                id="file_tie_1",
                release_id="release_tie_test",
                inventory={},
                source_id="source_tie_1",
                retrieved_at=retrieved_time,
            )
        )
        # Scopes: older 2026-01-01 scope vs active 2024-01-01 scope
        conn.execute(
            sa.insert(WorkScopes).values(
                [
                    {
                        "id": "scope_older",
                        "definition": {"publication_from": "2026-01-01"},
                    },
                    {
                        "id": "scope_active",
                        "definition": {"publication_from": "2024-01-01"},
                    },
                ]
            )
        )
        # Baselines: completed older baseline vs active partial baseline
        conn.execute(
            sa.insert(WorkBaselines).values(
                [
                    {
                        "id": "baseline_older_completed",
                        "release_id": "release_tie_test",
                        "scope_id": "scope_older",
                        "chunk_rows": 10,
                        "selection_status": "complete",
                    },
                    {
                        "id": "baseline_active_partial",
                        "release_id": "release_tie_test",
                        "scope_id": "scope_active",
                        "chunk_rows": 10,
                        "selection_status": "partial",
                    },
                ]
            )
        )
        conn.execute(
            sa.insert(WorkBatches).values(
                id="batch_tie_1",
                taxonomy_id=bundle_id,
                manifest={},
            )
        )
        # Baseline older: 10 chunks (100 rows out of 1000)
        chunks_older = [
            {
                "id": f"chunk_older_{i}",
                "baseline_id": "baseline_older_completed",
                "file_id": "file_tie_1",
                "start_row": i * 10,
                "row_count": 10,
                "batch_id": "batch_tie_1",
            }
            for i in range(10)
        ]
        # Baseline active: 50 chunks (500 rows out of 1000)
        chunks_active = [
            {
                "id": f"chunk_active_{i}",
                "baseline_id": "baseline_active_partial",
                "file_id": "file_tie_1",
                "start_row": i * 10,
                "row_count": 10,
                "batch_id": "batch_tie_1",
            }
            for i in range(50)
        ]
        conn.execute(sa.insert(WorkChunks).values(chunks_older + chunks_active))

    context = build_sensor_context(resources={"catalog": catalog})
    result = bootstrap_progress_sensor(context)

    assert len(result.run_requests) == 0
    assert result.skip_reason is not None
    assert "partial" in result.skip_reason.skip_message
    assert len(result.asset_events) == 1

    event = result.asset_events[0]
    assert event.asset_key.path == ["openalex", "bootstrap_progress"]
    assert event.metadata["baseline_id"].value == "baseline_active_partial"
    assert event.metadata["selection_status"].value == "partial"
    assert event.metadata["total_files"].value == 1
    assert event.metadata["acquired_files"].value == 1
    assert event.metadata["pct_downloaded"].value == 100.0
    assert "acquired_gib" in event.metadata
    assert isinstance(event.metadata["acquired_gib"].value, float)
    assert "acquired_bytes_str" in event.metadata
    assert isinstance(event.metadata["acquired_bytes_str"].value, str)
    assert "acquired_bytes" not in event.metadata
    assert event.metadata["processed_rows"].value == 500
    assert event.metadata["total_rows"].value == 1000
    assert event.metadata["pct_processed"].value == 50.0
    assert event.metadata["committed_chunks"].value == 50
