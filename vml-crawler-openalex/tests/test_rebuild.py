"""Rebuild derived outputs without recollection."""

from datetime import UTC, datetime
from pathlib import Path

import httpx
import sqlalchemy as sa

from src.ingestion.job import Runtime
from src.ingestion.rebuild import rebuild_job
from src.ingestion.works_store import WorksCatalog
from src.models.raw import TaxonomyBundles, TaxonomyObservations
from src.models.tmd import TaxonomyRuns
from tests.test_snapshot import execute, sample
from tests.test_taxonomy import Database, run


def test_rebuild_derived_outputs(database: Database, tmp_path: Path) -> None:
    """Rebuild a committed batch from raw inputs with new transformation/taxonomy."""
    # First run snapshot to get baseline
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)

    body = sample(
        tmp_path,
        [
            {
                "id": "https://openalex.org/W1",
                "title": "A",
                "publication_date": "2026-01-01",
                "is_xpac": False,
            }
        ],
    )
    assert execute(catalog, body, 1).success

    state = catalog.read_works()
    assert len(state["batches"]) == 1

    original_classification_hash = taxonomy.read()["bundles"][0]["classification_hash"]

    # Fake a count-only taxonomy change
    with catalog.transaction() as conn:
        conn.execute(
            sa.insert(TaxonomyBundles).values(
                id="https://openalex.org/bundles/999",
                classification_hash=original_classification_hash,
                canonicalization_version=1,
                records=[],
            )
        )
        conn.execute(
            sa.insert(TaxonomyRuns).values(
                id="00000000-0000-0000-0000-000000000999",
                started_at=datetime.fromisoformat("2026-09-24T00:00:00Z"),
                status="complete",
            )
        )
        conn.execute(
            sa.insert(TaxonomyObservations).values(
                attempt_id="00000000-0000-0000-0000-000000000999",
                bundle_id="https://openalex.org/bundles/999",
                started_at=datetime.fromisoformat("2026-09-24T00:00:00Z"),
                ended_at=datetime.fromisoformat("2026-09-24T00:00:01Z"),
            )
        )

    # Rebuild
    runtime = Runtime(
        catalog.settings,
        httpx.MockTransport(lambda request: httpx.Response(500)),
        lambda: datetime(2026, 9, 23, tzinfo=UTC),
        lambda stage: None,
    )
    rebuild_result = rebuild_job.execute_in_process(
        resources={"runtime": runtime},
        run_config={},
        raise_on_error=False,
    )
    assert rebuild_result.success

    state = catalog.read_works()
    # It should NOT create processing work for unchanged dependency, BUT it should create a new batch manifest!
    # Wait, the batch manifest has `taxonomy_snapshot_id`. Does it create a new batch?
    # "Count-only taxonomy changes retain changed bundles and observations without unnecessary classification-dependent processing; unchanged dependencies create no processing work."
    # The batch should reuse the same processing. But it still writes a batch manifest?
    # Yes, because the manifest says exactly which taxonomy snapshot was used.

    assert len(state["batches"]) == 2
    assert (
        state["batches"][1]["manifest"]["taxonomy_snapshot_id"]
        == "https://openalex.org/bundles/999"
    )
    # No new work_processing entries because the dependency didn't change!
    assert len(state["processing"]) == 1

    # Now fake a TRUE taxonomy change
    with catalog.transaction() as conn:
        conn.execute(
            sa.insert(TaxonomyBundles).values(
                id="https://openalex.org/bundles/998",
                classification_hash="new_dependency_hash",
                canonicalization_version=1,
                records=[],
            )
        )
        conn.execute(
            sa.insert(TaxonomyRuns).values(
                id="00000000-0000-0000-0000-000000000998",
                started_at=datetime.fromisoformat("2026-09-24T00:00:00Z"),
                status="complete",
            )
        )
        conn.execute(
            sa.insert(TaxonomyObservations).values(
                attempt_id="00000000-0000-0000-0000-000000000998",
                bundle_id="https://openalex.org/bundles/998",
                started_at=datetime.fromisoformat("2026-09-24T00:00:00Z"),
                ended_at=datetime.fromisoformat("2026-09-24T00:00:01Z"),
            )
        )

    rebuild_result = rebuild_job.execute_in_process(
        resources={"runtime": runtime},
        run_config={},
        raise_on_error=False,
    )
    assert rebuild_result.success

    state = catalog.read_works()
    assert len(state["batches"]) == 3
    assert (
        state["batches"][2]["manifest"]["taxonomy_snapshot_id"]
        == "https://openalex.org/bundles/998"
    )
    assert len(state["processing"]) == 2  # New dependency
