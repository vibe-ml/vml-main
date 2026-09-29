"""Reconciliation through ingestion jobs and committed consumer state."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from src.ingestion.works_store import WorksCatalog
from tests.test_api_partition import execute as execute_api
from tests.test_bootstrap import execute as execute_bootstrap
from tests.test_snapshot import execute as execute_snapshot
from tests.test_snapshot import sample
from tests.test_taxonomy import Database, run

BASE = {"id": "W1", "publication_date": "2026-01-01", "is_xpac": False}


def observe(catalog: WorksCatalog, rows: list[dict], tick: int = 0) -> None:
    """Observe an API response at a distinct instant through the public job."""
    body = json.dumps(
        {"meta": {"next_cursor": None, "count": tick}, "results": rows}
    ).encode()
    scan, _ = catalog.start_scan(
        {"observation": str(uuid4())}, str(uuid4()), datetime(2026, 9, 23, tzinfo=UTC)
    )
    assert execute_api(
        catalog,
        [body],
        overrides={"scan_id": scan},
        now=lambda: datetime(2026, 9, 23, tzinfo=UTC) + timedelta(seconds=tick),
    ).success


def current_payload(catalog: WorksCatalog) -> dict:
    """Read the selected version using the consumer contract."""
    state = catalog.read_works()
    assert len(state["current"]) == 1
    return next(
        row["payload"]
        for row in state["versions"]
        if row["id"] == state["current"][0]["version_id"]
    )


def test_unknown_record_fields_are_content(database: Database, tmp_path: Path) -> None:
    """Record metadata-like names stay content; response metadata does not."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    original = BASE | {"meta": {"source": 1}, "_local": "source supplied", "null": None}
    observe(catalog, [original])
    observe(catalog, [original], 1)
    state = catalog.read_works()
    assert len(state["versions"]) == len(state["processing"]) == 1
    observe(catalog, [original | {"_local": "changed"}], 2)
    observe(catalog, [original | {"meta": {"source": 2}}], 3)
    state = catalog.read_works()
    assert len(state["versions"]) == len(state["processing"]) == 3
    assert len(state["observations"]) == 4


@pytest.mark.parametrize(
    ("first", "second", "expected"),
    [
        ("2026-01-02T00:00:00+02:00", "2026-01-01T23:00:00Z", "second"),
        ("2026-01-02", "not-a-date", "second"),
        (None, "2026-01-01", "second"),
        ("2026-01-02", "2026-01-01", "first"),
        ("2026-01-01", "2026-01-01", "second"),
    ],
)
def test_api_timestamp_precedence(
    database: Database,
    tmp_path: Path,
    first: str | None,
    second: str | None,
    expected: str,
) -> None:
    """Compare instants, retain corrections, and expose invalid-date uncertainty."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    observe(catalog, [BASE | {"title": "first", "updated_date": first}])
    observe(catalog, [BASE | {"title": "second", "updated_date": second}], 1)
    assert current_payload(catalog)["title"] == expected
    report = catalog.read_works()["reconciliation"][0]
    assert report["uncertain"] == (first is None or second == "not-a-date")
    assert report["conflict"]
    assert len(report["version_ids"]) == 2


def baseline(catalog: WorksCatalog, tmp_path: Path, record: dict) -> None:
    """Publish a full baseline with a single original Parquet file."""
    body = sample(tmp_path, [record])
    url = "s3://openalex/data/parquet/works/history.parquet"
    manifest = json.dumps(
        {
            "date": "2026-09-23",
            "format": "parquet",
            "entity": "works",
            "record_count": 1,
            "content_length": len(body),
            "files": [
                {
                    "url": url,
                    "meta": {
                        "record_count": 1,
                        "content_length": len(body),
                        "sha256": hashlib.sha256(body).hexdigest(),
                    },
                }
            ],
        }
    ).encode()
    assert execute_bootstrap(
        catalog,
        manifest,
        {url.replace("s3://openalex/", "https://openalex.s3.amazonaws.com/"): body},
        [],
    ).success


@pytest.mark.parametrize("stamp", ["2026-01-01", "2026-01-02", None, "invalid"])
def test_late_baseline_preserves_api_selection(
    database: Database, tmp_path: Path, stamp: str | None
) -> None:
    """Old, equal, and incomparable snapshot dates cannot displace the API."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    observe(catalog, [BASE | {"title": "API", "updated_date": "2026-01-02"}])
    baseline(catalog, tmp_path, BASE | {"title": "snapshot", "updated_date": stamp})
    assert current_payload(catalog)["title"] == "API"
    state = catalog.read_works()
    assert len(state["versions"]) == len(state["observations"]) == 2
    assert state["reconciliation"][0]["uncertain"] == (stamp in (None, "invalid"))


def test_reversion_reuses_processing_and_retains_absent_works(
    database: Database, tmp_path: Path
) -> None:
    """A-B-A retains each observation without reprocessing A or removing W2."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    original = BASE | {"title": "A", "updated_date": "2026-01-01"}
    observe(catalog, [original, original | {"id": "W2"}])
    first = catalog.read_works()
    original_id = next(
        row["version_id"] for row in first["current"] if row["entity_id"] == "W1"
    )
    observe(catalog, [original | {"title": "B"}], 1)
    observe(catalog, [original], 2)
    observe(catalog, [], 3)
    state = catalog.read_works()
    assert len(state["current"]) == 2
    assert (
        next(row["version_id"] for row in state["current"] if row["entity_id"] == "W1")
        == original_id
    )
    assert len(state["versions"]) == len(state["processing"]) == 3
    assert len(state["observations"]) == 4
    assert all(
        row["source_id"] in {source["id"] for source in state["sources"]}
        for row in state["observations"]
    )


def test_supported_mapping_and_representation_conflicts(
    database: Database, tmp_path: Path
) -> None:
    """Numeric spelling agrees across sources; array order and nulls remain content."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    record = BASE | {
        "numeric": 1.0,
        "nested": {"unknown": None},
        "array": [1, 2],
        "updated_date": "2026-01-01",
    }
    assert execute_snapshot(catalog, sample(tmp_path, [record]), 1).success
    observe(catalog, [dict(reversed(list((record | {"numeric": 1}).items())))], 1)
    state = catalog.read_works()
    assert len(state["versions"]) == len(state["processing"]) == 1
    assert not state["reconciliation"][0]["conflict"]
    observe(catalog, [record | {"array": [2, 1]}], 2)
    observe(catalog, [record | {"nested": {}}], 3)
    state = catalog.read_works()
    assert len(state["versions"]) == len(state["processing"]) == 3
    assert state["reconciliation"][0]["conflict"]


@pytest.mark.parametrize("order", [(0, 1, 2), (2, 0, 1), (1, 2, 0)])
def test_incomparable_dates_do_not_make_selection_order_dependent(
    database: Database, tmp_path: Path, order: tuple[int, ...]
) -> None:
    """Known older timestamps cannot re-enter precedence through missing dates."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    records = [
        BASE | {"title": "new", "updated_date": "2026-01-03"},
        BASE | {"title": "unknown", "updated_date": None},
        BASE | {"title": "old", "updated_date": "2026-01-01"},
    ]
    # Fixed observation instants; arrival order varies.
    for index in order:
        observe(catalog, [records[index]], index)
    assert current_payload(catalog)["title"] == "unknown"
    assert catalog.read_works()["reconciliation"][0]["uncertain"]


def test_stable_tie_and_newer_snapshot(database: Database, tmp_path: Path) -> None:
    """Exact observation ties use stable identity; a genuinely newer baseline wins."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    records = [
        BASE | {"title": title, "updated_date": "2026-01-01"} for title in ("A", "B")
    ]
    for record in records:
        observe(catalog, [record])
    state = catalog.read_works()
    assert state["current"][0]["version_id"] == max(
        row["id"] for row in state["versions"]
    )
    baseline(
        catalog,
        tmp_path,
        BASE | {"title": "new snapshot", "updated_date": "2026-01-02"},
    )
    assert current_payload(catalog)["title"] == "new snapshot"


def test_later_page_reversion_uses_retrieval_time(
    database: Database, tmp_path: Path
) -> None:
    """Corrections and reversions within one run follow actual page observations."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    scan, _ = catalog.start_scan({}, str(uuid4()), datetime(2026, 9, 23, tzinfo=UTC))
    first = BASE | {"title": "original", "updated_date": "2026-01-01"}
    pages = [
        json.dumps(
            {"meta": {"next_cursor": cursor, "count": 3}, "results": [record]}
        ).encode()
        for cursor, record in (
            ("second", first),
            ("third", first | {"title": "correction"}),
            (None, first),
        )
    ]
    ticks = iter(range(1000))
    assert execute_api(
        catalog,
        pages,
        overrides={"scan_id": scan},
        now=lambda: (
            datetime(2026, 9, 23, tzinfo=UTC) + timedelta(milliseconds=next(ticks))
        ),
    ).success
    assert current_payload(catalog)["title"] == "original"


@pytest.mark.parametrize("timestamp", ["2026-01-01", None])
def test_snapshot_recency_with_equal_or_missing_dates(
    database: Database, tmp_path: Path, timestamp: str | None
) -> None:
    """Later snapshot observations resolve equal or incomparable source dates."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    first = BASE | {"title": "earlier", "updated_date": timestamp}
    assert execute_snapshot(catalog, sample(tmp_path, [first]), 1).success
    assert execute_snapshot(
        catalog,
        sample(tmp_path, [first | {"title": "later"}]),
        1,
        now=lambda: datetime(2026, 9, 23, 1, tzinfo=UTC),
        overrides={"source": "https://fixture.test/later.parquet"},
    ).success
    assert current_payload(catalog)["title"] == "later"
    assert catalog.read_works()["reconciliation"][0]["uncertain"] == (timestamp is None)
