"""Authoritative source coverage must survive changed reader boundaries."""

from pathlib import Path

import pytest

from src.ingestion.bootstrap import coverage_gaps, source_chunks


@pytest.mark.parametrize(
    "ranges", [[(-1, 1)], [(0, 0)], [(0, -1)], [(0, 6)], [(0, 3), (2, 2)]]
)
def test_invalid_coverage(ranges: list[tuple[int, int]]) -> None:
    with pytest.raises(ValueError, match="coverage"):
        coverage_gaps(5, ranges)


def test_gaps_preserve_committed_ranges() -> None:
    assert coverage_gaps(12, [(8, 2), (2, 3)]) == [(0, 2), (5, 8), (10, 12)]
    assert coverage_gaps(5, [(2, 3), (0, 2)]) == []
    assert coverage_gaps(0, []) == []


def test_complete_file_never_opens_reader(tmp_path: Path) -> None:
    assert (
        list(source_chunks(tmp_path / "absent.parquet", 5, [(0, 2), (2, 3)], 2)) == []
    )


def test_gap_chunks_do_not_cross_committed_range(tmp_path: Path) -> None:
    import pyarrow as pa
    from pyarrow import parquet

    path = tmp_path / "rows.parquet"
    parquet.write_table(pa.table({"id": list(range(12))}), path, row_group_size=3)
    chunks = list(source_chunks(path, 12, [(2, 3), (8, 2)], 2))
    assert [(start, len(rows)) for start, rows in chunks] == [
        (0, 2),
        (5, 2),
        (7, 1),
        (10, 2),
    ]
    import json

    assert [json.loads(row)["id"] for _, rows in chunks for row in rows] == [
        0,
        1,
        5,
        6,
        7,
        10,
        11,
    ]


@pytest.mark.parametrize("batch_size", [1, 7, 64])
def test_reader_preserves_legacy_mapping(tmp_path: Path, batch_size: int) -> None:
    """Typed Arrow batches retain exact legacy JSON bytes, hashes and derived data."""
    import json
    from datetime import UTC, date, datetime
    from decimal import Decimal

    import duckdb
    import pyarrow as pa
    from pyarrow import parquet

    from src.ingestion.bootstrap import json_records
    from src.ingestion.scope import CollectionScope
    from src.ingestion.snapshot import derived, work_identity
    from src.ingestion.taxonomy import canonical

    table = pa.table(
        {
            "id": ["W1", "W2", "W3"],
            "publication_date": pa.array(
                [date(2026, 1, 1), date(2026, 9, 28), None], type=pa.date32()
            ),
            "is_xpac": [False, False, True],
            "float32": pa.array([1e-7, 0.123456789, -0.0], type=pa.float32()),
            "float64": [1e-7, 1e20, 1.23456789012345],
            "decimal": pa.array(
                [Decimal("0.0000001"), None, Decimal("-1.2345678")],
                type=pa.decimal128(20, 7),
            ),
            "updated_date": pa.array(
                [datetime(2026, 1, 1, 1, 2, 3, 123456, tzinfo=UTC), None, None],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "abstract_inverted_index": pa.array(
                [[("world", [1]), ("hello", [0])], None, []],
                type=pa.map_(pa.string(), pa.list_(pa.int64())),
            ),
            "unknown": [{"nested": [1, None, 2]}, None, {"nested": []}],
            "title": ['漢字\n"\\', None, "🙂"],
        }
    )
    path = tmp_path / "typed.parquet"
    parquet.write_table(table, path, row_group_size=2)
    with duckdb.connect() as db:
        old = [
            r[0]
            for r in db.execute(
                "SELECT to_json(t) FROM read_parquet(?) t", [str(path)]
            ).fetchall()
        ]
    new = list(json_records(path, batch_size=batch_size))
    assert new == old
    scope = CollectionScope(publication_through=date(2026, 9, 28))
    for left, right in zip(old, new, strict=True):
        a, b = [json.loads(v, parse_float=Decimal) for v in [left, right]]
        assert canonical(a) == canonical(b)
        assert work_identity(a) == work_identity(b)
        assert scope.disposition(a) == scope.disposition(b)
        assert derived(a) == derived(b)


def test_oversized_rejected_row_is_not_skipped(tmp_path: Path) -> None:
    import pyarrow as pa
    from pyarrow import parquet

    from src.ingestion.bootstrap import MAX_RECORD_BYTES

    path = tmp_path / "oversize.parquet"
    parquet.write_table(
        pa.table({"is_xpac": [True], "title": ["a" * (MAX_RECORD_BYTES + 1)]}),
        path,
        compression="zstd",
    )
    with pytest.raises(ValueError, match="record exceeds"):
        list(source_chunks(path, 1, [], 1000))
