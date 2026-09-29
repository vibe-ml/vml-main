"""Upgrade and rollback through actual historical implementations in fresh processes."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
from pyarrow import parquet

from src.ingestion.works_store import WorksCatalog
from tests.test_bootstrap import fixture
from tests.test_taxonomy import Database, run


def test_legacy_byte_chunks_upgrade_and_range_safe_rollback(
    database: Database, tmp_path: Path
) -> None:
    """Old committed byte splits survive streaming upgrade and reader rollback."""
    _, taxonomy, _ = run(database, tmp_path)
    catalog = WorksCatalog(taxonomy.settings)
    manifest, _ = fixture(tmp_path)
    path = tmp_path / "wide.parquet"
    parquet.write_table(
        pa.table(
            {
                "id": [f"W{i}" for i in range(20)],
                "publication_date": ["2026-01-01"] * 20,
                "is_xpac": [True] * 20,
                "title": ["a" * 900000] * 20,
            }
        ),
        path,
        compression="zstd",
        row_group_size=7,
    )
    body = path.read_bytes()
    doc = json.loads(manifest)
    first = doc["files"][0]
    first["meta"] = {"content_length": len(body), "record_count": 20}
    second = {
        "url": first["url"].replace("part_0000", "part_0001"),
        "meta": first["meta"],
    }
    third = {
        "url": first["url"].replace("part_0000", "part_0002"),
        "meta": first["meta"],
    }
    doc.update(
        files=[first, second, third], record_count=60, content_length=3 * len(body)
    )
    (tmp_path / "source_manifest.json").write_text(json.dumps(doc))
    (tmp_path / "source_files.json").write_text(
        json.dumps(
            {
                f["url"].replace(
                    "s3://openalex/", "https://openalex.s3.amazonaws.com/"
                ): body.hex()
                for f in doc["files"]
            }
        )
    )
    historical = Path(__file__).parent / "fixtures" / "performance"
    env = os.environ | {
        "TEST_DATABASE_URL": database[0].get_secret_value(),
        "TEST_SCHEMA": database[1],
        "TEST_STORAGE": str(tmp_path),
        "BOOTSTRAP_CHUNK_ROWS": "1000",
        "CRASH_STAGE": "after_bootstrap_chunk_commit",
        "CRASH_COUNT": "3",
        "BOOTSTRAP_IMPLEMENTATION": str(historical / "bootstrap_v0.py"),
    }
    command = [sys.executable, "-m", "tests.bootstrap_worker"]
    old = subprocess.run(command, env=env, capture_output=True, check=False)
    assert old.returncode == 73, old.stdout.decode() + old.stderr.decode()[:1500]
    before = catalog.read_works()
    assert sorted(r["row_count"] for r in before["chunks"]) == [2, 18, 18]
    upgraded = subprocess.run(
        command,
        env=env | {"BOOTSTRAP_IMPLEMENTATION": "", "CRASH_COUNT": "1"},
        capture_output=True,
        check=False,
    )
    assert upgraded.returncode == 73, upgraded.stderr.decode()[-2000:]
    middle = catalog.read_works()
    assert {r["id"] for r in before["chunks"]} < {r["id"] for r in middle["chunks"]}
    rollback = subprocess.run(
        command,
        env=env
        | {
            "BOOTSTRAP_IMPLEMENTATION": str(historical / "bootstrap_v1.py"),
            "CRASH_STAGE": "",
        },
        capture_output=True,
        check=False,
    )
    assert rollback.returncode == 0, rollback.stderr.decode()[-2000:]
    after = catalog.read_works()
    assert len(after["observations"]) == 60
    assert len({(r["source_id"], r["row_number"]) for r in after["observations"]}) == 60
    assert after["baselines"][0]["selection_status"] == "complete"
    assert {r["id"] for r in middle["chunks"]} <= {r["id"] for r in after["chunks"]}
