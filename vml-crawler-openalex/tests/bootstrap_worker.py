"""Fresh bootstrap process with abrupt exit at durable boundaries."""

import json
import os
import resource
from pathlib import Path

from pydantic import SecretStr

from src.common.settings import Settings
from src.ingestion.works_store import WorksCatalog
from tests.test_bootstrap import execute


def main() -> None:
    """Recover against real persisted state without retained Python objects."""
    implementation = os.environ.get("BOOTSTRAP_IMPLEMENTATION")
    if implementation:
        import importlib.util
        import sys

        name = "src.ingestion.bootstrap"
        spec = importlib.util.spec_from_file_location(name, implementation)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    root = Path(os.environ["TEST_STORAGE"])
    settings = Settings(
        database_url=SecretStr(os.environ["TEST_DATABASE_URL"]),
        collection_schema=os.environ["TEST_SCHEMA"],
        storage_root=root,
    )
    manifest = (root / "source_manifest.json").read_bytes()
    bodies = {
        url: bytes.fromhex(body)
        for url, body in json.loads((root / "source_files.json").read_text()).items()
    }

    matching_stages = 0

    def terminate(stage: str) -> None:
        nonlocal matching_stages
        if stage == os.environ["CRASH_STAGE"]:
            matching_stages += 1
        if stage == os.environ["CRASH_STAGE"] and matching_stages == int(
            os.environ.get("CRASH_COUNT", "1")
        ):
            (root / "peak_rss_kib").write_text(
                str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            )
            os._exit(73)

    calls = []
    result = execute(
        WorksCatalog(settings),
        manifest,
        bodies,
        calls,
        terminate,
        chunk_rows=int(os.environ.get("BOOTSTRAP_CHUNK_ROWS", "2")),
    )
    (root / "resume_calls.json").write_text(json.dumps(calls))
    raise SystemExit(0 if result.success else 1)


if __name__ == "__main__":
    main()
