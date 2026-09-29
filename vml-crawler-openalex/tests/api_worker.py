"""Run API refresh in a fresh process with abrupt failure injection."""

import os
from datetime import UTC, datetime
from pathlib import Path

import httpx
from pydantic import SecretStr

from src.common.settings import Settings
from src.ingestion.job import Runtime
from src.ingestion.refresh import refresh_job


def main() -> None:
    """Exit at a persistence boundary; reuse only filesystem and PostgreSQL state."""
    settings = Settings(
        _env_file=None,
        database_url=SecretStr(os.environ["TEST_DATABASE_URL"]),
        collection_schema=os.environ["TEST_SCHEMA"],
        storage_root=Path(os.environ["TEST_STORAGE"]),
        api_key=None,
    )
    row = {"id": "W1", "publication_date": "2026-01-01", "is_xpac": False}

    def handle(request: httpx.Request) -> httpx.Response:
        cursor = request.url.params["cursor"]
        with (settings.storage_root / "requested-cursors").open("a") as output:
            output.write(cursor + "\n")
        return httpx.Response(
            200,
            json={
                "meta": {"count": 1, "next_cursor": "next" if cursor == "*" else None},
                "results": [row] if cursor == "*" else [],
            },
        )

    def terminate(stage: str) -> None:
        if stage == os.environ.get("CRASH_STAGE"):
            os._exit(73)

    runtime = Runtime(
        settings,
        httpx.MockTransport(handle),
        lambda: datetime.fromisoformat(os.environ["TEST_NOW"]).astimezone(UTC),
        terminate,
    )
    result = refresh_job.execute_in_process(
        resources={"runtime": runtime},
        run_config={
            "ops": {
                "refresh_partition": {
                    "config": {
                        "scan_id": os.environ["TEST_SCAN"],
                        "publication_date": "2026-01-01",
                    }
                }
            }
        },
        raise_on_error=False,
    )
    raise SystemExit(0 if result.success else 1)


if __name__ == "__main__":
    main()
