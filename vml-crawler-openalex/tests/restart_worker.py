"""Fresh process for abrupt termination at persistence boundaries."""

import os
from pathlib import Path

from pydantic import SecretStr

from tests.test_taxonomy import run


def main() -> None:
    """Run the job, optionally exiting without cleanup at an injected boundary."""

    def terminate(stage: str) -> None:
        """Exit abruptly at the requested persistence boundary."""
        if stage == os.environ.get("CRASH_STAGE"):
            os._exit(73)

    result, _, _ = run(
        (SecretStr(os.environ["TEST_DATABASE_URL"]), os.environ["TEST_SCHEMA"]),
        Path(os.environ["TEST_STORAGE"]),
        failure=terminate,
    )
    raise SystemExit(0 if result.success else 1)


if __name__ == "__main__":
    main()
