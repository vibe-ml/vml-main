"""Exercise deployed schedule evaluation without launching collection jobs."""

from datetime import UTC, datetime

import httpx

QUERY = """
mutation($selector: ScheduleSelector!, $timestamp: Float!) {
  scheduleDryRun(selectorData: $selector, timestamp: $timestamp) {
    __typename
    ... on DryRunInstigationTick {
      evaluationResult {
        runRequests { runKey }
        error { className }
      }
    }
    ... on PythonError { className }
  }
}
"""


def check() -> None:
    """Require one valid run request from each deployed OpenAlex schedule."""
    for name in (
        "daily_taxonomy_schedule",
        "daily_refresh_schedule",
        "process_batches_schedule",
    ):
        response = httpx.post(
            "http://localhost:3002/graphql",
            json={
                "query": QUERY,
                "variables": {
                    "selector": {
                        "repositoryLocationName": "openalex",
                        "repositoryName": "__repository__",
                        "scheduleName": name,
                    },
                    "timestamp": datetime(2026, 9, 25, tzinfo=UTC).timestamp(),
                },
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("errors"):
            raise RuntimeError("Schedule check GraphQL request failed")
        result = payload["data"]["scheduleDryRun"]
        evaluation = result.get("evaluationResult") or {}
        error = evaluation.get("error") or {}
        if (
            result["__typename"] != "DryRunInstigationTick"
            or error
            or len(evaluation.get("runRequests") or []) != 1
        ):
            # Error messages can contain instance configuration; expose only the class.
            failure_type = error.get("className", result["__typename"])
            raise RuntimeError(f"{name}: {failure_type}")
        print(f"{name}: PASS (one run request)")


if __name__ == "__main__":
    check()
