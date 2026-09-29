# ADR 0001: Hybrid Dagster Observability for OpenAlex Crawler Queues

- **Status**: Accepted
- **Date**: 2026-09-26
- **Context**: OpenAlex crawler orchestration via Dagster

## Context

The OpenAlex crawler processes two distinct operational queues:
1. **API Partition Queue**: Date-based partitions scheduled in [`src/ingestion/scheduler.py`](src/ingestion/scheduler.py), dynamically split between `recent_target` and `rotating_target` under daily API rate budget allowances ([`WorkApiAllowances`](src/models/tmd.py#L225)).
2. **Derived Batch Claims Queue**: Asynchronous worker claims tracked in [`tmd.openalex_work_batch_claims`](src/models/tmd.py#L236) and executed via [`rebuild_job`](src/ingestion/rebuild.py#L16).

Currently, these executions are opaque in the Dagster UI:
- [`schedule_daily_refreshes`](src/ingestion/scheduler.py#L78) runs a synchronous Python loop and publishes metadata only upon completion. During execution, operators cannot see how many partitions processed or remain.
- [`rebuild_job`](src/ingestion/rebuild.py#L16) processes single claims on schedule without exposing pending backlog depth.

Adopting pure Dagster declarative partitioning (`DailyPartitionsDefinition`) would require each date to be an independent run, complicating dynamic quota borrowing and capacity reservation.

## Decision

Adopt a **hybrid observability pattern**:

1. **In-Loop `AssetObservation` for API Refreshes**:
   - Keep current synchronous loop in [`scheduler.py`](src/ingestion/scheduler.py#L153) to preserve quota reservation and dynamic budget shifting.
   - Emit live `AssetObservation` events per partition iteration with metadata: `queue_total`, `queue_processed`, `queue_remaining`, `current_partition`, `budget_reserved`.
   - Dagster UI automatically visualizes these numeric metrics over time in asset telemetry plots.

2. **Decoupled Queue Sensor for Batch Claims**:
   - Introduce a Dagster `@sensor` polling [`WorkBatchClaims`](src/models/tmd.py#L236) status counts (`pending`, `running`, `complete`, `failed`).
   - Expose real-time backlog depth in Dagster UI sensor ticks without requiring active job runs.
   - Automatically trigger [`rebuild_job`](src/ingestion/rebuild.py#L16) when pending claims exist.

## Consequences

- **Positive**:
  - Live progress visible in Dagster UI during multi-hour crawler runs.
  - Constant visibility into claim backlog depth even when idle.
  - Zero disruption to API allowance tracking or budget borrowing logic.
- **Negative**:
  - Run timeline displays a single active step rather than native Gantt blocks per date.
  - Native partition matrix view deferred until rate-limit management moves to Dagster run-queue tags.
