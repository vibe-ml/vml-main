"""Declarative Dagster asset definitions for OpenAlex pipeline observability."""

from dagster import AssetKey, AssetSpec

bootstrap_download_asset = AssetSpec(
    key=AssetKey(["openalex", "bootstrap_download"]),
    group_name="openalex_bootstrap",
    description="Raw OpenAlex snapshot Parquet file acquisition",
    kinds={"s3", "parquet"},
)

bootstrap_selection_asset = AssetSpec(
    key=AssetKey(["openalex", "bootstrap_selection"]),
    deps=[bootstrap_download_asset.key],
    group_name="openalex_bootstrap",
    description="Snapshot decompression, scope filtering, and chunk insertion",
    kinds={"postgres", "sql"},
)

bootstrap_progress_asset = AssetSpec(
    key=AssetKey(["openalex", "bootstrap_progress"]),
    deps=[bootstrap_selection_asset.key],
    group_name="openalex_bootstrap",
    description="Aggregated baseline snapshot progress monitored by daemon sensor",
    kinds={"sensor", "postgres"},
)

batch_claims_asset = AssetSpec(
    key=AssetKey(["openalex", "batch_claims"]),
    group_name="openalex_queue",
    description="Worker claim queue depth and batch transform throughput",
    kinds={"sensor", "postgres"},
)

daily_refresh_asset = AssetSpec(
    key=AssetKey(["openalex", "daily_refresh"]),
    group_name="openalex_ingestion",
    description="Daily incremental API partition sync status and budget progress",
    kinds={"api", "http"},
)

observable_assets = [
    bootstrap_download_asset,
    bootstrap_selection_asset,
    bootstrap_progress_asset,
    batch_claims_asset,
    daily_refresh_asset,
]
