"""Unit tests for declarative OpenAlex asset graph and metadata."""

from dagster import AssetKey

from src.ingestion.assets import observable_assets
from src.ingestion.defs import defs


def test_defs_contains_all_observable_assets() -> None:
    """Asset graph resolves and registers all 5 pipeline assets."""
    asset_graph = defs.resolve_asset_graph()
    keys = asset_graph.get_all_asset_keys()

    expected_keys = {
        AssetKey(["openalex", "bootstrap_download"]),
        AssetKey(["openalex", "bootstrap_selection"]),
        AssetKey(["openalex", "bootstrap_progress"]),
        AssetKey(["openalex", "batch_claims"]),
        AssetKey(["openalex", "daily_refresh"]),
    }
    assert expected_keys.issubset(keys)
    assert len(observable_assets) == 5


def test_bootstrap_asset_lineage() -> None:
    """Bootstrap assets form a 3-tier linear dependency graph."""
    asset_graph = defs.resolve_asset_graph()

    download_key = AssetKey(["openalex", "bootstrap_download"])
    selection_key = AssetKey(["openalex", "bootstrap_selection"])
    progress_key = AssetKey(["openalex", "bootstrap_progress"])

    download_node = asset_graph.get(download_key)
    selection_node = asset_graph.get(selection_key)
    progress_node = asset_graph.get(progress_key)

    # Lineage: download -> selection -> progress
    assert len(download_node.parent_keys) == 0
    assert selection_key in download_node.child_keys

    assert download_key in selection_node.parent_keys
    assert progress_key in selection_node.child_keys

    assert selection_key in progress_node.parent_keys
    assert len(progress_node.child_keys) == 0


def test_asset_groups_and_kinds() -> None:
    """Assets are classified under expected groups and kinds."""
    asset_graph = defs.resolve_asset_graph()

    download = asset_graph.get(AssetKey(["openalex", "bootstrap_download"]))
    assert download.group_name == "openalex_bootstrap"
    assert "parquet" in download.kinds
    assert "s3" in download.kinds

    selection = asset_graph.get(AssetKey(["openalex", "bootstrap_selection"]))
    assert selection.group_name == "openalex_bootstrap"
    assert "postgres" in selection.kinds
    assert "sql" in selection.kinds

    progress = asset_graph.get(AssetKey(["openalex", "bootstrap_progress"]))
    assert progress.group_name == "openalex_bootstrap"
    assert "sensor" in progress.kinds
    assert "postgres" in progress.kinds

    claims = asset_graph.get(AssetKey(["openalex", "batch_claims"]))
    assert claims.group_name == "openalex_queue"
    assert "sensor" in claims.kinds
    assert "postgres" in claims.kinds

    refresh = asset_graph.get(AssetKey(["openalex", "daily_refresh"]))
    assert refresh.group_name == "openalex_ingestion"
    assert "api" in refresh.kinds
    assert "http" in refresh.kinds
