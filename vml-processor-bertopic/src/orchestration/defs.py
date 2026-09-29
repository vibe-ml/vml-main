"""Dagster Definitions for the processor code location."""

from __future__ import annotations

import dagster as dg
from qdrant_client import QdrantClient

from src.common.db import create_processor_engine
from src.common.log import configure_logging
from src.common.settings import Settings
from src.embeddings import HttpxEmbeddingClient, ParallelEmbeddingClient
from src.labels import HttpxLabelingClient
from src.orchestration.assets import discovery_assets, discovery_topics_job
from src.orchestration.ports import PipelinePorts


def build_ports(settings: Settings | None = None) -> PipelinePorts:
    """Build production ports from settings; embedding and labeling call remote hosts."""
    configure_logging(json=True)
    active = settings or Settings()
    embedding_config = active.embedding_config()
    if not active.embedder_api_urls:
        raise ValueError(
            "PWF_EMBEDDER_API_URLS must list at least one embedding endpoint"
        )
    endpoints = [
        (url, HttpxEmbeddingClient(url, active.embedding_model))
        for url in active.embedder_api_urls
    ]
    embedding_client = ParallelEmbeddingClient.from_config(endpoints, embedding_config)
    labeling_client = HttpxLabelingClient(
        active.labeling_api_url,
        active.labeling_api_key,
        active.labeling_model,
    )
    return PipelinePorts(
        settings=active,
        engine=create_processor_engine(
            active.database_url, upstream_schema=active.upstream_schema
        ),
        qdrant=QdrantClient(url=active.qdrant_url, timeout=120.0),
        embedding_client=embedding_client,
        labeling_client=labeling_client,
        tokenizer=active.labeling_tokenizer(),
        fitter=None,
    )


@dg.resource
def ports_resource(_init_context: dg.InitResourceContext) -> PipelinePorts:
    """Resolve settings and clients inside the run process."""
    return build_ports()


defs = dg.Definitions(
    assets=discovery_assets,
    jobs=[discovery_topics_job],
    resources={"ports": ports_resource},
)
