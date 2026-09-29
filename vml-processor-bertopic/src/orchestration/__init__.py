"""Dagster wiring for the five-stage discovery pipeline.

Stage modules under ``src.corpus``, ``src.embeddings``, ``src.topics``,
``src.labels``, and ``src.emergence`` stay free of Dagster imports. This package
owns assets, the job, and durable-store adopt helpers only.
"""

from src.orchestration.defs import defs

__all__ = ["defs"]
