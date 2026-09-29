"""Smoke checks that the machine-learning stack runs on this machine."""

from __future__ import annotations

import uuid
from pathlib import Path

import httpx
import numpy as np
import pytest
import structlog
from alembic import command
from alembic.config import Config
from bertopic import BERTopic
from hdbscan import HDBSCAN
from pydantic_settings import BaseSettings, SettingsConfigDict
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    column,
    create_engine,
    select,
    table,
)
from umap import UMAP

_SPIKE_DIR = Path(__file__).resolve().parents[1] / "spikes" / "ml_runtime"

log = structlog.get_logger()

_PROBE_VECTOR = [1.0, 0.0, 0.0, 0.0]


class SmokeSettings(BaseSettings):
    """Local service URLs for the runtime smoke check."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    pwf_test_database_url: str
    pwf_test_qdrant_url: str


_DOCUMENTS = (
    "quantum lattice superconductivity copper oxide",
    "quantum lattice superconductivity copper oxide phase",
    "quantum lattice superconductivity copper oxide gap",
    "quantum lattice superconductivity copper oxide doping",
    "coral reef bleaching ocean temperature",
    "coral reef bleaching ocean temperature stress",
    "coral reef bleaching ocean temperature recovery",
    "coral reef bleaching ocean temperature algae",
)


def _synthetic_embeddings() -> np.ndarray:
    """Return two tight groups of precomputed vectors from a pinned generator."""
    generator = np.random.default_rng(42)
    first = generator.normal(loc=0.0, scale=0.01, size=(4, 8))
    second = generator.normal(loc=5.0, scale=0.01, size=(4, 8))
    return np.vstack((first, second))


def test_bertopic_fits_synthetic_corpus_with_pinned_seed() -> None:
    """A BERTopic fit on precomputed vectors completes and repeats under seed 42."""
    embeddings = _synthetic_embeddings()
    first = _fit_topics(_DOCUMENTS, embeddings)
    second = _fit_topics(_DOCUMENTS, embeddings)

    assert len(first) == len(_DOCUMENTS)
    assert first == second


def _fit_topics(documents: tuple[str, ...], embeddings: np.ndarray) -> tuple[int, ...]:
    """Fit BERTopic with a pinned UMAP seed and return one topic id per document."""
    model = BERTopic(
        umap_model=UMAP(
            n_neighbors=2,
            n_components=2,
            min_dist=0.0,
            metric="cosine",
            random_state=42,
            n_jobs=1,
        ),
        hdbscan_model=HDBSCAN(min_cluster_size=2, min_samples=1, prediction_data=True),
        calculate_probabilities=False,
        verbose=False,
    )
    log.info("bertopic_smoke_fit", documents=len(documents))
    topics, _probabilities = model.fit_transform(list(documents), embeddings=embeddings)
    return tuple(int(topic) for topic in topics)


def test_qdrant_round_trip_upserts_and_reads_a_point() -> None:
    """An HTTP client reaches Qdrant, and an upserted point reads back."""
    settings = SmokeSettings()
    health = httpx.get(f"{settings.pwf_test_qdrant_url}/healthz", timeout=5.0)
    assert health.status_code == 200

    collection = f"smoke-{uuid.uuid4()}"
    client = QdrantClient(url=settings.pwf_test_qdrant_url)
    created = False
    try:
        client.create_collection(
            collection_name=collection,
            vectors_config=VectorParams(
                size=len(_PROBE_VECTOR), distance=Distance.COSINE
            ),
        )
        created = True
        client.upsert(
            collection_name=collection,
            points=[PointStruct(id=1, vector=_PROBE_VECTOR, payload={"probe": "ok"})],
        )
        points = client.retrieve(
            collection_name=collection,
            ids=[1],
            with_payload=True,
            with_vectors=True,
        )
        assert len(points) == 1
        assert points[0].payload == {"probe": "ok"}
        assert list(points[0].vector) == pytest.approx(_PROBE_VECTOR)
    finally:
        if created:
            client.delete_collection(collection_name=collection)


def test_alembic_migration_applies_on_postgresql() -> None:
    """SQLAlchemy reads a row inserted by an Alembic migration on PostgreSQL."""
    settings = SmokeSettings()
    config = Config(str(_SPIKE_DIR / "alembic.ini"))
    # ConfigParser treats "%" as interpolation, so a percent-encoded password must be escaped.
    config.set_main_option(
        "sqlalchemy.url", settings.pwf_test_database_url.replace("%", "%%")
    )
    engine = create_engine(settings.pwf_test_database_url)
    smoke_probe = table("smoke_probe", column("probe_value", Integer()))
    try:
        command.upgrade(config, "head")
        with engine.connect() as connection:
            probe_value = connection.execute(
                select(smoke_probe.c.probe_value)
            ).scalar_one()
        assert probe_value == 1
    finally:
        command.downgrade(config, "base")
        version_table = Table(
            "alembic_version_smoke",
            MetaData(),
            Column("version_num", String(32), nullable=False),
        )
        version_table.drop(bind=engine, checkfirst=True)
        engine.dispose()
