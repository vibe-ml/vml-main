"""End-to-end social_job against PostgreSQL with fake platform APIs."""

from datetime import UTC, date, datetime

import httpx
import sqlalchemy as sa

from src.collection.jobs import Runtime, social_job
from src.common.settings import Settings
from src.models import openalex
from src.models.social import Attention, Mentions, Scans, Terms, Volumes
from tests.conftest import Database
from tests.fakes import FakeApis, Post, month_posts

TOPICS = [
    {
        "id": "https://openalex.org/T1",
        "display_name": "Microfluidics",
        "keywords": ["Lab-on-a-Chip"],
        "subfield": {"display_name": "Biomedical Engineering"},
        "field": {"display_name": "Engineering"},
        "domain": {"display_name": "Physical Sciences"},
    },
    {
        "id": "https://openalex.org/T2",
        "display_name": "Soft Robotics",
        "keywords": ["Soft actuators", "AB"],
    },
    {"id": "https://openalex.org/T3", "display_name": "Unused Topic", "keywords": []},
]


def seed_openalex(db: Database) -> None:
    """Write a taxonomy bundle and in-scope works the way the crawler does."""
    engine = sa.create_engine(db.url.get_secret_value())
    with engine.begin() as connection:
        connection = connection.execution_options(
            schema_translate_map={"openalex_raw": db.schema_openalex}
        )
        connection.execute(
            sa.insert(openalex.taxonomy_bundles).values(
                id="bundle-1", records={"topics": TOPICS}
            )
        )
        connection.execute(
            sa.insert(openalex.taxonomy_observations).values(
                sequence=1,
                bundle_id="bundle-1",
                ended_at=datetime(2024, 5, 1, tzinfo=UTC),
            )
        )
        works = [("W1", "T1"), ("W2", "T1"), ("W3", "T1"), ("W4", "T2")]
        for entity, topic in works:
            connection.execute(
                sa.insert(openalex.work_versions).values(
                    id=f"v-{entity}",
                    payload={"primary_topic": {"id": f"https://openalex.org/{topic}"}},
                )
            )
            connection.execute(
                sa.insert(openalex.work_current).values(
                    scope_id="scope", entity_id=entity, version_id=f"v-{entity}"
                )
            )
    engine.dispose()


def corpus() -> list[Post]:
    """Posts that force splits, a truncated month, and cross-term duplicates."""
    return (
        month_posts(1000, 2024, 2, 150, "Show HN: my lab-on-a-chip build")
        + month_posts(2000, 2024, 1, 5, "New microfluidics paper")
        + [
            Post(
                3000,
                datetime(2024, 1, 20, tzinfo=UTC),
                "microfluidics and lab on a chip",
            )
        ]
        + month_posts(4000, 2024, 3, 2, "Soft actuators for grippers", "bob")
        + [Post(5000, datetime(2024, 3, 5, tzinfo=UTC), "soft robots are soft")]
    )


def run(db: Database, apis: FakeApis, today: datetime) -> None:
    """Execute the job in process with injected transport and clock."""
    settings = Settings(
        _env_file=None,
        database_url=db.url,
        schema_social=db.schema_social,
        openalex_schema_raw=db.schema_openalex,
        alembic_name=db.alembic_name,
        topic_limit=2,
        stackexchange_sites=("stackoverflow",),
        window_cap=100,
        retry_seconds=0,
    )
    runtime = Runtime(
        settings, httpx.MockTransport(apis), lambda: today, lambda _: None
    )
    result = social_job.execute_in_process(resources={"runtime": runtime})
    assert result.success


def test_social_job_collects_and_aggregates(database: Database) -> None:
    seed_openalex(database)
    apis = FakeApis(corpus())
    run(database, apis, datetime(2024, 5, 15, tzinfo=UTC))

    engine = sa.create_engine(database.url.get_secret_value())
    with engine.connect() as raw:
        connection = raw.execution_options(
            schema_translate_map={"social": database.schema_social}
        )
        # Topic selection follows OpenAlex work counts; short keywords are dropped.
        assert sorted(connection.scalars(sa.select(Terms.normalized))) == [
            "lab on a chip",
            "microfluidics",
            "soft actuators",
            "soft robotics",
        ]
        lab = connection.execute(
            sa.select(
                Scans.window_start, Scans.window_end, Scans.status, Scans.reported_total
            )
            .join(Terms, Terms.id == Scans.term_id)
            .where(Terms.normalized == "lab on a chip", Scans.platform == "hackernews")
            .order_by(Scans.id)
        ).all()
        assert [(row.window_start, row.window_end, row.status) for row in lab] == [
            (date(2024, 1, 1), date(2024, 5, 1), "split"),
            (date(2024, 1, 1), date(2024, 3, 1), "split"),
            (date(2024, 3, 1), date(2024, 5, 1), "complete"),
            (date(2024, 1, 1), date(2024, 2, 1), "complete"),
            (date(2024, 2, 1), date(2024, 3, 1), "truncated"),
        ]
        assert lab[-1].reported_total == 150
        # The "soft" post lacks the full phrase and is never stored.
        assert (
            connection.scalar(
                sa.select(sa.func.count())
                .select_from(Mentions)
                .where(Mentions.native_id == "5000")
            )
            == 0
        )
        assert (
            connection.scalar(
                sa.select(sa.func.count())
                .select_from(Volumes)
                .where(Volumes.platform == "hackernews")
            )
            == 4
        )
        attention = {
            (row.topic_id[-2:], row.platform, row.month.month): row
            for row in connection.execute(sa.select(Attention))
        }
    engine.dispose()

    january = attention[("T1", "hackernews", 1)]
    assert (january.mentions, january.coverage) == (6, "complete")
    assert january.share == 6 / 5000
    february = attention[("T1", "hackernews", 2)]
    assert (february.mentions, february.estimated, february.coverage) == (
        100,
        150,
        "truncated",
    )
    march = attention[("T2", "stackexchange", 3)]
    assert (march.mentions, march.authors, march.coverage) == (2, 1, "complete")
    assert march.share == 2 / 800
    # Missing credentials yield missing coverage, never zero attention.
    bluesky = attention[("T1", "bluesky", 1)]
    assert (bluesky.coverage, bluesky.estimated) == ("missing", None)

    # A rerun in the same month plans nothing and repeats no search.
    searches = len(apis.requests)
    run(database, apis, datetime(2024, 5, 20, tzinfo=UTC))
    assert len(apis.requests) == searches

    # A new month adds one root window per term and channel.
    run(database, apis, datetime(2024, 6, 2, tzinfo=UTC))
    assert len(apis.requests) > searches
