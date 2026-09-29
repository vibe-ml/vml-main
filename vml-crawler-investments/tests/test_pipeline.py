"""End-to-end investments_job with a social profile and a fake GDELT API."""

from datetime import UTC, date, datetime, timedelta

import httpx
import sqlalchemy as sa

from src.collection.jobs import Runtime, investments_job
from src.collection.terms import normalize
from src.common.settings import Settings
from src.models import social
from src.models.investments import Articles, ArticleTerms, EventArticles, Events, Scans
from tests.conftest import Database

TERMS = [
    ("t-drop", "droplet microfluidics", "display_name"),
    ("t-loc", "lab on a chip", "keyword"),
    ("t-design", "design", "keyword"),
]


def corpus() -> dict[str, list[tuple[datetime, str, str]]]:
    """Articles per phrase: (seen, title, domain)."""
    base = datetime(2025, 2, 20, 12, tzinfo=UTC)
    reprints = [
        (base, "Atrandi Biosciences Raises $25M in Series A Funding", "finsmes.com"),
        (base, "Atrandi Biosciences raises $25 million Series A", "genengnews.com"),
        (base, "Atrandi Biosciences Raises $25M Series A", "arcticstartup.com"),
    ]
    noise = [
        (base - timedelta(days=day), f"New lab-on-a-chip study {day}", "news.com")
        for day in range(1, 10)
    ]
    return {
        "lab on a chip": reprints + noise,
        "droplet microfluidics": [
            (
                datetime(2025, 3, 5, tzinfo=UTC),
                "Fluidix secures €4.5 million seed round led by Acme Ventures",
                "eu-startups.com",
            ),
            (
                datetime(2025, 3, 9, tzinfo=UTC),
                "Droplet Microfluidics Market Forecast To 2030",
                "reports.com",
            ),
        ],
    }


class FakeGdelt:
    """Serve artlist queries from the corpus."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.articles = corpus()
        self.russian = {
            "lab on a chip": [
                (
                    datetime(2025, 3, 3, tzinfo=UTC),
                    "Стартап « Микрофлюидика » привлек 300 млн рублей в раунде серии А",
                    "vc.ru",
                )
            ]
        }

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        params = request.url.params
        phrase = normalize(params["query"].split('"')[1])
        lower = datetime.strptime(params["startdatetime"], "%Y%m%d%H%M%S").replace(
            tzinfo=UTC
        )
        upper = datetime.strptime(params["enddatetime"], "%Y%m%d%H%M%S").replace(
            tzinfo=UTC
        )
        found = sorted(
            (
                item
                for item in (
                    self.russian
                    if "sourcelang:russian" in params["query"]
                    else self.articles
                ).get(phrase, [])
                if lower <= item[0] < upper
            ),
            key=lambda item: item[0],
            reverse=True,
        )[: int(params["maxrecords"])]
        if not found:
            return httpx.Response(200, content=b"{}")
        return httpx.Response(
            200,
            json={
                "articles": [
                    {
                        "url": f"https://{domain}/{abs(hash(title))}",
                        "title": title,
                        "seendate": seen.strftime("%Y%m%dT%H%M%SZ"),
                        "domain": domain,
                        "language": "English",
                        "sourcecountry": "United States",
                    }
                    for seen, title, domain in found
                ]
            },
        )


def seed(db: Database) -> None:
    """Write a social profile the way vml-crawler-social does."""
    engine = sa.create_engine(db.url.get_secret_value())
    with engine.begin() as connection:
        connection = connection.execution_options(
            schema_translate_map={"social_src": db.schema_social}
        )
        connection.execute(
            sa.insert(social.profiles).values(
                id="p1", created_at=datetime(2025, 1, 1, tzinfo=UTC)
            )
        )
        for identity, normalized, origin in TERMS:
            connection.execute(
                sa.insert(social.terms).values(
                    id=identity, term=normalized, normalized=normalized
                )
            )
            connection.execute(
                sa.insert(social.profile_terms).values(
                    profile_id="p1", topic_id="T1", term_id=identity, origin=origin
                )
            )
    engine.dispose()


def run(
    db: Database,
    api: FakeGdelt,
    sleeps: list[float],
    languages: tuple[str, ...] = ("english",),
) -> None:
    """Execute the job with injected transport, clock, and sleep."""
    settings = Settings(
        _env_file=None,
        database_url=db.url,
        schema_investments=db.schema_investments,
        social_schema=db.schema_social,
        alembic_name=db.alembic_name,
        collect_from=date(2025, 1, 1),
        window_cap=10,
        languages=languages,
        retry_seconds=0,
    )
    runtime = Runtime(
        settings,
        httpx.MockTransport(api),
        lambda: datetime(2025, 5, 10, tzinfo=UTC),
        sleeps.append,
        lambda: 0.0,
    )
    assert investments_job.execute_in_process(resources={"runtime": runtime}).success


def test_investments_job_extracts_grouped_events(database: Database) -> None:
    seed(database)
    api, sleeps = FakeGdelt(), []
    run(database, api, sleeps)

    engine = sa.create_engine(database.url.get_secret_value())
    with engine.connect() as raw:
        connection = raw.execution_options(
            schema_translate_map={"investments": database.schema_investments}
        )
        scans = connection.execute(
            sa.select(
                Scans.term_id, Scans.window_start, Scans.window_end, Scans.status
            ).order_by(Scans.id)
        ).all()
        events = {row.company: row for row in connection.execute(sa.select(Events))}
        reprints = connection.scalar(
            sa.select(sa.func.count())
            .select_from(EventArticles)
            .join(Events, Events.id == EventArticles.event_id)
            .where(Events.company == "Atrandi Biosciences")
        )
        articles = connection.scalar(sa.select(sa.func.count()).select_from(Articles))
        strong = connection.scalar(
            sa.select(sa.func.count()).where(ArticleTerms.in_title)
        )
    engine.dispose()

    # Single-word keywords never reach the source.
    assert all("design" not in request.url.params["query"] for request in api.requests)
    assert [(s.term_id, s.window_start, s.window_end, s.status) for s in scans] == [
        ("t-drop", date(2025, 1, 1), date(2025, 5, 1), "complete"),
        ("t-loc", date(2025, 1, 1), date(2025, 5, 1), "split"),
        ("t-loc", date(2025, 1, 1), date(2025, 3, 1), "split"),
        ("t-loc", date(2025, 3, 1), date(2025, 5, 1), "complete"),
        ("t-loc", date(2025, 1, 1), date(2025, 2, 1), "complete"),
        ("t-loc", date(2025, 2, 1), date(2025, 3, 1), "truncated"),
    ]
    # Three reprints form one event; the market report is not an event.
    assert set(events) == {"Atrandi Biosciences", "Fluidix"}
    assert reprints == 3
    atrandi, fluidix = events["Atrandi Biosciences"], events["Fluidix"]
    assert (atrandi.stage, atrandi.amount, atrandi.currency) == (
        "series_a",
        25e6,
        "USD",
    )
    assert (fluidix.stage, fluidix.investors, fluidix.month) == (
        "seed",
        "Acme Ventures",
        date(2025, 3, 1),
    )
    assert articles == 12 and (strong or 0) >= 1
    # Requests are spaced by the GDELT interval.
    assert sleeps and all(delay == 10 for delay in sleeps)

    # A rerun in the same month repeats no search.
    count = len(api.requests)
    run(database, api, sleeps)
    assert len(api.requests) == count


def test_russian_segment_uses_its_own_scans(database: Database) -> None:
    seed(database)
    api = FakeGdelt()
    run(database, api, [], languages=("english", "russian"))
    engine = sa.create_engine(database.url.get_secret_value())
    with engine.connect() as raw:
        connection = raw.execution_options(
            schema_translate_map={"investments": database.schema_investments}
        )
        languages = dict(
            connection.execute(
                sa.select(Scans.language, sa.func.count()).group_by(Scans.language)
            ).all()
        )
        russian = connection.execute(
            sa.select(Events).where(Events.currency == "RUB")
        ).one()
    engine.dispose()
    assert languages["russian"] >= 2 and languages["english"] >= 2
    assert (russian.company, russian.stage, russian.amount) == (
        "Микрофлюидика",
        "series_a",
        3e8,
    )
    assert any("sourcelang:russian" in r.url.params["query"] for r in api.requests)
