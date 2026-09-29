"""Headline extraction rules on real-world title shapes."""

from datetime import date

import pytest

from src.collection.extraction import Event, clean, event_id, extract


def test_clean_undoes_gdelt_tokenization() -> None:
    assert clean("Lab - on - a - Chip ( LoC ) test raises $ 2 . 5M | Site") == (
        "Lab-on-a-Chip (LoC) test raises $2.5M"
    )


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        (
            "Atrandi Biosciences Raises $25M in Series A Funding",
            Event("vc_round", "Atrandi Biosciences", "series_a", 25e6, "USD", None),
        ),
        (
            "Exclusive: Fluidix secures €4.5 million seed round led by Acme Ventures",
            Event("vc_round", "Fluidix", "seed", 4.5e6, "EUR", "Acme Ventures"),
        ),
        (
            "Stratasys to acquire Desktop Metal for $1.8 billion",
            Event("m_and_a", "Stratasys", None, 1.8e9, "USD", None),
        ),
        (
            "Startup X receives NIH grant to develop organ-on-chip",
            Event("grant", "Startup X", None, None, None, None),
        ),
        (
            "Guardrails AI files for IPO on Nasdaq",
            Event("ipo", "Guardrails AI", None, None, None, None),
        ),
    ],
)
def test_extract_events(title: str, expected: Event) -> None:
    assert extract(title) == expected


@pytest.mark.parametrize(
    "title",
    [
        "7 . 2 % + growth for High Throughput Screening Market Size raising to USD 18 . 33 Bn by 2025",  # noqa: E501
        "Purdue printing innovation fabricates multilevel microfluidic devices",
        "Lab-on-a-Chip COVID-19 Test Advances to Clinical Trials",
    ],
)
def test_non_events_are_ignored(title: str) -> None:
    assert extract(title) is None


def test_reprints_share_one_event_id() -> None:
    first = extract("Atrandi Biosciences Raises $25M in Series A Funding")
    second = extract("Atrandi Biosciences raises $25 million Series A")
    assert first and second
    month = date(2025, 2, 1)
    assert event_id(first, month, "a") == event_id(second, month, "b")
    assert event_id(first, month, "a") != event_id(first, date(2025, 3, 1), "a")


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        (
            "Стартап « Микрофлюидика » привлек 300 млн рублей в раунде серии А",
            Event("vc_round", "Микрофлюидика", "series_a", 3e8, "RUB", None),
        ),
        (
            "Компания «Робоглаз» закрыла посевной раунд на $1,5 млн при участии ФРИИ",
            Event("vc_round", "Робоглаз", "seed", 1.5e6, "USD", "ФРИИ"),
        ),
        (
            "Фонд Сколково: компания «Биочип» получила грант 5 млн руб.",
            Event("grant", "Биочип", None, 5e6, "RUB", None),
        ),
        (
            "Сбер приобрел разработчика 3D-принтеров «Принтум»",
            Event("m_and_a", "Принтум", None, None, None, None),
        ),
        (
            "«Промобот» разместил акции на Мосбирже",
            Event("ipo", "Промобот", None, None, None, None),
        ),
    ],
)
def test_extract_russian_events(title: str, expected: Event) -> None:
    assert extract(title) == expected


@pytest.mark.parametrize(
    "title",
    [
        "Объем рынка 3D-печати в России достигнет 10 млрд рублей к 2030 году",
        "Магазины закупили новые принтеры",
        "Учёные создали микрофлюидный чип для диагностики",
    ],
)
def test_russian_non_events_are_ignored(title: str) -> None:
    assert extract(title) is None
