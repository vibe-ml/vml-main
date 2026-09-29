"""Rule-based funding event extraction from English and Russian headlines."""

import hashlib
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date

from src.collection.terms import normalize

EXTRACTOR = "headline-v2"
FLAGS = re.IGNORECASE

# GDELT tokenizes titles: "Lab - on - a - Chip ( LoC )", "$ 25M".
SPACING = [
    (re.compile(r"\s+-\s+"), "-"),
    (re.compile(r"\(\s+"), "("),
    (re.compile(r"\s+\)"), ")"),
    (re.compile(r"«\s+"), "«"),
    (re.compile(r"\s+»"), "»"),
    (re.compile(r"(\d)\s*\.\s*(\d)"), r"\1.\2"),
    (re.compile(r"(\d)\s*,\s*(\d{3})\b"), r"\1\2"),
    (re.compile(r"\s+([,.:;!?%])"), r"\1"),
    (re.compile(r"([$€£¥₹₽])\s+"), r"\1"),
]
CYRILLIC = re.compile(r"[а-яё]", FLAGS)
CURRENCIES = {
    "us$": "USD",
    "$": "USD",
    "usd": "USD",
    "долл": "USD",
    "€": "EUR",
    "eur": "EUR",
    "евро": "EUR",
    "£": "GBP",
    "gbp": "GBP",
    "¥": "JPY",
    "₹": "INR",
    "₽": "RUB",
    "rub": "RUB",
    "руб": "RUB",
}
UNITS = {
    "k": 1e3,
    "thousand": 1e3,
    "тыс": 1e3,
    "m": 1e6,
    "mn": 1e6,
    "mln": 1e6,
    "million": 1e6,
    "млн": 1e6,
    "миллион": 1e6,
    "b": 1e9,
    "bn": 1e9,
    "billion": 1e9,
    "млрд": 1e9,
    "миллиард": 1e9,
}
# Cyrillic series letters that look like Latin ones.
SERIES_LETTERS = {"а": "a", "в": "b", "с": "c", "е": "e"}


@dataclass(frozen=True)
class Rules:
    """Language-specific headline patterns."""

    market_report: re.Pattern[str]
    m_and_a: re.Pattern[str]
    ipo: re.Pattern[str]
    grant: re.Pattern[str]
    round_verb: re.Pattern[str]
    round_noun: re.Pattern[str]
    stages: tuple[tuple[re.Pattern[str], str], ...]
    amounts: tuple[re.Pattern[str], ...]
    subject: re.Pattern[str]
    investors: re.Pattern[str]


ENGLISH = Rules(
    market_report=re.compile(
        r"\b(market|forecast|cagr|market size|industry analysis|outlook)\b"
        r"|\bby 20\d\d\b",
        FLAGS,
    ),
    m_and_a=re.compile(
        r"\b(acquires|acquired by|to acquire|acquisition of|merges with|buys)\b", FLAGS
    ),
    ipo=re.compile(
        r"\b(ipo|initial public offering|goes public|to go public)\b", FLAGS
    ),
    grant=re.compile(r"\b(receives|wins|awarded|secures|lands)\b.*\bgrant\b", FLAGS),
    round_verb=re.compile(
        r"\b(raises|raised|secures|secured|closes|closed|lands|bags|nabs|completes|"
        r"announces|gets|attracts)\b",
        FLAGS,
    ),
    round_noun=re.compile(
        r"\b(funding|round|pre-seed|seed|series [a-h]|investment|financing|capital)\b",
        FLAGS,
    ),
    stages=(
        (re.compile(r"\bpre-seed\b", FLAGS), "pre_seed"),
        (re.compile(r"\bseries ([a-h])\b", FLAGS), "series_{}"),
        (re.compile(r"\bseed\b", FLAGS), "seed"),
        (re.compile(r"\bgrowth (round|equity|funding)\b", FLAGS), "growth"),
        (re.compile(r"\bbridge\b", FLAGS), "bridge"),
    ),
    amounts=(
        re.compile(
            r"(?P<currency>US\$|\$|€|£|¥|₹|₽|USD|EUR|GBP|RUB)\s?"
            r"(?P<number>\d+(?:\.\d+)?)\s?"
            r"(?P<unit>thousand|million|billion|mln|mn|bn|k|m|b)?\b",
            FLAGS,
        ),
    ),
    subject=re.compile(
        r"^(?P<company>.+?)\s+(raises|raised|secures|secured|closes|closed|lands|bags|"
        r"nabs|completes|announces|gets|attracts|acquires|to acquire|buys|merges with|"
        r"goes public|files|receives|wins|is awarded)\b",
        FLAGS,
    ),
    investors=re.compile(r"\bled by (?P<investors>[^,;|()]+)", FLAGS),
)

RUSSIAN = Rules(
    market_report=re.compile(
        r"(объ[её]м рынка|рынок .{0,40}достигнет|прогноз|среднегодов)", FLAGS
    ),
    m_and_a=re.compile(
        r"\b(приобр[её]л[аи]?|купил[аи]?|поглотил[аи]?|покупк[аеиу]|"
        r"приобретени[еяю]|слияни[еяю])\b",
        FLAGS,
    ),
    ipo=re.compile(
        r"(\bipo\b|\bspo\b|выход\w* на биржу|"
        r"разместил[аи]? акции|размещени[еяю] акций)",
        FLAGS,
    ),
    grant=re.compile(
        r"(получил[аи]?|выиграл[аи]?|выделил[аи]?|присудил[аи]?).*грант", FLAGS
    ),
    round_verb=re.compile(
        r"\b(привл[её]к(л[аи])?|получил[аи]?|закрыл[аи]?|инвестировал[аи]?|"
        r"вложил[аи]?|проинвестировал[аи]?)\b",
        FLAGS,
    ),
    round_noun=re.compile(
        r"(инвестици\w*|раунд\w*|финансировани\w*|посевн\w*|сери[июя] [a-hа-в]\b|"
        r"венчурн\w*)",
        FLAGS,
    ),
    stages=(
        (re.compile(r"(pre-seed|пре-?сид|предпосевн)", FLAGS), "pre_seed"),
        (re.compile(r"сери[июя] ([a-hа-в])\b", FLAGS), "series_{}"),
        (re.compile(r"(посевн|\bseed\b)", FLAGS), "seed"),
    ),
    amounts=(
        re.compile(
            r"(?P<number>\d+(?:[.,]\d+)?)\s?"
            r"(?P<unit>тыс|млн|миллион\w*|млрд|миллиард\w*)\.?\s?"
            r"(?P<currency>руб|₽|долл|\$|евро|€)",
            FLAGS,
        ),
        re.compile(
            r"(?P<currency>\$|€|₽)\s?(?P<number>\d+(?:[.,]\d+)?)\s?"
            r"(?P<unit>тыс|млн|млрд)?",
            FLAGS,
        ),
    ),
    subject=re.compile(
        r"^(?P<company>.+?)\s+(привл[её]к(л[аи])?|получил[аи]?|закрыл[аи]?|"
        r"приобр[её]л[аи]?|купил[аи]?|поглотил[аи]?|выш(ел|ла|ли) на|разместил[аи]?|"
        r"выиграл[аи]?)\b",
        FLAGS,
    ),
    investors=re.compile(
        r"(при участии|во главе с|под руководством|от фонда) (?P<investors>[^,;|()]+)",
        FLAGS,
    ),
)


@dataclass(frozen=True)
class Event:
    """One funding event recognized in a headline."""

    event_type: str
    company: str | None
    stage: str | None
    amount: float | None
    currency: str | None
    investors: str | None


def clean(title: str) -> str:
    """Undo source tokenization and drop a trailing site name."""
    text = " ".join(title.split())
    for pattern, replacement in SPACING:
        text = pattern.sub(replacement, text)
    return text.split(" | ")[0].strip()


def prefix_key(value: str, keys: Iterable[str]) -> str | None:
    """Find the key that starts the matched token."""
    lowered = value.lower()
    return next((key for key in keys if lowered.startswith(key)), None)


def amount(text: str, rules: Rules) -> tuple[float | None, str | None]:
    """Parse the first currency amount."""
    for pattern in rules.amounts:
        if match := pattern.search(text):
            unit_key = prefix_key(match["unit"] or "", UNITS) if match["unit"] else None
            currency_key = prefix_key(match["currency"], CURRENCIES)
            number = float(match["number"].replace(",", "."))
            return number * (UNITS[unit_key] if unit_key else 1.0), (
                CURRENCIES[currency_key] if currency_key else None
            )
    return None, None


def company(text: str, rules: Rules) -> str | None:
    """Prefer a «quoted» name; else the subject before the verb."""
    if quoted := re.search(r"«([^»]{2,60})»", text):
        return quoted[1].strip()
    subject = rules.subject.search(text)
    if not subject:
        return None
    # "Exclusive: Acme raises..." keeps the part after the last colon.
    name = subject["company"].rsplit(":", 1)[-1].strip(" '\"") or None
    return name if name and len(name.split()) <= 6 else None


def extract(title: str) -> Event | None:
    """Recognize VC rounds, M&A, IPOs, and grants; ignore market reports."""
    text = clean(title)
    rules = RUSSIAN if CYRILLIC.search(text) else ENGLISH
    if rules.market_report.search(text):
        return None
    if rules.m_and_a.search(text):
        event_type = "m_and_a"
    elif rules.ipo.search(text):
        event_type = "ipo"
    elif rules.grant.search(text):
        event_type = "grant"
    elif rules.round_verb.search(text) and (
        rules.round_noun.search(text) or amount(text, rules)[0] is not None
    ):
        event_type = "vc_round"
    else:
        return None
    stage = None
    if event_type == "vc_round":
        for pattern, label in rules.stages:
            if match := pattern.search(text):
                if "{}" in label:
                    letter = match[1].lower()
                    stage = label.format(SERIES_LETTERS.get(letter, letter))
                else:
                    stage = label
                break
    value, currency = amount(text, rules)
    led = rules.investors.search(text)
    return Event(
        event_type=event_type,
        company=company(text, rules),
        stage=stage,
        amount=value,
        currency=currency,
        investors=led["investors"].strip() if led else None,
    )


def event_id(event: Event, month: date, fallback: str) -> str:
    """Group reprints: same company, type, stage, rounded amount, and month."""
    if event.company is None:
        key = f"article|{fallback}"
    else:
        rounded = None if event.amount is None else round(event.amount, -5)
        key = "|".join(
            str(part)
            for part in (
                event.event_type,
                normalize(event.company),
                event.stage,
                rounded,
                event.currency,
                month.isoformat(),
            )
        )
    return hashlib.sha256(f"{EXTRACTOR}\n{key}".encode()).hexdigest()
