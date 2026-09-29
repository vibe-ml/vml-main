"""Calendar-month windows: [start, end) with first-of-month boundaries."""

from collections.abc import Iterator
from datetime import UTC, date, datetime


def month_start(value: date) -> date:
    """Return the first day of the value's month."""
    return value.replace(day=1)


def add_months(value: date, months: int) -> date:
    """Shift a first-of-month date by whole months."""
    index = value.year * 12 + value.month - 1 + months
    return date(index // 12, index % 12 + 1, 1)


def months_between(start: date, end: date) -> int:
    """Count whole months in a first-of-month window."""
    return (end.year - start.year) * 12 + end.month - start.month


def months(start: date, end: date) -> Iterator[date]:
    """Iterate month starts inside a window."""
    for offset in range(months_between(start, end)):
        yield add_months(start, offset)


def split(start: date, end: date) -> tuple[tuple[date, date], tuple[date, date]]:
    """Halve a multi-month window on a month boundary."""
    count = months_between(start, end)
    if count < 2:
        raise ValueError("Cannot split a single-month window")
    middle = add_months(start, count // 2)
    return (start, middle), (middle, end)


def timestamp(value: date) -> int:
    """Convert a UTC midnight date to Unix seconds."""
    return int(datetime(value.year, value.month, value.day, tzinfo=UTC).timestamp())
