import csv
from datetime import date
from pathlib import Path

import pytest

from medallion_ingest.scheduling import FREQUENCIES, is_due, occurrences

SEED = Path(__file__).parents[1] / "dbt" / "seeds" / "activity_frequencies.csv"


def test_python_rules_match_dbt_seed():
    with SEED.open(newline="") as handle:
        seed = {row["frequency"]: (row["cadence_unit"], int(row["cadence_interval"])) for row in csv.DictReader(handle)}
    assert seed == FREQUENCIES


@pytest.mark.parametrize(
    ("frequency", "start", "day", "expected"),
    [
        ("one_time", date(2026, 3, 10), date(2026, 3, 10), True),
        ("one_time", date(2026, 3, 10), date(2026, 3, 11), False),
        ("daily", date(2026, 3, 10), date(2026, 3, 9), False),
        ("weekly", date(2026, 3, 10), date(2026, 3, 24), True),
        ("biweekly", date(2026, 3, 10), date(2026, 3, 17), False),
        ("monthly", date(2026, 1, 31), date(2026, 2, 28), True),  # clamped to month end
        ("monthly", date(2026, 1, 31), date(2026, 3, 31), True),
        ("quarterly", date(2026, 1, 15), date(2026, 4, 15), True),
        ("quarterly", date(2026, 1, 15), date(2026, 3, 15), False),
        ("annual", date(2024, 2, 29), date(2025, 2, 28), True),
        ("biennial", date(2024, 5, 1), date(2025, 5, 1), False),
    ],
)
def test_is_due(frequency, start, day, expected):
    assert is_due(frequency, start, day) is expected


def test_end_date_stops_recurrence():
    assert not is_due("daily", date(2026, 1, 1), date(2026, 2, 1), end=date(2026, 1, 31))


def test_occurrences_respect_window():
    days = occurrences("weekly", date(2026, 1, 5), window_start=date(2026, 2, 1), window_end=date(2026, 2, 28))
    assert days == [date(2026, 2, 2), date(2026, 2, 9), date(2026, 2, 16), date(2026, 2, 23)]
