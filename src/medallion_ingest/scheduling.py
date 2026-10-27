"""Maintenance-plan recurrence rules.

This is the Python twin of ``dbt/seeds/activity_frequencies.csv`` and the
``expected_activities`` CTE of ``fact_maintenance_activities``. The generator
uses it to emit realistic scheduled activities; a unit test keeps both
definitions in sync.

Every frequency reduces to one of three cadence units:

* ``once``  - due only on the plan start date
* ``day``   - due every *n* days from the start date
* ``month`` - due every *n* months on the start day-of-month, clamped to the
  last day of shorter months (a plan starting Jan 31 is due Feb 28/29)
"""

from __future__ import annotations

import calendar
from datetime import date, timedelta

FREQUENCIES: dict[str, tuple[str, int]] = {
    "one_time": ("once", 0),
    "daily": ("day", 1),
    "weekly": ("day", 7),
    "biweekly": ("day", 14),
    "monthly": ("month", 1),
    "bimonthly": ("month", 2),
    "quarterly": ("month", 3),
    "every_four_months": ("month", 4),
    "semiannual": ("month", 6),
    "annual": ("month", 12),
    "biennial": ("month", 24),
    "triennial": ("month", 36),
    "quadrennial": ("month", 48),
    "quinquennial": ("month", 60),
}


def is_due(frequency: str, start: date, day: date, end: date | None = None) -> bool:
    """Return True when a plan with ``frequency`` starting on ``start`` is due on ``day``."""
    if day < start or (end is not None and day > end):
        return False

    unit, interval = FREQUENCIES[frequency]
    if unit == "once":
        return day == start
    if unit == "day":
        return (day - start).days % interval == 0

    month_diff = (day.year * 12 + day.month) - (start.year * 12 + start.month)
    last_day_of_month = calendar.monthrange(day.year, day.month)[1]
    return month_diff % interval == 0 and day.day == min(start.day, last_day_of_month)


def occurrences(
    frequency: str,
    start: date,
    window_start: date,
    window_end: date,
    end: date | None = None,
) -> list[date]:
    """List every due date of a plan inside ``[window_start, window_end]``."""
    first = max(start, window_start)
    days = (window_end - first).days + 1
    candidates = (first + timedelta(days=offset) for offset in range(max(days, 0)))
    return [day for day in candidates if is_due(frequency, start, day, end)]
