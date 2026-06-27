from __future__ import annotations

import re

_DURATION_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*([smhdw]?)$", re.IGNORECASE)
_UNIT_TO_HOURS: dict[str, float] = {
    "":  1.0,       # bare number → treat as hours
    "s": 1 / 3600,
    "m": 1 / 60,
    "h": 1.0,
    "d": 24.0,
    "w": 168.0,
}
_UNIT_TO_MINUTES: dict[str, float] = {
    "":  1.0,       # bare number → treat as minutes
    "s": 1 / 60,
    "m": 1.0,
    "h": 60.0,
    "d": 1440.0,
    "w": 10080.0,
}


def _parse(s: str | int | float, unit_map: dict[str, float]) -> float:
    if isinstance(s, (int, float)):
        return float(s)
    m = _DURATION_RE.match(str(s).strip())
    if not m:
        raise ValueError(f"Cannot parse duration string: {s!r}")
    value = float(m.group(1))
    unit = m.group(2).lower()
    return value * unit_map[unit]


def parse_duration_to_hours(s: str | int | float) -> float:
    """Parse a duration string into fractional hours.

    Accepted: "24h", "7d", "1w", "30m", "3600s", "168" (bare = hours).
    Used for metric lookback windows (coolback-period).
    """
    return _parse(s, _UNIT_TO_HOURS)


def parse_duration_to_minutes(s: str | int | float) -> float:
    """Parse a duration string into fractional minutes.

    Accepted: "360m", "6h", "1d", "3600s", "30" (bare = minutes).
    Used for schedule cadences (run-every) where minute granularity matters.
    """
    return _parse(s, _UNIT_TO_MINUTES)
