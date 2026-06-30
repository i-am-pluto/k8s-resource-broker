from __future__ import annotations

import pytest

from resource_broker.common.k8s_units import parse_quantity


def test_parse_cpu_millicores() -> None:
    assert parse_quantity("500m", is_cpu=True) == 0.5


def test_parse_cpu_plain() -> None:
    assert parse_quantity("2", is_cpu=True) == 2.0


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1K", 1e3),
        ("1M", 1e6),
        ("1G", 1e9),
        ("1T", 1e12),
        ("1Ki", 1024),
        ("1Mi", 1024**2),
        ("1Gi", 1024**3),
        ("1Ti", 1024**4),
    ],
)
def test_parse_memory_suffixes(raw: str, expected: float) -> None:
    assert parse_quantity(raw) == expected


def test_parse_memory_plain_bytes() -> None:
    assert parse_quantity("1024") == 1024.0


def test_parse_numeric_passthrough() -> None:
    assert parse_quantity(2) == 2.0
    assert parse_quantity(0.5) == 0.5


def test_parse_numeric_passthrough_ignores_is_cpu() -> None:
    assert parse_quantity(3, is_cpu=True) == 3.0
