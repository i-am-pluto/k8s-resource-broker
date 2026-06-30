"""Thin wrapper around kubernetes.utils.parse_quantity.

Delegates to the official k8s client library's parser which handles
all standard Kubernetes quantity suffixes (m, K, M, G, T, Ki, Mi, Gi, Ti).
"""

from __future__ import annotations

from kubernetes.utils import parse_quantity as _parse_quantity


def parse_quantity(value: str | int | float, *, is_cpu: bool = False) -> float:
    return float(_parse_quantity(value))
