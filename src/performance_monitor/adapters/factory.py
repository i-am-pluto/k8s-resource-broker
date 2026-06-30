from typing import Any

from .auth import AuthProvider
from .base import PromQLAdapter
from .prometheus import PrometheusAdapter
from .thanos import ThanosAdapter
from .victoriametrics import VictoriaMetricsAdapter


def create_adapter(
    backend: str,
    base_url: str,
    auth: AuthProvider | None = None,
    **kwargs: Any,
) -> PromQLAdapter:
    backends: dict[str, type[PrometheusAdapter | VictoriaMetricsAdapter | ThanosAdapter]] = {
        "prometheus": PrometheusAdapter,
        "victoriametrics": VictoriaMetricsAdapter,
        "thanos": ThanosAdapter,
    }
    cls = backends.get(backend)
    if cls is None:
        msg = f"Unknown PromQL backend: {backend!r} (choices: {', '.join(backends)})"
        raise ValueError(msg)
    return cls(base_url=base_url, auth=auth, **kwargs)
