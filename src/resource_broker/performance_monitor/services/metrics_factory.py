from __future__ import annotations

import httpx
from structlog import get_logger

from resource_broker.common.config import Settings
from resource_broker.performance_monitor.services.metrics_adapter import (
    MetricsAdapter,
    MetricSample,
    PrometheusHttpAdapter,
)

logger = get_logger(__name__)

_KUBECOST_PATH = "/model/prometheus"


class KubecostAdapter(PrometheusHttpAdapter):
    async def query(self, promql: str) -> list[MetricSample]:
        client = await self._get_client()
        try:
            resp = await client.get(f"{_KUBECOST_PATH}/api/v1/query", params={"query": promql})
            resp.raise_for_status()
            return self._parse_result(resp.json())
        except httpx.HTTPStatusError as exc:
            logger.error(
                "kubecost query failed",
                promql=promql,
                status=exc.response.status_code,
                body=exc.response.text,
            )
            return []
        except Exception as exc:
            logger.error("kubecost query error", promql=promql, error=str(exc))
            return []

    async def query_range(self, promql: str, start: float, end: float, step: str = "60s") -> list[MetricSample]:
        client = await self._get_client()
        try:
            resp = await client.get(
                f"{_KUBECOST_PATH}/api/v1/query_range",
                params={"query": promql, "start": start, "end": end, "step": step},
            )
            resp.raise_for_status()
            return self._parse_range_result(resp.json())
        except Exception as exc:
            logger.error("kubecost range query error", promql=promql, error=str(exc))
            return []


_METRICS_ADAPTER_TYPE_MAP: dict[str, type[PrometheusHttpAdapter]] = {
    "prometheus": PrometheusHttpAdapter,
    "thanos": PrometheusHttpAdapter,
    "victoria_metrics": PrometheusHttpAdapter,
    "mimir": PrometheusHttpAdapter,
    "kubecost": KubecostAdapter,
}


def create_metrics_adapter(settings: Settings) -> MetricsAdapter:
    adapter_cls = _METRICS_ADAPTER_TYPE_MAP.get(settings.metrics_adapter_type.value)
    if adapter_cls is None:
        raise ValueError(f"Unknown metrics adapter type: {settings.metrics_adapter_type}")
    return adapter_cls(base_url=settings.metrics_url)
