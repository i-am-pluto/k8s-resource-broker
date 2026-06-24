from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import httpx
from structlog import get_logger

from resource_broker.common.config import settings

logger = get_logger(__name__)


@dataclass
class MetricSample:
    metric: dict[str, str] = field(default_factory=dict)
    value: float = 0.0
    timestamp: float | None = None
    values: list[tuple[float, float]] | None = None


class MetricsAdapter(ABC):
    @abstractmethod
    async def query(self, promql: str) -> list[MetricSample]: ...

    @abstractmethod
    async def query_range(self, promql: str, start: float, end: float, step: str = "60s") -> list[MetricSample]: ...

    @abstractmethod
    async def close(self) -> None: ...


class PrometheusHttpAdapter(MetricsAdapter):
    def __init__(self, base_url: str = "") -> None:
        self._base_url = base_url or settings.metrics_url
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=httpx.Timeout(settings.metrics_timeout_seconds),
            )
        return self._client

    async def query(self, promql: str) -> list[MetricSample]:
        client = await self._get_client()
        try:
            resp = await client.get("/api/v1/query", params={"query": promql})
            resp.raise_for_status()
            return self._parse_result(resp.json())
        except httpx.HTTPStatusError as exc:
            logger.error("metrics query failed", promql=promql, status=exc.response.status_code, body=exc.response.text)
            return []
        except Exception as exc:
            logger.error("metrics query error", promql=promql, error=str(exc))
            return []

    async def query_range(self, promql: str, start: float, end: float, step: str = "60s") -> list[MetricSample]:
        client = await self._get_client()
        try:
            resp = await client.get(
                "/api/v1/query_range",
                params={"query": promql, "start": start, "end": end, "step": step},
            )
            resp.raise_for_status()
            return self._parse_range_result(resp.json())
        except Exception as exc:
            logger.error("metrics range query error", promql=promql, error=str(exc))
            return []

    @staticmethod
    def _parse_result(body: dict[str, Any]) -> list[MetricSample]:
        results = body.get("data", {}).get("result", [])
        samples = []
        for r in results:
            metric = r.get("metric", {})
            val_raw = r.get("value", [0, 0])
            samples.append(
                MetricSample(
                    metric=metric,
                    value=float(val_raw[1]) if len(val_raw) > 1 else 0.0,
                    timestamp=float(val_raw[0]) if val_raw else None,
                )
            )
        return samples

    @staticmethod
    def _parse_range_result(body: dict[str, Any]) -> list[MetricSample]:
        results = body.get("data", {}).get("result", [])
        samples = []
        for r in results:
            metric = r.get("metric", {})
            values = r.get("values", [])
            if values:
                samples.append(
                    MetricSample(
                        metric=metric,
                        value=float(values[-1][1]),
                        timestamp=float(values[-1][0]),
                        values=[(float(ts), float(v)) for ts, v in values],
                    )
                )
        return samples

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
