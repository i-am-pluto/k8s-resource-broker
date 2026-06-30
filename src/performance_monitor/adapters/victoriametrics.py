from datetime import datetime

from .auth import AuthProvider
from .base import Sample
from .prometheus import PrometheusAdapter


class VictoriaMetricsAdapter(PrometheusAdapter):
    """VictoriaMetrics is API-compatible with Prometheus but may host
    the query endpoints at a different path."""

    def __init__(
        self,
        base_url: str,
        auth: AuthProvider | None = None,
        timeout: float = 30.0,
        query_path: str = "/api/v1/query",
        query_range_path: str = "/api/v1/query_range",
    ) -> None:
        super().__init__(base_url=base_url, auth=auth, timeout=timeout)
        self._query_path = query_path
        self._query_range_path = query_range_path

    async def query(
        self,
        promql: str,
        time: datetime | None = None,
    ) -> list[Sample]:
        params: dict[str, str] = {"query": promql}
        if time is not None:
            params["time"] = str(time.timestamp())
        return await self._get(self._query_path, params)

    async def query_range(
        self,
        promql: str,
        start: datetime,
        end: datetime,
        step: str = "30s",
    ) -> list[Sample]:
        params = {
            "query": promql,
            "start": str(start.timestamp()),
            "end": str(end.timestamp()),
            "step": step,
        }
        return await self._get(self._query_range_path, params)
