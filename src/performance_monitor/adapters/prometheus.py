from datetime import datetime

import httpx

from .auth import AuthProvider, NoAuth
from .base import PromQLAdapter, Sample


class PrometheusAdapter(PromQLAdapter):
    def __init__(
        self,
        base_url: str,
        auth: AuthProvider | None = None,
        timeout: float = 30.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._auth = auth or NoAuth()
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(timeout))

    async def _build_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        await self._auth.apply_headers(headers)
        return headers

    async def _get(
        self,
        endpoint: str,
        params: dict[str, str],
    ) -> list[Sample]:
        headers = await self._build_headers()

        resp = await self._client.get(
            f"{self._base_url}{endpoint}",
            params=params,
            headers=headers,
        )
        resp.raise_for_status()
        body = resp.json()

        if body.get("status") != "success":
            msg = f"Prometheus query failed: {body.get('error', 'unknown error')}"
            raise RuntimeError(msg)

        results: list[Sample] = []
        for result in body["data"]["result"]:
            labels = result.get("metric", {})
            raw = result.get("values") or [result.get("value")]
            for v in raw:
                ts = datetime.fromtimestamp(float(v[0]))
                results.append(Sample(value=float(v[1]), timestamp=ts, labels=labels))

        return results

    async def query(
        self,
        promql: str,
        time: datetime | None = None,
    ) -> list[Sample]:
        params: dict[str, str] = {"query": promql}
        if time is not None:
            params["time"] = str(time.timestamp())
        return await self._get("/api/v1/query", params)

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
        return await self._get("/api/v1/query_range", params)

    async def health(self) -> bool:
        try:
            headers = await self._build_headers()
            resp = await self._client.get(
                f"{self._base_url}/api/v1/query",
                params={"query": "1"},
                headers=headers,
            )
            return resp.status_code == 200
        except Exception:
            return False

    async def close(self) -> None:
        await self._client.aclose()
