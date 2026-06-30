from .auth import AuthProvider
from .prometheus import PrometheusAdapter


class ThanosAdapter(PrometheusAdapter):
    """Thanos is Prometheus-compatible but supports an optional
    deduplication header."""

    def __init__(
        self,
        base_url: str,
        auth: AuthProvider | None = None,
        timeout: float = 30.0,
        dedup: bool = True,
    ) -> None:
        super().__init__(base_url=base_url, auth=auth, timeout=timeout)
        self._dedup = dedup

    async def _build_headers(self) -> dict[str, str]:
        headers = await super()._build_headers()
        if self._dedup:
            headers["Thanos-Prometheus-Receive-Dedup"] = "true"
        return headers
