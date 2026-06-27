from __future__ import annotations

from typing import Any

from structlog import get_logger

from resource_broker.algorithms.base import RecommendationAlgorithm
from resource_broker.common.dao.database import get_session
from resource_broker.common.dao.repositories.metrics import MetricsRepository
from resource_broker.common.models.strategy import StrategyResult

logger = get_logger(__name__)


class PercentileAlgorithm(RecommendationAlgorithm):
    PERCENTILE_MAP: dict[int, str] = {
        50: "cpu_p50",
        75: "cpu_p75",
        90: "cpu_p90",
        95: "cpu_p95",
        99: "cpu_p99",
    }
    MEM_PERCENTILE_MAP: dict[int, str] = {
        50: "mem_p50",
        75: "mem_p75",
        90: "mem_p90",
        95: "mem_p95",
        99: "mem_p99",
    }

    async def compute(
        self,
        field: str,
        config: dict[str, Any],
        context: dict[str, Any] | None = None,
    ) -> StrategyResult:
        if not context or "profile_name" not in context:
            return StrategyResult(value=None, source="percentile-no-context")

        percentile = config.get("percentile", 75)
        lookback_hours = config.get("lookback_hours", 24)
        min_val = config.get("min")
        max_val = config.get("max")

        async with get_session() as session:
            metrics_repo = MetricsRepository(session)
            percentiles = await metrics_repo.get_percentiles(context["profile_name"], lookback_hours=lookback_hours)

        pmap = self.MEM_PERCENTILE_MAP if "mem" in field.lower() or "memory" in field.lower() else self.PERCENTILE_MAP
        key = pmap.get(percentile)
        value = percentiles.get(key) if key else None

        if value is None:
            logger.debug("no percentile data available, returning None", profile=context["profile_name"], p=percentile)
            return StrategyResult(value=None, source="percentile-no-data")

        if min_val is not None:
            value = max(value, _parse_resource_value(min_val, field=field))
        if max_val is not None:
            value = min(value, _parse_resource_value(max_val, field=field))

        return StrategyResult(value=value, source=f"percentile-p{percentile}")


def _parse_resource_value(raw: str | int | float, field: str = "") -> float:
    if isinstance(raw, (int, float)):
        return float(raw)

    if "cpu" in field.lower():
        raw = str(raw).lower().rstrip(" ")
        if raw.endswith("m"):
            return float(raw[:-1]) / 1000.0
        return float(raw)

    raw = str(raw).upper().strip()
    multipliers = {"K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "KI": 1024, "MI": 1024**2, "GI": 1024**3, "TI": 1024**4}
    for suffix, mult in sorted(multipliers.items(), key=lambda x: -len(x[0])):
        if raw.endswith(suffix):
            try:
                return float(raw[: -len(suffix)]) * mult
            except ValueError:
                pass
    return float(raw)
