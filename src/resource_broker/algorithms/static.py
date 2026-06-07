from __future__ import annotations

from typing import Any

from resource_broker.algorithms.base import RecommendationAlgorithm
from resource_broker.common.models.strategy import StrategyResult


class StaticAlgorithm(RecommendationAlgorithm):
    async def compute(
        self,
        field: str,
        config: dict[str, Any],
        context: dict[str, Any] | None = None,
    ) -> StrategyResult:
        value = config.get("value")
        if value is None:
            raise ValueError("StaticAlgorithm requires 'config.value'")
        return StrategyResult(value=value, source="static")
