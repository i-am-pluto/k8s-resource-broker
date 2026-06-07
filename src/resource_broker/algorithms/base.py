from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from resource_broker.common.models.strategy import StrategyResult


class RecommendationAlgorithm(ABC):
    @abstractmethod
    async def compute(
        self,
        field: str,
        config: dict[str, Any],
        context: dict[str, Any] | None = None,
    ) -> StrategyResult: ...
