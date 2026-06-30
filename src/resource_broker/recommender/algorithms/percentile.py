from __future__ import annotations

from typing import Any

from structlog import get_logger

from resource_broker.common.models.strategy import StrategyResult
from resource_broker.recommender.algorithms.base import RecommendationAlgorithm

logger = get_logger(__name__)


class PercentileAlgorithm(RecommendationAlgorithm):
    PERCENTILE_MAP: dict[int, str] = {50: "cpu_p50", 75: "cpu_p75", 90: "cpu_p90", 95: "cpu_p95"}

    async def compute(
        self,
        field: str,
        config: dict[str, Any],
        context: dict[str, Any] | None = None,
    ) -> StrategyResult:
        # Percentile grouping is moving from `profile_name` to `active_service` as part
        # of the performance_monitor rebuild (see .superpowers/sdd task series). The old
        # legacy metrics-repository percentile lookup path no longer exists; this
        # algorithm needs to be rewired onto the new active_service-scoped schema in a
        # later, separate rebuild. Out of scope here.
        raise NotImplementedError(
            "PercentileAlgorithm.compute is being rewired from profile_name-based grouping "
            "to active_service-based grouping; not yet implemented on the new schema."
        )
