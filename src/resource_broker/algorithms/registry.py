from __future__ import annotations

from structlog import get_logger

from resource_broker.algorithms.base import RecommendationAlgorithm
from resource_broker.algorithms.derived import DerivedAlgorithm
from resource_broker.algorithms.percentile import PercentileAlgorithm
from resource_broker.algorithms.static import StaticAlgorithm

logger = get_logger(__name__)

_BUILTIN_ALGORITHMS: dict[str, type[RecommendationAlgorithm]] = {
    "static": StaticAlgorithm,
    "percentile": PercentileAlgorithm,
    "derived": DerivedAlgorithm,
}


class AlgorithmRegistry:
    def __init__(self) -> None:
        self._algorithms: dict[str, type[RecommendationAlgorithm]] = {}

        for name, cls in _BUILTIN_ALGORITHMS.items():
            self._algorithms[name] = cls

    def register(self, name: str, algorithm_cls: type[RecommendationAlgorithm]) -> None:
        self._algorithms[name] = algorithm_cls
        logger.info("algorithm registered", name=name)

    def get(self, name: str) -> RecommendationAlgorithm:
        cls = self._algorithms.get(name)
        if cls is None:
            msg = f"unknown algorithm: {name!r} (available: {list(self._algorithms)})"
            raise ValueError(msg)
        return cls()

    def list_available(self) -> list[str]:
        return list(self._algorithms)


algorithm_registry = AlgorithmRegistry()
