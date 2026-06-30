from __future__ import annotations

import pytest

from resource_broker.recommender.algorithms.derived import DerivedAlgorithm
from resource_broker.recommender.algorithms.registry import algorithm_registry
from resource_broker.recommender.algorithms.static import StaticAlgorithm


@pytest.mark.asyncio
async def test_static_algorithm() -> None:
    algo = StaticAlgorithm()
    result = await algo.compute(
        field="requests.cpu",
        config={"value": {"cpu": "500m", "memory": "1Gi"}},
    )
    assert result.value == {"cpu": "500m", "memory": "1Gi"}
    assert result.source == "static"


@pytest.mark.asyncio
async def test_static_algorithm_missing_value() -> None:
    algo = StaticAlgorithm()
    with pytest.raises(ValueError, match="requires 'config.value'"):
        await algo.compute(field="requests.cpu", config={})


@pytest.mark.asyncio
async def test_derived_algorithm_to_string() -> None:
    algo = DerivedAlgorithm()
    result = await algo.compute(
        field="args",
        config={
            "source_field": "spec.containers[0].resources.requests.cpu",
            "transform": "to_string",
        },
        context={"pod_spec": {"spec": {"containers": [{"resources": {"requests": {"cpu": "500m"}}}]}}},
    )
    assert result.value == "500m"


@pytest.mark.asyncio
async def test_derived_algorithm_multiply() -> None:
    algo = DerivedAlgorithm()
    result = await algo.compute(
        field="limits.cpu",
        config={
            "source_field": "spec.containers[0].resources.requests.cpu",
            "transform": "multiply(2.0)",
        },
        context={"pod_spec": {"spec": {"containers": [{"resources": {"requests": {"cpu": 0.5}}}]}}},
    )
    assert result.value == 1.0


# _parse_resource_value was deleted from percentile.py as part of the performance_monitor
# rebuild (Task 1, see .superpowers/sdd/task-1-brief.md) — PercentileAlgorithm.compute()
# now raises NotImplementedError pending the active_service-based rewrite. These tests are
# skipped (not deleted) until that rewrite lands and reintroduces equivalent parsing.
@pytest.mark.skip(reason="_parse_resource_value removed in Task 1; pending active_service-based percentile rewrite")
def test_parse_cpu_millicores() -> None:
    pass


@pytest.mark.skip(reason="_parse_resource_value removed in Task 1; pending active_service-based percentile rewrite")
def test_parse_memory() -> None:
    pass


@pytest.mark.asyncio
async def test_registry_lookup() -> None:
    algo = algorithm_registry.get("static")
    assert isinstance(algo, StaticAlgorithm)

    algo = algorithm_registry.get("derived")
    assert isinstance(algo, DerivedAlgorithm)


def test_registry_list() -> None:
    available = algorithm_registry.list_available()
    assert "static" in available
    assert "percentile" in available
    assert "derived" in available


@pytest.mark.asyncio
async def test_registry_unknown() -> None:
    with pytest.raises(ValueError, match="unknown algorithm"):
        algorithm_registry.get("nonexistent")
