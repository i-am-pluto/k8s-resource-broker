from __future__ import annotations

import pytest

from resource_broker.common.models.profile import FieldEntry, ResourceProfile
from resource_broker.watcher.services.patcher import compute_patches


@pytest.mark.asyncio
async def test_static_patch() -> None:
    profile = ResourceProfile(
        name="test-profile",
        namespace="default",
        resource_type="k8s-pod",
        mode="recommendation",
        fields={
            "cpu_request": FieldEntry(strategy={"algo": "static", "value": "250m"}),
            "memory_request": FieldEntry(strategy={"algo": "static", "value": "256Mi"}),
        },
    )
    pod_spec = {"spec": {"containers": [{"name": "app"}]}}
    patches = await compute_patches(profile, pod_spec)

    assert len(patches) == 2
    assert patches[0]["op"] == "replace"
    assert patches[0]["path"] == "/spec/containers/0/resources/requests/cpu"
    assert patches[0]["value"] == "250m"


@pytest.mark.asyncio
async def test_profile_level_strategy_default() -> None:
    profile = ResourceProfile(
        name="test-profile",
        namespace="default",
        resource_type="k8s-pod",
        mode="recommendation",
        strategy={"algo": "static", "value": "500m"},
        fields={
            "cpu_request": FieldEntry(),
            "memory_request": FieldEntry(strategy={"algo": "static", "value": "1Gi"}),
        },
    )
    pod_spec = {"spec": {"containers": [{"name": "app"}]}}
    patches = await compute_patches(profile, pod_spec)

    assert len(patches) == 2
    assert patches[0]["path"] == "/spec/containers/0/resources/requests/cpu"
    assert patches[0]["value"] == "500m"
    assert patches[1]["path"] == "/spec/containers/0/resources/requests/memory"
    assert patches[1]["value"] == "1Gi"


@pytest.mark.asyncio
async def test_empty_fields() -> None:
    profile = ResourceProfile(
        name="test-profile",
        namespace="default",
        resource_type="k8s-pod",
        fields={},
    )
    patches = await compute_patches(profile, {"spec": {"containers": []}})
    assert patches == []


@pytest.mark.asyncio
async def test_unknown_resource_type() -> None:
    profile = ResourceProfile(
        name="test-profile",
        namespace="default",
        resource_type="nonexistent",
        fields={"cpu_request": FieldEntry(strategy={"algo": "static", "value": "250m"})},
    )
    with pytest.raises(ValueError, match="unknown resource type"):
        await compute_patches(profile, {"spec": {"containers": []}})
