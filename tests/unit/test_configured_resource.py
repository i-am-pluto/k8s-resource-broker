from __future__ import annotations

import pytest

from resource_broker.common.models.configured_resources import (
    ConfiguredResourceRegistry,
    K8sConfiguredResource,
    configured_resource_registry,
)


def test_registry_resolves_k8s_pod() -> None:
    cls = configured_resource_registry.get("k8s-pod")
    assert cls is K8sConfiguredResource


def test_registry_unknown_raises_value_error() -> None:
    with pytest.raises(ValueError, match="unknown resource type for configured-resource shaping"):
        configured_resource_registry.get("nonexistent")


def test_registry_list_available() -> None:
    available = configured_resource_registry.list_available()
    assert "k8s-pod" in available


def test_registry_register_custom_type() -> None:
    registry = ConfiguredResourceRegistry()

    class CustomConfiguredResource(K8sConfiguredResource):
        pass

    registry.register("custom", CustomConfiguredResource)
    assert registry.get("custom") is CustomConfiguredResource
    assert configured_resource_registry.list_available() == ["k8s-pod"]


def test_to_dict_from_dict_round_trip() -> None:
    resource = K8sConfiguredResource(
        cpu_request="500m",
        memory_request="512Mi",
        cpu_limit="1",
        memory_limit="1Gi",
        ephemeral_storage="2Gi",
    )
    data = resource.to_dict()
    assert data == {
        "cpu_request": "500m",
        "memory_request": "512Mi",
        "cpu_limit": "1",
        "memory_limit": "1Gi",
        "ephemeral_storage": "2Gi",
    }

    rebuilt = K8sConfiguredResource.from_dict(data)
    assert rebuilt == resource


def test_from_dict_defaults_missing_keys_to_none() -> None:
    resource = K8sConfiguredResource.from_dict({})
    assert resource.cpu_request is None
    assert resource.memory_request is None
    assert resource.cpu_limit is None
    assert resource.memory_limit is None
    assert resource.ephemeral_storage is None
