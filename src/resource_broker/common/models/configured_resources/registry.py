"""Registry mapping resource-type names to ConfiguredResource subclasses.

Callers use `configured_resource_registry.get(name)(cpu_request=..., ...)`
to construct a dataclass from live pod data, or `.get(name).from_dict(raw)`
to deserialize from the JSONB column.
"""

from __future__ import annotations

from resource_broker.common.models.configured_resources.base import ConfiguredResource
from resource_broker.common.models.configured_resources.k8s import K8sConfiguredResource

_BUILTIN_CONFIGURED_RESOURCES: dict[str, type[ConfiguredResource]] = {
    "k8s-pod": K8sConfiguredResource,
}


class ConfiguredResourceRegistry:
    def __init__(self) -> None:
        self._types: dict[str, type[ConfiguredResource]] = dict(_BUILTIN_CONFIGURED_RESOURCES)

    def register(self, name: str, cls: type[ConfiguredResource]) -> None:
        self._types[name] = cls

    def get(self, name: str) -> type[ConfiguredResource]:
        cls = self._types.get(name)
        if cls is None:
            msg = f"unknown resource type for configured-resource shaping: {name!r}"
            raise ValueError(msg)
        return cls

    def list_available(self) -> list[str]:
        return list(self._types)


configured_resource_registry = ConfiguredResourceRegistry()
