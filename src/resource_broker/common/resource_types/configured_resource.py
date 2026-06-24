from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


class ConfiguredResource(ABC):
    """Current-state shape of a resource-type's configured cpu/mem (requests+limits).

    Fetch+shape only — never computes patches, never touches strategy/algorithm state.
    """

    @abstractmethod
    def to_dict(self) -> dict[str, Any]: ...

    @classmethod
    @abstractmethod
    def from_dict(cls, data: dict[str, Any]) -> "ConfiguredResource": ...


@dataclass
class K8sConfiguredResource(ConfiguredResource):
    cpu_request: str | None
    memory_request: str | None
    cpu_limit: str | None
    memory_limit: str | None
    ephemeral_storage: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "cpu_request": self.cpu_request,
            "memory_request": self.memory_request,
            "cpu_limit": self.cpu_limit,
            "memory_limit": self.memory_limit,
            "ephemeral_storage": self.ephemeral_storage,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "K8sConfiguredResource":
        return cls(
            cpu_request=data.get("cpu_request"),
            memory_request=data.get("memory_request"),
            cpu_limit=data.get("cpu_limit"),
            memory_limit=data.get("memory_limit"),
            ephemeral_storage=data.get("ephemeral_storage"),
        )


_BUILTIN_CONFIGURED_RESOURCES: dict[str, type[ConfiguredResource]] = {
    "k8s-pod": K8sConfiguredResource,
}


class ConfiguredResourceRegistry:
    def __init__(self) -> None:
        self._types: dict[str, type[ConfiguredResource]] = dict(_BUILTIN_CONFIGURED_RESOURCES)

    def register(self, name: str, cls: type[ConfiguredResource]) -> None:
        self._types[name] = cls

    def get(self, name: str) -> type[ConfiguredResource]:
        """Returns the CLASS, not an instance — unlike resource_type_registry.get(),
        which returns an instance. Construction needs kwargs from the live pod spec,
        so callers do `configured_resource_registry.get(name)(cpu_request=..., ...)`
        or `.get(name).from_dict(raw_dict)`."""
        cls = self._types.get(name)
        if cls is None:
            msg = f"unknown resource type for configured-resource shaping: {name!r}"
            raise ValueError(msg)
        return cls

    def list_available(self) -> list[str]:
        return list(self._types)


configured_resource_registry = ConfiguredResourceRegistry()
