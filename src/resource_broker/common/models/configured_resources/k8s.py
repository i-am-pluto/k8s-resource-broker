"""K8sConfiguredResource: per-pod CPU/memory requests and limits for k8s-pod.

A dataclass that marshals the first container's resource fields into a
shaped dict for storage in the active_pods JSONB column.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from resource_broker.common.models.configured_resources.base import ConfiguredResource


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
    def from_dict(cls, data: dict[str, Any]) -> K8sConfiguredResource:
        return cls(
            cpu_request=data.get("cpu_request"),
            memory_request=data.get("memory_request"),
            cpu_limit=data.get("cpu_limit"),
            memory_limit=data.get("memory_limit"),
            ephemeral_storage=data.get("ephemeral_storage"),
        )
