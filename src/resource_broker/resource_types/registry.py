from __future__ import annotations

from structlog import get_logger

from resource_broker.resource_types.base import ResourceType
from resource_broker.resource_types.k8s_resources import K8sResources

logger = get_logger(__name__)

_BUILTIN_TYPES: dict[str, type[ResourceType]] = {
    "k8s-pod": K8sResources,
}


class ResourceTypeRegistry:
    def __init__(self) -> None:
        self._types: dict[str, type[ResourceType]] = {}

        for name, cls in _BUILTIN_TYPES.items():
            self._types[name] = cls

    def register(self, name: str, rtype_cls: type[ResourceType]) -> None:
        self._types[name] = rtype_cls
        logger.info("resource type registered", name=name)

    def get(self, name: str) -> ResourceType:
        cls = self._types.get(name)
        if cls is None:
            msg = f"unknown resource type: {name!r} (available: {list(self._types)})"
            raise ValueError(msg)
        return cls()

    def list_available(self) -> list[str]:
        return list(self._types)


resource_type_registry = ResourceTypeRegistry()
