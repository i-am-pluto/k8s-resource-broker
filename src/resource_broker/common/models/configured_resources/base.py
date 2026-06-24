"""Abstract base for configured-resource data classes.

Each resource type (e.g. k8s-pod) defines its own ConfiguredResource
subclass that knows how to shape the pod's current requests/limits into
a portable dict and back.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class ConfiguredResource(ABC):
    @abstractmethod
    def to_dict(self) -> dict[str, Any]: ...

    @classmethod
    @abstractmethod
    def from_dict(cls, data: dict[str, Any]) -> ConfiguredResource: ...
