from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FieldEntry:
    locator: str | None = None
    min: str | None = None
    max: str | None = None
    strategy: dict[str, Any] | None = None


@dataclass
class ResourceProfile:
    name: str
    namespace: str
    resource_type: str
    mode: str = "recommendation"
    strategy: dict[str, Any] | None = None
    fields: dict[str, FieldEntry] = field(default_factory=dict)

    @classmethod
    def from_crd(cls, crd: dict[str, Any]) -> ResourceProfile:
        metadata = crd.get("metadata", {})
        spec = crd.get("spec", {})

        fields_raw = spec.get("fields", {})
        parsed_fields = {
            name: FieldEntry(
                locator=entry.get("locator") or None,
                min=entry.get("min"),
                max=entry.get("max"),
                strategy=entry.get("strategy"),
            )
            for name, entry in fields_raw.items()
        }

        return cls(
            name=metadata.get("name", "unknown"),
            namespace=metadata.get("namespace", "default"),
            resource_type=spec.get("resource-type", ""),
            mode=spec.get("mode", "recommendation"),
            strategy=spec.get("strategy"),
            fields=parsed_fields,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "namespace": self.namespace,
            "resource_type": self.resource_type,
            "mode": self.mode,
            "strategy": self.strategy,
            "fields": {
                name: {k: v for k, v in {
                    "locator": entry.locator,
                    "min": entry.min,
                    "max": entry.max,
                    "strategy": entry.strategy,
                }.items() if v is not None}
                for name, entry in self.fields.items()
            },
        }

    def is_enforce_mode(self) -> bool:
        return self.mode == "enforce"
