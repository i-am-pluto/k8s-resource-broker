from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FieldStrategy:
    algo: str
    value: str | None = None            # static
    percentile: int | None = None       # percentile
    lookback_hours: int | None = None   # percentile
    source_field: str | None = None     # derived
    multiplier: float | None = None     # derived

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> FieldStrategy:
        algo = d.get("algo", "static")
        # Accept both CRD form ("transform": "p90") and stored form ("percentile": 90).
        percentile: int | None = None
        if "percentile" in d:
            percentile = int(d["percentile"])
        elif "transform" in d:
            t = d["transform"]
            if isinstance(t, str) and t.startswith("p"):
                percentile = int(t[1:])
            elif isinstance(t, int):
                percentile = t
        return cls(
            algo=algo,
            value=d.get("value"),
            percentile=percentile,
            lookback_hours=d.get("lookback_hours"),
            source_field=d.get("source"),
            multiplier=d.get("multiplier"),
        )

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"algo": self.algo}
        if self.value is not None:
            d["value"] = self.value
        if self.percentile is not None:
            d["percentile"] = self.percentile
        if self.lookback_hours is not None:
            d["lookback_hours"] = self.lookback_hours
        if self.source_field is not None:
            d["source"] = self.source_field
        if self.multiplier is not None:
            d["multiplier"] = self.multiplier
        return d


@dataclass
class FieldEntry:
    locator: str | None = None
    min: str | None = None
    max: str | None = None
    strategy: FieldStrategy | None = None


@dataclass
class ResourceProfile:
    name: str
    namespace: str
    resource_type: str
    mode: str = "recommendation"
    strategy: FieldStrategy | None = None
    fields: dict[str, FieldEntry] = field(default_factory=dict)

    @classmethod
    def from_crd(cls, crd: dict[str, Any]) -> ResourceProfile:
        metadata = crd.get("metadata", {})
        spec = crd.get("spec", {})

        raw_strategy = spec.get("strategy")
        default_strategy = FieldStrategy.from_dict(raw_strategy) if raw_strategy else None

        fields_raw = spec.get("fields", {})
        parsed_fields = {
            name: FieldEntry(
                locator=entry.get("locator") or None,
                min=entry.get("min"),
                max=entry.get("max"),
                strategy=FieldStrategy.from_dict(entry["strategy"]) if entry.get("strategy") else None,
            )
            for name, entry in fields_raw.items()
        }

        return cls(
            name=metadata.get("name", "unknown"),
            namespace=metadata.get("namespace", "default"),
            resource_type=spec.get("resource-type", ""),
            mode=spec.get("mode", "recommendation"),
            strategy=default_strategy,
            fields=parsed_fields,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "namespace": self.namespace,
            "resource_type": self.resource_type,
            "mode": self.mode,
            "strategy": self.strategy.to_dict() if self.strategy else None,
            "fields": {
                name: {k: v for k, v in {
                    "locator": entry.locator,
                    "min": entry.min,
                    "max": entry.max,
                    "strategy": entry.strategy.to_dict() if entry.strategy else None,
                }.items() if v is not None}
                for name, entry in self.fields.items()
            },
        }

    def is_enforce_mode(self) -> bool:
        return self.mode == "enforce"
