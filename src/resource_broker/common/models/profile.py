from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from resource_broker.common.utils import parse_duration_to_hours


@dataclass
class FieldStrategy:
    algo: str
    value: str | None = None             # static
    percentile: int | None = None        # percentile
    lookback_hours: float | None = None  # percentile (stored as float hours)
    source_field: str | None = None      # derived
    multiplier: float | None = None      # derived
    transform: str | dict | None = None  # derived: full transform expression preserved
    extra_args: dict[str, Any] | None = None  # unknown/custom algos: raw args

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> FieldStrategy:
        # New CRD ref+args format: {ref: "percentile", args: {percentile-type: "p75", ...}}
        if "ref" in d:
            return cls._from_ref_args(str(d["ref"]), d.get("args") or {})

        # Legacy flat format: {algo: "percentile", percentile: 75, lookback_hours: 24, ...}
        algo = d.get("algo", "static")

        percentile: int | None = None
        if "percentile" in d:
            percentile = int(d["percentile"])
        elif "transform" in d:
            t = d["transform"]
            if isinstance(t, str) and t.startswith("p"):
                percentile = int(t[1:])
            elif isinstance(t, int):
                percentile = t

        lookback_hours: float | None = None
        raw_lh = d.get("lookback_hours")
        if raw_lh is not None:
            lookback_hours = parse_duration_to_hours(raw_lh)

        return cls(
            algo=algo,
            value=d.get("value"),
            percentile=percentile,
            lookback_hours=lookback_hours,
            source_field=d.get("source"),
            multiplier=d.get("multiplier"),
        )

    @classmethod
    def _from_ref_args(cls, ref: str, args: dict[str, Any]) -> FieldStrategy:
        if ref == "percentile":
            ptype = args.get("percentile-type", "p90")
            percentile = int(str(ptype).lstrip("p"))
            coolback = args.get("coolback-period", "24h")
            lookback_hours = parse_duration_to_hours(coolback)
            return cls(algo="percentile", percentile=percentile, lookback_hours=lookback_hours)

        if ref == "static":
            return cls(algo="static", value=args.get("value"))

        if ref == "derived":
            transform = args.get("transform")
            multiplier: float | None = None
            if isinstance(transform, dict) and transform.get("op") == "mul":
                try:
                    multiplier = float(transform.get("operand", 1.0))
                except (TypeError, ValueError):
                    pass
            return cls(
                algo="derived",
                source_field=args.get("source-field"),
                multiplier=multiplier,
                transform=transform,
            )

        # Unknown/custom algo registered at runtime — preserve all args so round-trips work.
        return cls(algo=ref, extra_args=dict(args) if args else None)

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
        if self.transform is not None:
            d["transform"] = self.transform
        if self.extra_args:
            d["extra_args"] = self.extra_args
        return d


@dataclass
class FieldEntry:
    locator: str | None = None
    min: str | None = None
    max: str | None = None
    max_percentage: int | None = None
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

        # CRD field name is "default-strategy"; fall back to bare "strategy" for
        # any in-flight objects written with the old schema.
        raw_strategy = spec.get("default-strategy") or spec.get("strategy")
        default_strategy = FieldStrategy.from_dict(raw_strategy) if raw_strategy else None

        fields_raw = spec.get("fields", {})
        parsed_fields: dict[str, FieldEntry] = {}
        for name, entry in fields_raw.items():
            raw_s = entry.get("strategy")
            raw_mp = entry.get("max_percentage")
            parsed_fields[name] = FieldEntry(
                locator=entry.get("locator") or None,
                min=entry.get("min"),
                max=entry.get("max"),
                max_percentage=int(raw_mp) if raw_mp is not None else None,
                strategy=FieldStrategy.from_dict(raw_s) if raw_s else None,
            )

        return cls(
            name=metadata.get("name", "unknown"),
            namespace=metadata.get("namespace", ""),
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
                    "max_percentage": entry.max_percentage,
                    "strategy": entry.strategy.to_dict() if entry.strategy else None,
                }.items() if v is not None}
                for name, entry in self.fields.items()
            },
        }

    def is_enforce_mode(self) -> bool:
        return self.mode == "enforce"
