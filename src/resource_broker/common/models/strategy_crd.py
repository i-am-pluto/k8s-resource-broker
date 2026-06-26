from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from resource_broker.common.utils import parse_duration_to_minutes


@dataclass
class ArgDefinition:
    type: str | None = None
    enum: list | None = None
    required: bool = False
    default: Any = None
    description: str | None = None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ArgDefinition:
        return cls(
            type=d.get("type"),
            enum=d.get("enum"),
            required=bool(d.get("required", False)),
            default=d.get("default"),
            description=d.get("description"),
        )

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {}
        if self.type is not None:
            d["type"] = self.type
        if self.enum is not None:
            d["enum"] = self.enum
        d["required"] = self.required
        if self.default is not None:
            d["default"] = self.default
        if self.description is not None:
            d["description"] = self.description
        return d


@dataclass
class StrategyCRD:
    """In-memory representation of a Strategy custom resource."""

    name: str
    algo: str
    image: str | None = None
    args_schema: dict[str, ArgDefinition] = field(default_factory=dict)
    # Parsed from spec.schedule.run-every; None means no periodic re-evaluation.
    run_every_minutes: float | None = None

    @classmethod
    def from_crd(cls, crd: dict[str, Any]) -> StrategyCRD:
        metadata = crd.get("metadata", {})
        spec = crd.get("spec", {})

        args_raw = spec.get("args") or {}
        args_schema: dict[str, ArgDefinition] = {
            k: ArgDefinition.from_dict(v) if isinstance(v, dict) else ArgDefinition()
            for k, v in args_raw.items()
        }

        run_every_minutes: float | None = None
        schedule = spec.get("schedule") or {}
        raw_re = schedule.get("run-every")
        if raw_re:
            try:
                run_every_minutes = parse_duration_to_minutes(raw_re)
            except ValueError:
                pass

        return cls(
            name=metadata.get("name", "unknown"),
            algo=spec.get("algo", ""),
            image=spec.get("image") or None,
            args_schema=args_schema,
            run_every_minutes=run_every_minutes,
        )

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"name": self.name, "algo": self.algo}
        if self.image is not None:
            d["image"] = self.image
        if self.args_schema:
            d["args_schema"] = {k: v.to_dict() for k, v in self.args_schema.items()}
        if self.run_every_minutes is not None:
            d["run_every_minutes"] = self.run_every_minutes
        return d

    def content_hash(self) -> str:
        return hashlib.sha256(
            json.dumps(self.to_dict(), sort_keys=True).encode()
        ).hexdigest()
