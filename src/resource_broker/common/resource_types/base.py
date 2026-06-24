from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from resource_broker.common.models.patch import PatchOperation
from resource_broker.common.models.profile import FieldStrategy


@dataclass
class FieldDef:
    path: str
    default_algorithm: str = "static"
    patch_type: str = "replace"
    description: str = ""


def resolve_strategy(strategy: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Translate a {algo, ...} strategy dict into (algorithm_name, config_dict)."""
    algo = strategy["algo"]
    rest: dict[str, Any] = {k: v for k, v in strategy.items() if k != "algo"}

    if algo == "percentile":
        if "percentile" not in rest:
            # Accept the CRD shorthand {"transform": "p90"} in addition to {"percentile": 90}.
            transform = rest.pop("transform", "p75")
            rest["percentile"] = int(transform.lstrip("p")) if isinstance(transform, str) else int(transform)
        else:
            rest.pop("transform", None)
        return "percentile", rest

    if algo == "static":
        return "static", rest

    return algo, rest


def _strategy_to_dict(s: FieldStrategy | dict[str, Any] | None) -> dict[str, Any] | None:
    """Normalise either a typed FieldStrategy or a legacy raw dict to a plain dict."""
    if s is None:
        return None
    if isinstance(s, FieldStrategy):
        return s.to_dict()
    return dict(s)


class ResourceType(ABC):
    name: str = ""
    description: str = ""

    @property
    @abstractmethod
    def fields(self) -> dict[str, FieldDef]:
        ...

    def resolve_locator(self, field: str, custom_locator: str | None = None) -> str:
        fd = self.fields.get(field)
        if fd is None:
            msg = f"unknown field {field!r} for resource type {self.name!r}"
            raise ValueError(msg)
        return custom_locator or fd.path

    async def build_patches(
        self,
        fields: dict[str, Any],
        strategy: FieldStrategy | dict[str, Any] | None,
        context: dict[str, Any],
    ) -> list[PatchOperation]:
        # NOTE: deferred import of a sibling subpackage (recommender) — common is meant
        # to be the dependency-free base layer, but build_patches is only ever invoked
        # from recommender.services.patcher. TODO(#25): remove by having the engine pass
        # an algorithm lookup callable into build_patches instead of importing it here.
        from resource_broker.common.models.profile import FieldEntry
        from resource_broker.recommender.algorithms.registry import algorithm_registry

        profile_strategy_dict = _strategy_to_dict(strategy)

        patches: list[PatchOperation] = []
        for field_name, entry in fields.items():
            fd = self.fields.get(field_name)
            if fd is None:
                continue

            if isinstance(entry, FieldEntry):
                field_strategy_dict = _strategy_to_dict(entry.strategy)
                locator = (entry.locator or None) or fd.path
                field_min = entry.min
                field_max = entry.max
            else:
                raw_s = entry.get("strategy")
                field_strategy_dict = _strategy_to_dict(raw_s)
                raw_locator = entry.get("locator")
                locator = (raw_locator or None) or fd.path
                field_min = entry.get("min")
                field_max = entry.get("max")

            effective_strategy = field_strategy_dict or profile_strategy_dict
            if effective_strategy is None:
                algorithm_name = fd.default_algorithm
                algo_config: dict[str, Any] = {}
            else:
                algorithm_name, algo_config = resolve_strategy(effective_strategy)

            if field_min is not None:
                algo_config = {**algo_config, "min": field_min}
            if field_max is not None:
                algo_config = {**algo_config, "max": field_max}

            algorithm = algorithm_registry.get(algorithm_name)
            result = await algorithm.compute(field_name, algo_config, context)

            if result.value is not None:
                patches.append(PatchOperation(op="replace", path=locator, value=result.value))

        return patches
