from __future__ import annotations

import operator
from typing import Any

from resource_broker.algorithms.base import RecommendationAlgorithm
from resource_broker.common.models.strategy import StrategyResult

_OPS: dict[str, Any] = {
    "add": operator.add,
    "sub": operator.sub,
    "mul": operator.mul,
    "multiply": operator.mul,
    "truediv": operator.truediv,
    "div": operator.truediv,
    "floor_div": operator.floordiv,
    "mod": operator.mod,
}


class DerivedAlgorithm(RecommendationAlgorithm):
    async def compute(
        self,
        field: str,
        config: dict[str, Any],
        context: dict[str, Any] | None = None,
    ) -> StrategyResult:
        if not context:
            return StrategyResult(value=None, source="derived-no-context")

        source_field = config.get("source_field", "spec.containers[0].resources.requests")
        transform = config.get("transform", "to_string")
        pod_spec = context.get("pod_spec", {})

        source_value = _resolve_jsonpath(source_field, pod_spec)
        if source_value is None:
            return StrategyResult(value=None, source="derived-source-not-found")

        transformed = _apply_transform(transform, source_value)
        return StrategyResult(value=transformed, source="derived")


def _resolve_jsonpath(path: str, obj: dict[str, Any]) -> Any:
    import re

    parts = re.split(r"\.|(?=\[)", path)
    current: Any = obj
    for part in parts:
        if part.startswith("["):
            idx_str = part.strip("[]")
            if idx_str == "*":
                if isinstance(current, list) and current:
                    current = current[0]
                else:
                    return None
            else:
                try:
                    idx = int(idx_str)
                    current = current[idx] if isinstance(current, list) else None
                except (ValueError, IndexError, TypeError):
                    return None
        elif part:
            current = current.get(part) if isinstance(current, dict) else None
        if current is None:
            return None
    return current


def _apply_transform(transform: str | dict[str, Any], value: Any) -> Any:
    if isinstance(transform, str):
        if transform == "to_string":
            return str(value)
        if transform == "to_int":
            return int(value)
        if transform == "to_float":
            return float(value)

        m = __import__("re").match(r"(\w+)\(([^)]+)\)", transform)
        if m:
            op_name, arg = m.group(1), m.group(2)
            op_func = _OPS.get(op_name)
            if op_func:
                arg_val = float(arg) if "." in arg else int(arg)
                return op_func(float(value), arg_val)

    if isinstance(transform, dict):
        op_func = _OPS.get(transform.get("op", ""))
        if op_func:
            right = transform.get("value", 0)
            return op_func(float(value), float(right))

    return value
