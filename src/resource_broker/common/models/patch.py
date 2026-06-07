from __future__ import annotations

from typing import Any


class PatchOperation:
    def __init__(self, op: str, path: str, value: Any | None = None) -> None:
        self.op = op
        self.path = path
        self.value = value

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"op": self.op, "path": self.path}
        if self.value is not None:
            d["value"] = self.value
        return d
