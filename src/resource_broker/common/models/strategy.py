from __future__ import annotations

from typing import Any


class StrategyResult:
    def __init__(self, value: Any, source: str = "static") -> None:
        self.value: Any = value
        self.source: str = source
