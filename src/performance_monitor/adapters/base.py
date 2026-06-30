from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Sample:
    value: float
    timestamp: datetime
    labels: dict[str, str] = field(default_factory=dict)


class PromQLAdapter(ABC):

    @abstractmethod
    async def query(self, promql: str, time: datetime | None = None) -> list[Sample]:
        ...

    @abstractmethod
    async def query_range(
        self,
        promql: str,
        start: datetime,
        end: datetime,
        step: str = "30s",
    ) -> list[Sample]:
        ...

    @abstractmethod
    async def health(self) -> bool:
        ...
