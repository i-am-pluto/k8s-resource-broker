from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import datetime


class MetricSample:
    def __init__(
        self,
        profile: str,
        resource_type: str,
        field: str,
        value: float,
        timestamp: datetime,
    ) -> None:
        self.profile = profile
        self.resource_type = resource_type
        self.field = field
        self.value = value
        self.timestamp = timestamp


class UsageStore(ABC):

    @abstractmethod
    async def store(self, samples: Sequence[MetricSample]) -> None: ...

    @abstractmethod
    async def query(
        self,
        profile: str,
        field: str,
        start: datetime,
        end: datetime,
    ) -> list[MetricSample]: ...
