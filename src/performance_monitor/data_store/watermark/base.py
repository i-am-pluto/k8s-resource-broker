from abc import ABC, abstractmethod
from datetime import datetime


class WatermarkStore(ABC):

    @abstractmethod
    async def get(self, key: str) -> datetime | None: ...

    @abstractmethod
    async def set(self, key: str, timestamp: datetime) -> None: ...
