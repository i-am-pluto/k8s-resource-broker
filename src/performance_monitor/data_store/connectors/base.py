from abc import ABC, abstractmethod


class StoreConnector(ABC):

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    async def health(self) -> bool: ...
