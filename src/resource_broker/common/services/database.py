from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession


class DatabaseService:
    @asynccontextmanager
    async def get_session(self) -> AsyncIterator[AsyncSession]:
        from resource_broker.common.dao.database import get_session

        async with get_session() as session:
            yield session

    async def check_connection(self) -> bool:
        from resource_broker.common.dao.database import check_connection

        return await check_connection()


database = DatabaseService()
