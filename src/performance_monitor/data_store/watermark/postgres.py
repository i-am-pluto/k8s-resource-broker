from datetime import datetime

from sqlalchemy import BigInteger, DateTime, String, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from ..connectors.postgres import PostgresConnector
from .base import WatermarkStore


class Base(DeclarativeBase):
    pass


class WatermarkModel(Base):
    __tablename__ = "watermarks"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    watermark: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class PostgresWatermarkStore(WatermarkStore):
    def __init__(self, connector: PostgresConnector) -> None:
        self._connector = connector

    async def _ensure_table(self) -> None:
        async with self._connector.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def get(self, key: str) -> datetime | None:
        await self._ensure_table()
        async with self._connector.session() as session:
            result = await session.execute(
                select(WatermarkModel).where(WatermarkModel.key == key)
            )
            row = result.scalar_one_or_none()
            return row.watermark if row is not None else None

    async def set(self, key: str, timestamp: datetime) -> None:
        await self._ensure_table()
        async with self._connector.session() as session:
            result = await session.execute(
                select(WatermarkModel).where(WatermarkModel.key == key)
            )
            row = result.scalar_one_or_none()
            if row is not None:
                row.watermark = timestamp
            else:
                session.add(WatermarkModel(key=key, watermark=timestamp))
            await session.commit()
