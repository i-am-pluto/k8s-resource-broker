from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    Float,
    Index,
    String,
    delete,
    select,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from ..connectors.postgres import PostgresConnector
from .base import MetricSample, UsageStore


class Base(DeclarativeBase):
    pass


class UsageSampleModel(Base):
    __tablename__ = "usage_samples"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    profile: Mapped[str] = mapped_column(String(255), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(255), nullable=False)
    field: Mapped[str] = mapped_column(String(255), nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("ix_usage_profile_field_ts", "profile", "field", "timestamp"),
    )


class PostgresUsageStore(UsageStore):
    def __init__(self, connector: PostgresConnector, ttl_days: int = 30) -> None:
        self._connector = connector
        self._ttl_days = ttl_days

    async def _ensure_table(self) -> None:
        async with self._connector.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def store(self, samples: Sequence[MetricSample]) -> None:
        await self._ensure_table()
        async with self._connector.session() as session:
            for s in samples:
                session.add(
                    UsageSampleModel(
                        profile=s.profile,
                        resource_type=s.resource_type,
                        field=s.field,
                        value=s.value,
                        timestamp=s.timestamp,
                    )
                )
            await session.commit()

    async def query(
        self,
        profile: str,
        field: str,
        start: datetime,
        end: datetime,
    ) -> list[MetricSample]:
        await self._ensure_table()
        async with self._connector.session() as session:
            result = await session.execute(
                select(UsageSampleModel)
                .where(UsageSampleModel.profile == profile)
                .where(UsageSampleModel.field == field)
                .where(UsageSampleModel.timestamp >= start)
                .where(UsageSampleModel.timestamp <= end)
                .order_by(UsageSampleModel.timestamp)
            )
            rows = result.scalars().all()
            return [
                MetricSample(
                    profile=r.profile,
                    resource_type=r.resource_type,
                    field=r.field,
                    value=r.value,
                    timestamp=r.timestamp,
                )
                for r in rows
            ]

    async def prune(self) -> int:
        await self._ensure_table()
        async with self._connector.session() as session:
            cutoff = datetime.utcnow()
            result = await session.execute(
                delete(UsageSampleModel).where(UsageSampleModel.timestamp < cutoff)
            )
            await session.commit()
            return int(result.rowcount)  # type: ignore[attr-defined]
