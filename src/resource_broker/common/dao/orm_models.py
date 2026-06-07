from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Double, Index, String, Uuid, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class PodMetricModel(Base):
    __tablename__ = "pod_metrics"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    profile_name: Mapped[str] = mapped_column(String(255), nullable=False)
    namespace: Mapped[str] = mapped_column(String(253), nullable=False)
    pod_name: Mapped[str] = mapped_column(String(253), nullable=False)
    container: Mapped[str] = mapped_column(String(253), nullable=False)
    cpu_usage_cores: Mapped[float | None] = mapped_column(Double, nullable=True)
    mem_usage_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    scraped_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )

    __table_args__ = (
        Index("idx_metrics_scraped", "profile_name", "scraped_at"),
        Index("idx_metrics_pod", "namespace", "pod_name", "container"),
    )
