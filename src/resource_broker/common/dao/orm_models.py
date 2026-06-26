from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Double, Index, String, UniqueConstraint, Uuid, func
from sqlalchemy.dialects.postgresql import JSONB
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


class ProfileSnapshotModel(Base):
    """Flat current-state snapshot of every known Profile CRD.

    Written on every bootstrap / watch event (hash-gated to avoid no-op writes).
    Used as the cold-start fallback when the Kubernetes API server is unavailable.
    Stores the last-known raw CRD spec as jsonb — no history, just current state.
    """

    __tablename__ = "profile_snapshots"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    namespace: Mapped[str] = mapped_column(String(253), nullable=False)
    profile_name: Mapped[str] = mapped_column(String(253), nullable=False)
    profile_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    profile_info: Mapped[dict] = mapped_column(JSONB, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("namespace", "profile_name", name="uq_profile_snapshot"),
        Index("idx_profile_snapshot_name", "profile_name"),
    )


class StrategySnapshotModel(Base):
    """Flat current-state snapshot of every known Strategy CRD.

    Same write-through / hash-gated pattern as ProfileSnapshotModel.
    Strategy CRDs are cluster-scoped so namespace is always stored as an empty string.
    """

    __tablename__ = "strategy_snapshots"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    namespace: Mapped[str] = mapped_column(String(253), nullable=False)
    strategy_name: Mapped[str] = mapped_column(String(253), nullable=False)
    strategy_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    strategy_info: Mapped[dict] = mapped_column(JSONB, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("namespace", "strategy_name", name="uq_strategy_snapshot"),
        Index("idx_strategy_snapshot_name", "strategy_name"),
    )