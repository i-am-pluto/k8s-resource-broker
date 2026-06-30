from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Double,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class ActiveServiceModel(Base):
    __tablename__ = "active_services"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, server_default=text("gen_random_uuid()"))
    namespace: Mapped[str] = mapped_column(String(253), nullable=False)
    service_name: Mapped[str] = mapped_column(String(253), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(253), nullable=False)

    __table_args__ = (UniqueConstraint("namespace", "service_name", name="uq_active_services_ns_name"),)


class ActivePodModel(Base):
    __tablename__ = "active_pods"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, server_default=text("gen_random_uuid()"))
    active_service_id: Mapped[uuid.UUID] = mapped_column(Uuid, ForeignKey("active_services.id"), nullable=False)
    pod_name: Mapped[str] = mapped_column(String(253), nullable=False)
    configured_resource: Mapped[dict] = mapped_column(JSONB, nullable=False)
    pod_status: Mapped[str] = mapped_column(String(64), nullable=False)
    restart_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    last_terminated_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)

    __table_args__ = (UniqueConstraint("active_service_id", "pod_name", name="uq_active_pods_service_pod"),)


class PodPerformanceMetricModel(Base):
    __tablename__ = "pod_performance_metric"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, server_default=text("gen_random_uuid()"))
    active_pod_id: Mapped[uuid.UUID] = mapped_column(Uuid, ForeignKey("active_pods.id"), nullable=False)
    cpu_usage_cores: Mapped[float] = mapped_column(Double, nullable=False)
    mem_usage_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    scraped_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (Index("idx_pod_performance_metric_pod_time", "active_pod_id", "scraped_at"),)


class ProfileVersionModel(Base):
    """One row per profile version (SCD Type 2). Only one row per (name, namespace) has is_current=True."""

    __tablename__ = "resource_profile_versions"

    profile_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(253), nullable=False)
    namespace: Mapped[str] = mapped_column(String(253), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(253), nullable=False)
    mode: Mapped[str] = mapped_column(String(64), nullable=False, server_default="recommendation")
    # Profile-level default strategy (algo name + params minus the "algo" key).
    default_algo: Mapped[str | None] = mapped_column(String(64), nullable=True)
    default_algo_config: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    # SHA-256 of canonical profile content — used for idempotency across distributed replicas.
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    # Optimistic concurrency version. Starts at 1 for every new SCD row.
    # record_version() expires the current row with WHERE version=$v; if 0 rows
    # are affected another replica won the race and this replica skips the insert.
    version: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("1"))
    valid_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    valid_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))

    field_strategies: Mapped[list[ProfileFieldStrategyModel]] = relationship(
        "ProfileFieldStrategyModel",
        back_populates="profile_version",
        cascade="all, delete-orphan",
        lazy="raise",
    )

    __table_args__ = (
        # Partial index — only one current row per (name, namespace) in practice.
        Index("idx_profile_current", "name", "namespace", postgresql_where=text("is_current = true")),
        Index("idx_profile_history", "name", "namespace", "valid_from"),
    )


class ProfileFieldStrategyModel(Base):
    """Normalized per-field strategy. One row per managed field per profile version."""

    __tablename__ = "resource_profile_field_strategies"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    profile_id: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey("resource_profile_versions.profile_id", ondelete="CASCADE"),
        nullable=False,
    )
    field_name: Mapped[str] = mapped_column(String(253), nullable=False)
    locator: Mapped[str | None] = mapped_column(String(512), nullable=True)
    # NULL algo means "inherit the profile-level default strategy at runtime".
    algo: Mapped[str | None] = mapped_column(String(64), nullable=True)
    algo_config: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    min_value: Mapped[str | None] = mapped_column(String(64), nullable=True)
    max_value: Mapped[str | None] = mapped_column(String(64), nullable=True)

    profile_version: Mapped[ProfileVersionModel] = relationship(
        "ProfileVersionModel", back_populates="field_strategies"
    )

    __table_args__ = (Index("idx_field_strategy_profile", "profile_id"),)


class ProfileRecommendationModel(Base):
    """Audit trail: which profile version produced which patch for which pod."""

    __tablename__ = "profile_recommendations"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    profile_id: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey("resource_profile_versions.profile_id"),
        nullable=False,
    )
    pod_name: Mapped[str] = mapped_column(String(253), nullable=False)
    pod_namespace: Mapped[str] = mapped_column(String(253), nullable=False)
    patches: Mapped[dict] = mapped_column(JSONB, nullable=False)
    recommended_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (Index("idx_recommendation_profile", "profile_id", "recommended_at"),)
