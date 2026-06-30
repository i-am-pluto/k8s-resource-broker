"""Initial schema: active_services, active_pods, pod_performance_metric.

performance_monitor tracks pods, their k8s status, configured resources,
and time-series usage. No profile/strategy coupling — resource_type is the
only domain seam (routed through the ConfiguredResource registry).

Revision ID: 0001
Revises:
Create Date: 2026-05-30
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "active_services",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("namespace", sa.String(253), nullable=False),
        sa.Column("service_name", sa.String(253), nullable=False),
        sa.Column("resource_type", sa.String(253), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("namespace", "service_name", name="uq_active_services_ns_name"),
    )

    op.create_table(
        "active_pods",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("active_service_id", sa.Uuid(), nullable=False),
        sa.Column("pod_name", sa.String(253), nullable=False),
        sa.Column("configured_resource", postgresql.JSONB(), nullable=False),
        sa.Column("pod_status", sa.String(64), nullable=False),
        sa.Column("restart_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("last_terminated_reason", sa.String(64), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["active_service_id"], ["active_services.id"]),
        sa.UniqueConstraint("active_service_id", "pod_name", name="uq_active_pods_service_pod"),
    )

    op.create_table(
        "pod_performance_metric",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("active_pod_id", sa.Uuid(), nullable=False),
        sa.Column("cpu_usage_cores", sa.Double(), nullable=False),
        sa.Column("mem_usage_bytes", sa.BigInteger(), nullable=False),
        sa.Column("scraped_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["active_pod_id"], ["active_pods.id"]),
    )
    op.create_index(
        "idx_pod_performance_metric_pod_time",
        "pod_performance_metric",
        ["active_pod_id", "scraped_at"],
    )


def downgrade() -> None:
    op.drop_index("idx_pod_performance_metric_pod_time", table_name="pod_performance_metric")
    op.drop_table("pod_performance_metric")
    op.drop_table("active_pods")
    op.drop_table("active_services")
