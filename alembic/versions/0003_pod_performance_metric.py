"""pod_performance_metric: time-series of per-pod CPU and memory usage.

A pure-append time-series table: each row records cpu_usage_cores and
mem_usage_bytes scraped from Prometheus/Kubecost at scraped_at. The
composite index (active_pod_id, scraped_at) supports the percentile
queries used by the recommender and the read API.

Revision ID: 0003
Revises: 0002
Create Date: 2026-05-30
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
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
