"""active_pods: per-pod status snapshots for monitored services.

Each row is keyed by (active_service_id, pod_name) and holds the last-known
k8s pod status, restart count, termination reason, and the JSONB blob of
configured resources (requests/limits) shaped by the ConfiguredResource
registry. Two independent UPSERT code paths write here — StatusWatcher
touches only status columns, UsageCollector touches all columns.

Revision ID: 0002
Revises: 0001
Create Date: 2026-05-30
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
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


def downgrade() -> None:
    op.drop_table("active_pods")
