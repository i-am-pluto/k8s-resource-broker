"""active_services: registry of discovered k8s services being monitored.

Each row represents a unique (namespace, service_name) pair observed by the
performance monitor's StatusWatcher or UsageCollector. The resource_type
column acts as a domain seam — it routes configured-resource shaping through
the ConfiguredResource registry (common/models/configured_resources/).

Revision ID: 0001
Revises:
Create Date: 2026-05-30
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

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


def downgrade() -> None:
    op.drop_table("active_services")
