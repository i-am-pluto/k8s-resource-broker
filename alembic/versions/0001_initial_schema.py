"""Initial schema: pod_metrics table.

Resource profiles are now stored as Kubernetes CRDs (ResourceProfile),
not in the database. The database is used only for historical metrics.

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
        "pod_metrics",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("profile_name", sa.String(255), nullable=False),
        sa.Column("namespace", sa.String(253), nullable=False),
        sa.Column("pod_name", sa.String(253), nullable=False),
        sa.Column("container", sa.String(253), nullable=False),
        sa.Column("cpu_usage_cores", sa.Double(), nullable=True),
        sa.Column("mem_usage_bytes", sa.BigInteger(), nullable=True),
        sa.Column("scraped_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_metrics_scraped", "pod_metrics", ["profile_name", "scraped_at"])
    op.create_index("idx_metrics_pod", "pod_metrics", ["namespace", "pod_name", "container"])


def downgrade() -> None:
    op.drop_table("pod_metrics")
