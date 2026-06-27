"""Snapshot tables for Profile and Strategy CRDs.

Creates profile_snapshots and strategy_snapshots — flat current-state stores
used as cold-start fallback when the Kubernetes API is unavailable.

Design decisions:
  - One row per CRD, keyed by (namespace, profile_name) or strategy_name.
  - profile_info / strategy_info stores the full raw CRD as jsonb.
  - profile_hash / strategy_hash is SHA-256 of the canonical CRD; upserts
    are skipped when the hash is unchanged (safe for N concurrent replicas).
  - is_active=False signals a CRD was removed from Kubernetes. The row is
    kept rather than deleted so the history is preserved and re-applying
    the CRD reactivates the row atomically via ON CONFLICT DO UPDATE.

Revision ID: 0003
Revises: 0001
Create Date: 2026-06-27
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "profile_snapshots",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("namespace", sa.String(253), nullable=False),
        sa.Column("profile_name", sa.String(253), nullable=False),
        sa.Column("profile_hash", sa.String(64), nullable=False),
        sa.Column("profile_info", postgresql.JSONB(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("namespace", "profile_name", name="uq_profile_snapshot"),
    )
    op.create_index("idx_profile_snapshot_name", "profile_snapshots", ["profile_name"])
    op.create_index(
        "idx_profile_snapshot_active", "profile_snapshots", ["is_active", "profile_name"]
    )

    op.create_table(
        "strategy_snapshots",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("namespace", sa.String(253), nullable=False),
        sa.Column("strategy_name", sa.String(253), nullable=False),
        sa.Column("strategy_hash", sa.String(64), nullable=False),
        sa.Column("strategy_info", postgresql.JSONB(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("namespace", "strategy_name", name="uq_strategy_snapshot"),
    )
    op.create_index("idx_strategy_snapshot_name", "strategy_snapshots", ["strategy_name"])
    op.create_index(
        "idx_strategy_snapshot_active", "strategy_snapshots", ["is_active", "strategy_name"]
    )


def downgrade() -> None:
    op.drop_index("idx_strategy_snapshot_active", table_name="strategy_snapshots")
    op.drop_index("idx_strategy_snapshot_name", table_name="strategy_snapshots")
    op.drop_table("strategy_snapshots")

    op.drop_index("idx_profile_snapshot_active", table_name="profile_snapshots")
    op.drop_index("idx_profile_snapshot_name", table_name="profile_snapshots")
    op.drop_table("profile_snapshots")
