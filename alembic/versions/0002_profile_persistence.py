"""Add resource_profiles table for profile DB persistence.

Profiles are still the source-of-truth as Kubernetes CRDs, but this table acts
as a write-through cache: every CRD event persists here so bootstrap can fall
back to DB when the Kubernetes API is temporarily unavailable.

Revision ID: 0002
Revises: 0001
Create Date: 2026-06-12
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
        "resource_profiles",
        sa.Column("name", sa.String(253), nullable=False),
        sa.Column("namespace", sa.String(253), nullable=False),
        sa.Column("resource_type", sa.String(253), nullable=False),
        sa.Column("mode", sa.String(64), nullable=False, server_default="recommendation"),
        sa.Column("strategy", postgresql.JSONB(), nullable=True),
        sa.Column("fields", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("name", "namespace"),
    )
    # Fast lookups for bootstrap fallback query (all active profiles).
    op.create_index("idx_profiles_active", "resource_profiles", ["namespace", "deleted_at"])


def downgrade() -> None:
    op.drop_index("idx_profiles_active", table_name="resource_profiles")
    op.drop_table("resource_profiles")