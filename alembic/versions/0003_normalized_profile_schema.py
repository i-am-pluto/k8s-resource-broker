"""Normalized profile schema: SCD Type 2 versions + per-field strategies + recommendation audit.

Replaces the flat resource_profiles table (ON CONFLICT DO UPDATE, JSONB fields blob) with:
  - resource_profile_versions  — one row per CRD version, SCD Type 2
  - resource_profile_field_strategies — normalized field-level strategies (one row per field)
  - profile_recommendations    — audit trail: profile_id → patches given to each pod

Revision ID: 0003
Revises: 0001
Create Date: 2026-06-13
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
    # SCD Type 2 spine — one row per profile version.
    op.create_table(
        "resource_profile_versions",
        sa.Column("profile_id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("name", sa.String(253), nullable=False),
        sa.Column("namespace", sa.String(253), nullable=False),
        sa.Column("resource_type", sa.String(253), nullable=False),
        sa.Column("mode", sa.String(64), nullable=False, server_default="recommendation"),
        sa.Column("default_algo", sa.String(64), nullable=True),
        sa.Column("default_algo_config", postgresql.JSONB(), nullable=True),
        sa.Column("content_hash", sa.String(64), nullable=False),
        # Optimistic concurrency version. Starts at 1 for every new SCD row.
        # Expiry UPDATE uses WHERE version=$v; rowcount==0 means another replica won.
        sa.Column("version", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_current", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.PrimaryKeyConstraint("profile_id"),
    )
    # Partial index: fast current-version lookup; also enforces at-most-one current row per profile.
    op.create_index(
        "idx_profile_current",
        "resource_profile_versions",
        ["name", "namespace"],
        postgresql_where=sa.text("is_current = true"),
    )
    op.create_index(
        "idx_profile_history",
        "resource_profile_versions",
        ["name", "namespace", "valid_from"],
    )

    # Normalized field strategies — one row per managed field per version.
    op.create_table(
        "resource_profile_field_strategies",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("profile_id", sa.Uuid(), nullable=False),
        sa.Column("field_name", sa.String(253), nullable=False),
        sa.Column("locator", sa.String(512), nullable=True),
        # NULL algo = inherit profile-level default at runtime.
        sa.Column("algo", sa.String(64), nullable=True),
        sa.Column("algo_config", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("min_value", sa.String(64), nullable=True),
        sa.Column("max_value", sa.String(64), nullable=True),
        sa.ForeignKeyConstraint(
            ["profile_id"],
            ["resource_profile_versions.profile_id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_field_strategy_profile", "resource_profile_field_strategies", ["profile_id"])

    # Recommendation audit trail.
    op.create_table(
        "profile_recommendations",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("profile_id", sa.Uuid(), nullable=False),
        sa.Column("pod_name", sa.String(253), nullable=False),
        sa.Column("pod_namespace", sa.String(253), nullable=False),
        sa.Column("patches", postgresql.JSONB(), nullable=False),
        sa.Column(
            "recommended_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["profile_id"],
            ["resource_profile_versions.profile_id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx_recommendation_profile",
        "profile_recommendations",
        ["profile_id", "recommended_at"],
    )


def downgrade() -> None:
    op.drop_index("idx_recommendation_profile", table_name="profile_recommendations")
    op.drop_table("profile_recommendations")

    op.drop_index("idx_field_strategy_profile", table_name="resource_profile_field_strategies")
    op.drop_table("resource_profile_field_strategies")

    op.drop_index("idx_profile_history", table_name="resource_profile_versions")
    op.drop_index("idx_profile_current", table_name="resource_profile_versions")
    op.drop_table("resource_profile_versions")