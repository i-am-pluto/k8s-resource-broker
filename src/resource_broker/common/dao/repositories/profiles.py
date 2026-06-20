from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from structlog import get_logger

from resource_broker.common.dao.orm_models import (
    ProfileFieldStrategyModel,
    ProfileRecommendationModel,
    ProfileVersionModel,
)
from resource_broker.common.models.profile import FieldEntry, FieldStrategy, ResourceProfile

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _content_hash(profile: ResourceProfile) -> str:
    """SHA-256 of the canonical profile definition. Same hash ↔ no meaningful change."""
    canonical: dict[str, Any] = {
        "rt": profile.resource_type,
        "mode": profile.mode,
        "ds": profile.strategy.to_dict() if profile.strategy else None,
        "fields": {
            k: {
                "loc": e.locator,
                "min": e.min,
                "max": e.max,
                "s": e.strategy.to_dict() if e.strategy else None,
            }
            for k, e in sorted(profile.fields.items())
        },
    }
    return hashlib.sha256(json.dumps(canonical, sort_keys=True).encode()).hexdigest()


def _to_domain(row: ProfileVersionModel) -> ResourceProfile:
    default_strategy: FieldStrategy | None = None
    if row.default_algo:
        d: dict[str, Any] = {"algo": row.default_algo, **(row.default_algo_config or {})}
        default_strategy = FieldStrategy.from_dict(d)

    fields: dict[str, FieldEntry] = {}
    for fs in row.field_strategies:
        field_strategy: FieldStrategy | None = None
        if fs.algo:
            fd: dict[str, Any] = {"algo": fs.algo, **(fs.algo_config or {})}
            field_strategy = FieldStrategy.from_dict(fd)
        fields[fs.field_name] = FieldEntry(
            locator=fs.locator,
            min=fs.min_value,
            max=fs.max_value,
            strategy=field_strategy,
        )

    return ResourceProfile(
        name=row.name,
        namespace=row.namespace,
        resource_type=row.resource_type,
        mode=row.mode,
        strategy=default_strategy,
        fields=fields,
    )


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------

class ProfileRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def record_version(self, profile: ResourceProfile) -> uuid.UUID | None:
        """SCD Type 2 insert for a profile using optimistic concurrency control.

        Returns:
            The active profile_id (new or existing) if this replica handled the write.
            None if another replica won the optimistic race — the caller should skip.

        Two-layer protection:

        1. Content hash (idempotency): if the profile content has not changed since
           the last DB row, return the existing profile_id with zero writes. Handles
           repeated delivery of the same CRD event (watch reconnects, bootstrap re-list).

        2. Optimistic version column (distributed race): when content HAS changed, expire
           the current row with WHERE version = $known_version. PostgreSQL row-level locking
           ensures exactly one replica's UPDATE affects 1 row; all others see rowcount == 0
           and skip. No explicit lock acquisition or infrastructure needed.
        """
        incoming_hash = _content_hash(profile)

        # Read the current version (hash + version number for optimistic check).
        current = (await self._session.execute(
            select(
                ProfileVersionModel.profile_id,
                ProfileVersionModel.content_hash,
                ProfileVersionModel.version,
            )
            .where(
                ProfileVersionModel.name == profile.name,
                ProfileVersionModel.namespace == profile.namespace,
                ProfileVersionModel.is_current.is_(True),
            )
        )).one_or_none()

        # Layer 1: content unchanged → nothing to do.
        if current is not None and current.content_hash == incoming_hash:
            logger.debug("profile unchanged, skipping SCD insert", name=profile.name)
            return current.profile_id

        now = datetime.now(UTC)

        # Layer 2: expire current row using the known version as an optimistic guard.
        # If another replica already expired this row, rowcount == 0 and we skip.
        if current is not None:
            result = await self._session.execute(
                update(ProfileVersionModel)
                .where(
                    ProfileVersionModel.name == profile.name,
                    ProfileVersionModel.namespace == profile.namespace,
                    ProfileVersionModel.is_current.is_(True),
                    ProfileVersionModel.version == current.version,
                )
                .values(is_current=False, valid_to=now)
            )
            if result.rowcount == 0:
                logger.debug(
                    "optimistic version conflict: another replica already wrote this profile",
                    name=profile.name,
                    namespace=profile.namespace,
                )
                return None

        # We won the race (or this is the first ever version). Insert the new row.
        default_algo_dict = profile.strategy.to_dict() if profile.strategy else None
        new_id = uuid.uuid4()
        self._session.add(ProfileVersionModel(
            profile_id=new_id,
            name=profile.name,
            namespace=profile.namespace,
            resource_type=profile.resource_type,
            mode=profile.mode,
            default_algo=default_algo_dict.get("algo") if default_algo_dict else None,
            default_algo_config=(
                {k: v for k, v in default_algo_dict.items() if k != "algo"}
                if default_algo_dict else None
            ),
            content_hash=incoming_hash,
            version=1,
            valid_from=now,
            valid_to=None,
            is_current=True,
        ))

        for field_name, entry in profile.fields.items():
            s_dict = entry.strategy.to_dict() if entry.strategy else {}
            self._session.add(ProfileFieldStrategyModel(
                id=uuid.uuid4(),
                profile_id=new_id,
                field_name=field_name,
                locator=entry.locator,
                algo=s_dict.get("algo") if s_dict else None,
                algo_config={k: v for k, v in s_dict.items() if k != "algo"} if s_dict else {},
                min_value=entry.min,
                max_value=entry.max,
            ))

        await self._session.flush()
        logger.info(
            "profile version recorded",
            name=profile.name,
            namespace=profile.namespace,
            profile_id=str(new_id),
            previous_id=str(current.profile_id) if current else None,
        )
        return new_id

    async def soft_expire(self, name: str, namespace: str) -> None:
        """Expire the current version when the CRD is deleted. Keeps full history."""
        await self._session.execute(
            update(ProfileVersionModel)
            .where(
                ProfileVersionModel.name == name,
                ProfileVersionModel.namespace == namespace,
                ProfileVersionModel.is_current.is_(True),
            )
            .values(is_current=False, valid_to=datetime.now(UTC))
        )
        logger.debug("profile version expired", name=name, namespace=namespace)

    async def get_current(self, name: str, namespace: str) -> ResourceProfile | None:
        result = await self._session.execute(
            select(ProfileVersionModel)
            .where(
                ProfileVersionModel.name == name,
                ProfileVersionModel.namespace == namespace,
                ProfileVersionModel.is_current.is_(True),
            )
            .options(selectinload(ProfileVersionModel.field_strategies))
        )
        row = result.scalar_one_or_none()
        return _to_domain(row) if row else None

    async def get_all_current(self) -> list[ResourceProfile]:
        """All active profiles — used as bootstrap fallback when k8s API is unavailable."""
        result = await self._session.execute(
            select(ProfileVersionModel)
            .where(ProfileVersionModel.is_current.is_(True))
            .options(selectinload(ProfileVersionModel.field_strategies))
        )
        return [_to_domain(row) for row in result.scalars().all()]

    async def record_recommendation(
        self,
        profile_id: uuid.UUID,
        pod_name: str,
        pod_namespace: str,
        patches: list[dict[str, Any]],
    ) -> None:
        """Append an audit record: which profile version produced which patches for which pod."""
        self._session.add(ProfileRecommendationModel(
            id=uuid.uuid4(),
            profile_id=profile_id,
            pod_name=pod_name,
            pod_namespace=pod_namespace,
            patches=patches,
        ))
        await self._session.flush()
