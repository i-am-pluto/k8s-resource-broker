from __future__ import annotations

import hashlib
import json
import struct
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select, update
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


def _advisory_lock_key(name: str, namespace: str) -> int:
    """Stable int64 keyed to (namespace, name) — suitable for pg_try_advisory_xact_lock."""
    digest = hashlib.sha256(f"{namespace}/{name}".encode()).digest()
    return struct.unpack(">q", digest[:8])[0]


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
        """SCD Type 2 insert for a profile.

        Returns:
            The active profile_id (new or existing) if the write was handled.
            None if another replica holds the advisory lock — the caller should skip.

        Distributed safety:
            pg_try_advisory_xact_lock prevents two replicas from racing on the
            same (name, namespace) pair. The lock is transaction-scoped — released
            automatically on commit or rollback.

        Idempotency:
            A content_hash check ensures that repeated delivery of the same CRD
            event (k8s watch reconnects, bootstrap re-list) produces no DB write.
        """
        incoming_hash = _content_hash(profile)
        lock_key = _advisory_lock_key(profile.name, profile.namespace)

        locked: bool = await self._session.scalar(
            select(func.pg_try_advisory_xact_lock(lock_key))
        )
        if not locked:
            logger.debug(
                "advisory lock not acquired; another replica is writing this profile",
                name=profile.name,
                namespace=profile.namespace,
            )
            return None

        # Check whether the profile has actually changed.
        current = (await self._session.execute(
            select(ProfileVersionModel.content_hash, ProfileVersionModel.profile_id)
            .where(
                ProfileVersionModel.name == profile.name,
                ProfileVersionModel.namespace == profile.namespace,
                ProfileVersionModel.is_current.is_(True),
            )
        )).one_or_none()

        if current is not None and current.content_hash == incoming_hash:
            logger.debug("profile unchanged, skipping SCD insert", name=profile.name)
            return current.profile_id

        now = datetime.now(UTC)

        # Expire the existing current version (if any).
        if current is not None:
            await self._session.execute(
                update(ProfileVersionModel)
                .where(
                    ProfileVersionModel.name == profile.name,
                    ProfileVersionModel.namespace == profile.namespace,
                    ProfileVersionModel.is_current.is_(True),
                )
                .values(is_current=False, valid_to=now)
            )

        # Build the new version.
        default_algo_dict = profile.strategy.to_dict() if profile.strategy else None
        new_id = uuid.uuid4()
        version = ProfileVersionModel(
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
            valid_from=now,
            valid_to=None,
            is_current=True,
        )
        self._session.add(version)

        # Insert normalized field rows (one per managed field).
        for field_name, entry in profile.fields.items():
            s_dict = entry.strategy.to_dict() if entry.strategy else {}
            self._session.add(ProfileFieldStrategyModel(
                id=uuid.uuid4(),
                profile_id=new_id,
                field_name=field_name,
                locator=entry.locator,
                # NULL algo = inherit profile-level default at runtime.
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