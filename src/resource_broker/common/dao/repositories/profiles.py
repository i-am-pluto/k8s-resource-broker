from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from structlog import get_logger

from resource_broker.common.dao.orm_models import ProfileSnapshotModel
from resource_broker.common.models.profile import ResourceProfile

logger = get_logger(__name__)


def _snapshot_hash(raw_crd: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(raw_crd, sort_keys=True).encode()).hexdigest()


class ProfileSnapshotRepository:
    """CRUD for the profile_snapshots table.

    Stores the full raw CRD dict as jsonb for cold-start bootstrap when the
    Kubernetes API server is unavailable.  Write-through on every bootstrap
    and watch event; hash-gated to suppress redundant DB writes.
    Profile CRDs are cluster-scoped; namespace is stored as the empty string.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(self, raw_crd: dict[str, Any]) -> bool:
        """Persist or update a profile snapshot.  Returns True when a row was written."""
        metadata = raw_crd.get("metadata", {})
        name = metadata.get("name", "")
        namespace = metadata.get("namespace", "")
        incoming_hash = _snapshot_hash(raw_crd)

        existing = (await self._session.execute(
            select(ProfileSnapshotModel.id, ProfileSnapshotModel.profile_hash)
            .where(
                ProfileSnapshotModel.profile_name == name,
                ProfileSnapshotModel.namespace == namespace,
            )
        )).one_or_none()

        if existing is not None and existing.profile_hash == incoming_hash:
            return False

        now = datetime.now(UTC)
        if existing is not None:
            await self._session.execute(
                update(ProfileSnapshotModel)
                .where(ProfileSnapshotModel.id == existing.id)
                .values(profile_hash=incoming_hash, profile_info=raw_crd, updated_at=now)
            )
        else:
            self._session.add(ProfileSnapshotModel(
                id=uuid.uuid4(),
                namespace=namespace,
                profile_name=name,
                profile_hash=incoming_hash,
                profile_info=raw_crd,
                updated_at=now,
            ))

        logger.debug("profile snapshot written", name=name, namespace=namespace)
        return True

    async def remove(self, name: str, namespace: str) -> None:
        from sqlalchemy import delete as sa_delete
        await self._session.execute(
            sa_delete(ProfileSnapshotModel).where(
                ProfileSnapshotModel.profile_name == name,
                ProfileSnapshotModel.namespace == namespace,
            )
        )
        logger.debug("profile snapshot removed", name=name, namespace=namespace)

    async def get_all(self) -> list[ResourceProfile]:
        """Reconstruct all ResourceProfile objects from stored jsonb snapshots."""
        result = await self._session.execute(select(ProfileSnapshotModel))
        rows = result.scalars().all()
        profiles: list[ResourceProfile] = []
        for row in rows:
            try:
                profiles.append(ResourceProfile.from_crd(row.profile_info))
            except Exception:
                logger.exception("failed to reconstruct profile from snapshot", name=row.profile_name)
        return profiles
