from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select, update as sa_update
from sqlalchemy.dialects.postgresql import insert as pg_insert
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

    Rows are never physically deleted.  is_active=False signals a CRD has been
    removed from Kubernetes; upsert reactivates it if the CRD is re-applied.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(self, raw_crd: dict[str, Any]) -> bool:
        """Persist or update a profile snapshot.  Returns True when a row was written.

        Uses INSERT ... ON CONFLICT DO UPDATE so concurrent replicas never race on
        first insert.  The DO UPDATE fires when the hash changed OR when the row was
        previously soft-deleted (is_active=False), reactivating it in one statement.
        """
        metadata = raw_crd.get("metadata", {})
        name = metadata.get("name", "")
        namespace = metadata.get("namespace", "")
        incoming_hash = _snapshot_hash(raw_crd)
        now = datetime.now(UTC)

        stmt = pg_insert(ProfileSnapshotModel).values(
            id=uuid.uuid4(),
            namespace=namespace,
            profile_name=name,
            profile_hash=incoming_hash,
            profile_info=raw_crd,
            is_active=True,
            updated_at=now,
        )
        do_update = stmt.on_conflict_do_update(
            constraint="uq_profile_snapshot",
            set_={
                "profile_hash": stmt.excluded.profile_hash,
                "profile_info": stmt.excluded.profile_info,
                "is_active": True,
                "updated_at": stmt.excluded.updated_at,
            },
            # Update when content changed OR when reactivating a soft-deleted row.
            where=(
                (ProfileSnapshotModel.profile_hash != stmt.excluded.profile_hash)
                | ~ProfileSnapshotModel.is_active
            ),
        )
        result = await self._session.execute(do_update)
        written = result.rowcount > 0
        if written:
            logger.debug("profile snapshot written", name=name, namespace=namespace)
        return written

    async def remove(self, name: str, namespace: str) -> None:
        """Soft-delete: set is_active=False rather than removing the row."""
        await self._session.execute(
            sa_update(ProfileSnapshotModel)
            .where(
                ProfileSnapshotModel.profile_name == name,
                ProfileSnapshotModel.namespace == namespace,
            )
            .values(is_active=False)
        )
        logger.debug("profile snapshot deactivated", name=name, namespace=namespace)

    async def delete_stale(self, current_names: set[str]) -> None:
        """Soft-delete snapshots for profiles no longer present in Kubernetes.

        Called after bootstrap upserts; marks rows inactive so they are not
        loaded by get_all() on the next k8s outage.
        """
        if not current_names:
            await self._session.execute(
                sa_update(ProfileSnapshotModel).values(is_active=False)
            )
            return
        await self._session.execute(
            sa_update(ProfileSnapshotModel)
            .where(ProfileSnapshotModel.profile_name.not_in(current_names))
            .values(is_active=False)
        )

    async def get_all(self) -> list[ResourceProfile]:
        """Reconstruct active ResourceProfile objects from stored jsonb snapshots."""
        result = await self._session.execute(
            select(ProfileSnapshotModel).where(ProfileSnapshotModel.is_active == True)  # noqa: E712
        )
        rows = result.scalars().all()
        profiles: list[ResourceProfile] = []
        for row in rows:
            try:
                profiles.append(ResourceProfile.from_crd(row.profile_info))
            except Exception:
                logger.exception("failed to reconstruct profile from snapshot", name=row.profile_name)
        return profiles
