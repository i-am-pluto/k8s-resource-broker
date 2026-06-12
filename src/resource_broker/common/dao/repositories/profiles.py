from __future__ import annotations

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from resource_broker.common.dao.orm_models import ProfileModel
from resource_broker.common.models.profile import FieldEntry, ResourceProfile


class ProfileRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(self, profile: ResourceProfile) -> None:
        """Insert or update a profile snapshot. Clears deleted_at so restored CRDs become active."""
        fields_dict = {
            name: {k: v for k, v in {
                "locator": entry.locator,
                "min": entry.min,
                "max": entry.max,
                "strategy": entry.strategy,
            }.items() if v is not None}
            for name, entry in profile.fields.items()
        }
        stmt = pg_insert(ProfileModel).values(
            name=profile.name,
            namespace=profile.namespace,
            resource_type=profile.resource_type,
            mode=profile.mode,
            strategy=profile.strategy,
            fields=fields_dict,
            updated_at=func.now(),
            deleted_at=None,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["name", "namespace"],
            set_={
                "resource_type": stmt.excluded.resource_type,
                "mode": stmt.excluded.mode,
                "strategy": stmt.excluded.strategy,
                "fields": stmt.excluded.fields,
                "updated_at": func.now(),
                "deleted_at": None,
            },
        )
        await self._session.execute(stmt)

    async def soft_delete(self, name: str, namespace: str) -> None:
        """Mark a profile deleted without removing the row — preserves audit history."""
        stmt = (
            update(ProfileModel)
            .where(ProfileModel.name == name, ProfileModel.namespace == namespace)
            .values(deleted_at=func.now())
        )
        await self._session.execute(stmt)

    async def get(self, name: str, namespace: str) -> ResourceProfile | None:
        stmt = select(ProfileModel).where(
            ProfileModel.name == name,
            ProfileModel.namespace == namespace,
            ProfileModel.deleted_at.is_(None),
        )
        result = await self._session.execute(stmt)
        row = result.scalar_one_or_none()
        return _to_domain(row) if row else None

    async def get_all_active(self) -> list[ResourceProfile]:
        """Returns all non-deleted profiles — used as bootstrap fallback."""
        stmt = select(ProfileModel).where(ProfileModel.deleted_at.is_(None))
        result = await self._session.execute(stmt)
        return [_to_domain(row) for row in result.scalars().all()]


def _to_domain(row: ProfileModel) -> ResourceProfile:
    fields_raw: dict = row.fields or {}
    parsed_fields = {
        name: FieldEntry(
            locator=entry.get("locator"),
            min=entry.get("min"),
            max=entry.get("max"),
            strategy=entry.get("strategy"),
        )
        for name, entry in fields_raw.items()
    }
    return ResourceProfile(
        name=row.name,
        namespace=row.namespace,
        resource_type=row.resource_type,
        mode=row.mode,
        strategy=row.strategy,
        fields=parsed_fields,
    )