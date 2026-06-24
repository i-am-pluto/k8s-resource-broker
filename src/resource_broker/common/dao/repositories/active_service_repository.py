"""Repository for the active_services table.

Provides upsert semantics keyed on the natural key (namespace, service_name).
Used by StatusWatcher and UsageCollector to register discovered services
before inserting pod or metric rows that reference them.
"""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from structlog import get_logger

from resource_broker.common.dao.orm_models import ActiveServiceModel

logger = get_logger(__name__)


class ActiveServiceRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(
        self,
        namespace: str,
        service_name: str,
        resource_type: str,
    ) -> uuid.UUID:
        result = await self._session.execute(
            select(ActiveServiceModel)
            .where(
                ActiveServiceModel.namespace == namespace,
                ActiveServiceModel.service_name == service_name,
            )
        )
        existing = result.scalar_one_or_none()

        if existing:
            if existing.resource_type != resource_type:
                existing.resource_type = resource_type
                logger.debug(
                    "updated active_service resource_type",
                    id=str(existing.id),
                    namespace=namespace,
                    service_name=service_name,
                    resource_type=resource_type,
                )
            await self._session.flush()
            return existing.id

        new_id = uuid.uuid4()
        new_service = ActiveServiceModel(
            id=new_id,
            namespace=namespace,
            service_name=service_name,
            resource_type=resource_type,
        )
        self._session.add(new_service)
        await self._session.flush()
        logger.info(
            "created active_service",
            id=str(new_id),
            namespace=namespace,
            service_name=service_name,
            resource_type=resource_type,
        )
        return new_id
