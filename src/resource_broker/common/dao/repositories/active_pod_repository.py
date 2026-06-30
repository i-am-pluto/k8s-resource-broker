"""Repository for the active_pods table.

Two UPSERT code paths exist:
  - upsert(): writes all columns (configured_resource, status, etc.).
  - update_status_only(): touches only status-related columns, leaving
    configured_resource intact — enables StatusWatcher and UsageCollector
    to write independently without clobbering each other's data.

Also exposes read methods that join to active_services:
  - get_status()
  - get_configured_resources()
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from structlog import get_logger

from resource_broker.common.dao.orm_models import ActivePodModel, ActiveServiceModel

logger = get_logger(__name__)


class ActivePodRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(
        self,
        active_service_id: uuid.UUID,
        pod_name: str,
        configured_resource: dict[str, Any],
        pod_status: str,
        restart_count: int = 0,
        last_terminated_reason: str | None = None,
    ) -> uuid.UUID:
        result = await self._session.execute(
            select(ActivePodModel)
            .where(
                ActivePodModel.active_service_id == active_service_id,
                ActivePodModel.pod_name == pod_name,
            )
        )
        existing = result.scalar_one_or_none()

        if existing:
            existing.configured_resource = configured_resource
            existing.pod_status = pod_status
            existing.restart_count = restart_count
            existing.last_terminated_reason = last_terminated_reason
            logger.debug(
                "updated active_pod",
                id=str(existing.id),
                active_service_id=str(active_service_id),
                pod_name=pod_name,
                pod_status=pod_status,
            )
            await self._session.flush()
            return existing.id

        new_id = uuid.uuid4()
        new_pod = ActivePodModel(
            id=new_id,
            active_service_id=active_service_id,
            pod_name=pod_name,
            configured_resource=configured_resource,
            pod_status=pod_status,
            restart_count=restart_count,
            last_terminated_reason=last_terminated_reason,
        )
        self._session.add(new_pod)
        await self._session.flush()
        logger.info(
            "created active_pod",
            id=str(new_id),
            active_service_id=str(active_service_id),
            pod_name=pod_name,
            pod_status=pod_status,
        )
        return new_id

    async def update_status_only(
        self,
        active_service_id: uuid.UUID,
        pod_name: str,
        pod_status: str,
        restart_count: int,
        last_terminated_reason: str | None,
    ) -> uuid.UUID:
        result = await self._session.execute(
            select(ActivePodModel)
            .where(
                ActivePodModel.active_service_id == active_service_id,
                ActivePodModel.pod_name == pod_name,
            )
        )
        existing = result.scalar_one_or_none()

        if existing:
            existing.pod_status = pod_status
            existing.restart_count = restart_count
            existing.last_terminated_reason = last_terminated_reason
            logger.debug(
                "updated active_pod status only",
                id=str(existing.id),
                active_service_id=str(active_service_id),
                pod_name=pod_name,
                pod_status=pod_status,
            )
            await self._session.flush()
            return existing.id

        new_id = uuid.uuid4()
        new_pod = ActivePodModel(
            id=new_id,
            active_service_id=active_service_id,
            pod_name=pod_name,
            configured_resource={},
            pod_status=pod_status,
            restart_count=restart_count,
            last_terminated_reason=last_terminated_reason,
        )
        self._session.add(new_pod)
        await self._session.flush()
        logger.info(
            "created active_pod (status_only)",
            id=str(new_id),
            active_service_id=str(active_service_id),
            pod_name=pod_name,
            pod_status=pod_status,
        )
        return new_id

    async def get_status(
        self,
        namespace: str,
        service_name: str,
    ) -> list[dict[str, Any]]:
        result = await self._session.execute(
            select(
                ActivePodModel.pod_name,
                ActivePodModel.pod_status,
                ActivePodModel.restart_count,
                ActivePodModel.last_terminated_reason,
            )
            .join(ActiveServiceModel)
            .where(
                ActiveServiceModel.namespace == namespace,
                ActiveServiceModel.service_name == service_name,
            )
        )
        rows = result.all()
        return [
            {
                "pod_name": row.pod_name,
                "pod_status": row.pod_status,
                "restart_count": row.restart_count,
                "last_terminated_reason": row.last_terminated_reason,
            }
            for row in rows
        ]

    async def get_configured_resources(
        self,
        namespace: str,
        service_name: str,
    ) -> list[dict[str, Any]]:
        result = await self._session.execute(
            select(
                ActivePodModel.pod_name,
                ActivePodModel.configured_resource,
            )
            .join(ActiveServiceModel)
            .where(
                ActiveServiceModel.namespace == namespace,
                ActiveServiceModel.service_name == service_name,
            )
        )
        rows = result.all()
        return [
            {
                "pod_name": row.pod_name,
                "configured_resource": row.configured_resource,
            }
            for row in rows
        ]
