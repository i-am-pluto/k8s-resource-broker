from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from structlog import get_logger

from resource_broker.common.dao.orm_models import (
    ActivePodModel,
    ActiveServiceModel,
    PodPerformanceMetricModel,
)

logger = get_logger(__name__)


class PerformanceRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert_active_service(
        self,
        namespace: str,
        service_name: str,
        resource_type: str,
    ) -> uuid.UUID:
        """Select by (namespace, service_name). If found, update resource_type
        if changed and return its id. If not found, insert a new row (new uuid4)
        and return its id. Natural key: (namespace, service_name)."""

        # Try to find existing row
        result = await self._session.execute(
            select(ActiveServiceModel)
            .where(
                ActiveServiceModel.namespace == namespace,
                ActiveServiceModel.service_name == service_name,
            )
        )
        existing = result.scalar_one_or_none()

        if existing:
            # Update resource_type if different
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

        # Insert new row
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

    async def upsert_active_pod(
        self,
        active_service_id: uuid.UUID,
        pod_name: str,
        configured_resource: dict[str, Any],
        pod_status: str,
        restart_count: int = 0,
        last_terminated_reason: str | None = None,
    ) -> uuid.UUID:
        """Select by (active_service_id, pod_name). If found, update ALL of
        configured_resource/pod_status/restart_count/last_terminated_reason and
        return its id. If not found, insert a new row and return its id.
        Used by UsageCollector — writes every column."""

        # Try to find existing row
        result = await self._session.execute(
            select(ActivePodModel)
            .where(
                ActivePodModel.active_service_id == active_service_id,
                ActivePodModel.pod_name == pod_name,
            )
        )
        existing = result.scalar_one_or_none()

        if existing:
            # Update all mutable columns
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

        # Insert new row
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

    async def update_pod_status_only(
        self,
        active_service_id: uuid.UUID,
        pod_name: str,
        pod_status: str,
        restart_count: int,
        last_terminated_reason: str | None,
    ) -> uuid.UUID:
        """Select by (active_service_id, pod_name). If found, update ONLY
        pod_status/restart_count/last_terminated_reason — do NOT touch
        configured_resource (leave whatever is already there). If not found,
        insert a new row with configured_resource={} as a placeholder (since
        this method's caller, StatusWatcher, doesn't have configured-resource
        data — that's UsageCollector's job to fill in on its own pass).
        Returns the row id. This lets StatusWatcher and UsageCollector both
        write to active_pods without clobbering each other's columns."""

        # Try to find existing row
        result = await self._session.execute(
            select(ActivePodModel)
            .where(
                ActivePodModel.active_service_id == active_service_id,
                ActivePodModel.pod_name == pod_name,
            )
        )
        existing = result.scalar_one_or_none()

        if existing:
            # Update only status-related columns
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

        # Insert new row with empty configured_resource placeholder
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

    async def append_metric(
        self,
        active_pod_id: uuid.UUID,
        cpu_usage_cores: float,
        mem_usage_bytes: int,
        scraped_at: datetime | None = None,
    ) -> None:
        """Pure append — session.add(PodPerformanceMetricModel(...)), then flush.
        Pass scraped_at through only if given (let the column's server_default
        apply otherwise)."""

        metric = PodPerformanceMetricModel(
            id=uuid.uuid4(),
            active_pod_id=active_pod_id,
            cpu_usage_cores=cpu_usage_cores,
            mem_usage_bytes=mem_usage_bytes,
        )
        if scraped_at is not None:
            metric.scraped_at = scraped_at
        self._session.add(metric)
        await self._session.flush()
        logger.debug(
            "appended performance metric",
            active_pod_id=str(active_pod_id),
            cpu_usage_cores=cpu_usage_cores,
            mem_usage_bytes=mem_usage_bytes,
        )

    async def get_usage(
        self,
        namespace: str,
        service_name: str,
        lookback_hours: int = 24,
    ) -> dict[str, float]:
        """percentile_cont (0.50/0.90/0.95) over cpu_usage_cores and
        mem_usage_bytes, joined pod_performance_metric -> active_pods ->
        active_services, filtered to the given (namespace, service_name) and
        scraped_at >= now - lookback_hours, grouped implicitly (no GROUP BY
        needed since percentile_cont over the whole filtered set is one row).
        Returns {} if no matching rows (row is None or row[0] is None).
        Otherwise returns {"cpu_p50": ..., "cpu_p90": ..., "cpu_p95": ...,
        "mem_p50": ..., "mem_p90": ..., "mem_p95": ...} as floats.
        NOTE: percentile_cont is a Postgres-specific aggregate — sqlite does
        not support it. This method's own unit test (see below) should
        either skip on sqlite or you should note this as a concern; don't
        change the DB engine or fixture to work around it — just flag it."""

        cutoff_time = datetime.now(UTC).replace(microsecond=0)
        cutoff_time = cutoff_time - timedelta(hours=lookback_hours)

        result = await self._session.execute(
            select(
                func.percentile_cont(0.50).within_group(PodPerformanceMetricModel.cpu_usage_cores).label("cpu_p50"),
                func.percentile_cont(0.90).within_group(PodPerformanceMetricModel.cpu_usage_cores).label("cpu_p90"),
                func.percentile_cont(0.95).within_group(PodPerformanceMetricModel.cpu_usage_cores).label("cpu_p95"),
                func.percentile_cont(0.50).within_group(PodPerformanceMetricModel.mem_usage_bytes).label("mem_p50"),
                func.percentile_cont(0.90).within_group(PodPerformanceMetricModel.mem_usage_bytes).label("mem_p90"),
                func.percentile_cont(0.95).within_group(PodPerformanceMetricModel.mem_usage_bytes).label("mem_p95"),
            )
            .select_from(PodPerformanceMetricModel)
            .join(ActivePodModel)
            .join(ActiveServiceModel)
            .where(
                ActiveServiceModel.namespace == namespace,
                ActiveServiceModel.service_name == service_name,
                PodPerformanceMetricModel.scraped_at >= cutoff_time,
            )
        )
        row = result.one_or_none()

        if row is None or row[0] is None:
            return {}

        return {
            "cpu_p50": row.cpu_p50,
            "cpu_p90": row.cpu_p90,
            "cpu_p95": row.cpu_p95,
            "mem_p50": row.mem_p50,
            "mem_p90": row.mem_p90,
            "mem_p95": row.mem_p95,
        }

    async def get_status(
        self,
        namespace: str,
        service_name: str,
    ) -> list[dict[str, Any]]:
        """Plain select over active_pods joined to active_services filtered to
        (namespace, service_name). Returns list of dicts with pod_name,
        pod_status, restart_count, last_terminated_reason."""

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
        """Plain select over active_pods joined to active_services filtered to
        (namespace, service_name). Returns list of dicts with pod_name,
        configured_resource."""

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
