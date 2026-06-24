"""Repository for the pod_performance_metric table.

Provides append-only metric insertion and percentile-based usage queries
that join through active_pods and active_services to filter by
(namespace, service_name).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from structlog import get_logger

from resource_broker.common.dao.orm_models import (
    ActivePodModel,
    ActiveServiceModel,
    PodPerformanceMetricModel,
)

logger = get_logger(__name__)


class PodPerformanceMetricRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def append(
        self,
        active_pod_id: uuid.UUID,
        cpu_usage_cores: float,
        mem_usage_bytes: int,
        scraped_at: datetime | None = None,
    ) -> None:
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
