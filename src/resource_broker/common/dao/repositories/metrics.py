from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from resource_broker.common.dao.orm_models import PodMetricModel


class MetricsRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def bulk_insert(self, metrics: list[PodMetricModel]) -> list[PodMetricModel]:
        self._session.add_all(metrics)
        await self._session.flush()
        return metrics

    async def get_for_profile(
        self,
        profile_name: str,
        since: datetime | None = None,
        limit: int = 1000,
    ) -> list[PodMetricModel]:
        stmt = select(PodMetricModel).where(PodMetricModel.profile_name == profile_name)
        if since:
            stmt = stmt.where(PodMetricModel.scraped_at >= since)
        stmt = stmt.order_by(PodMetricModel.scraped_at.desc()).limit(limit)
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_percentiles(
        self,
        profile_name: str,
        lookback_hours: int = 24,
    ) -> dict[str, float]:
        since = datetime.utcnow() - timedelta(hours=lookback_hours)
        stmt = select(
            func.percentile_cont(0.50).within_group(PodMetricModel.cpu_usage_cores).label("cpu_p50"),
            func.percentile_cont(0.75).within_group(PodMetricModel.cpu_usage_cores).label("cpu_p75"),
            func.percentile_cont(0.90).within_group(PodMetricModel.cpu_usage_cores).label("cpu_p90"),
            func.percentile_cont(0.95).within_group(PodMetricModel.cpu_usage_cores).label("cpu_p95"),
            func.percentile_cont(0.50).within_group(PodMetricModel.mem_usage_bytes).label("mem_p50"),
            func.percentile_cont(0.75).within_group(PodMetricModel.mem_usage_bytes).label("mem_p75"),
            func.percentile_cont(0.90).within_group(PodMetricModel.mem_usage_bytes).label("mem_p90"),
            func.percentile_cont(0.95).within_group(PodMetricModel.mem_usage_bytes).label("mem_p95"),
        ).where(
            PodMetricModel.profile_name == profile_name,
            PodMetricModel.scraped_at >= since,
            PodMetricModel.cpu_usage_cores.isnot(None),
            PodMetricModel.mem_usage_bytes.isnot(None),
        )
        result = await self._session.execute(stmt)
        row = result.one_or_none()
        if row is None or row[0] is None:
            return {}
        return {
            "cpu_p50": float(row[0]),
            "cpu_p75": float(row[1]),
            "cpu_p90": float(row[2]),
            "cpu_p95": float(row[3]),
            "mem_p50": float(row[4]),
            "mem_p75": float(row[5]),
            "mem_p90": float(row[6]),
            "mem_p95": float(row[7]),
        }
