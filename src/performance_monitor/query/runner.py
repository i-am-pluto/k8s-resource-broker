from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from generated.models import MetricConfig, ResourceTypeCRD

from ..adapters.base import PromQLAdapter, Sample
from ..data_store.usage.base import MetricSample, UsageStore
from ..data_store.watermark.base import WatermarkStore
from .config import PromQLRunnerConfig
from .resolver import QueryResolver

logger = logging.getLogger(__name__)


class PromQLRunner:
    def __init__(
        self,
        adapter: PromQLAdapter,
        resolver: QueryResolver,
        watermark: WatermarkStore,
        usage: UsageStore,
        config: PromQLRunnerConfig | None = None,
    ) -> None:
        self._adapter = adapter
        self._resolver = resolver
        self._watermark = watermark
        self._usage = usage
        self._config = config or PromQLRunnerConfig()

    async def fetch_resource_type(
        self,
        profile: str,
        resource_type: ResourceTypeCRD,
        context: dict[str, str],
    ) -> list[MetricSample]:
        if not resource_type.fields:
            return []

        tasks: list[asyncio.Task[list[MetricSample]]] = []
        for field_name, field_def in resource_type.fields.items():
            metrics = field_def.metrics
            is_usage = (
                metrics is not None
                and metrics.usage is not None
            ) or get_cadvisor_default_safe(field_name, "usage") is not None
            is_configured = (
                metrics is not None
                and metrics.configured is not None
            ) or get_cadvisor_default_safe(field_name, "configured") is not None

            if is_usage:
                tasks.append(
                    asyncio.ensure_future(
                        self._fetch_and_store(
                            profile,
                            resource_type.name,
                            field_name,
                            "usage",
                            metrics.usage if metrics else None,
                            context,
                        )
                    )
                )
            if is_configured:
                tasks.append(
                    asyncio.ensure_future(
                        self._fetch_and_store(
                            profile,
                            resource_type.name,
                            field_name,
                            "configured",
                            metrics.configured if metrics else None,
                            context,
                        )
                    )
                )

        if not tasks:
            return []

        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_samples: list[MetricSample] = []
        for r in results:
            if isinstance(r, BaseException):
                logger.error("PromQL fetch task failed", exc_info=r)
            else:
                all_samples.extend(r)
        return all_samples

    async def _fetch_and_store(
        self,
        profile: str,
        resource_type: str,
        field: str,
        query_type: str,
        metric_config: MetricConfig | None,
        context: dict[str, str],
    ) -> list[MetricSample]:
        watermark_key = f"{profile}/{resource_type}/{field}/{query_type}"

        start = await self._resolve_start(watermark_key)
        end = datetime.now(timezone.utc)

        promql = self._resolver.resolve(metric_config, field, query_type, context)
        if promql is None:
            logger.debug(
                "No PromQL for %s/%s/%s/%s — skipping",
                profile, resource_type, field, query_type,
            )
            return []

        samples = await self._query_with_retry(promql, start, end)

        metric_samples = [
            MetricSample(
                profile=profile,
                resource_type=resource_type,
                field=f"{field}.{query_type}",
                value=s.value,
                timestamp=s.timestamp,
            )
            for s in samples
        ]

        if metric_samples:
            await self._usage.store(metric_samples)

        await self._watermark.set(watermark_key, end)

        return metric_samples

    async def _resolve_start(self, watermark_key: str) -> datetime:
        wm = await self._watermark.get(watermark_key)
        if wm is not None:
            return wm
        return datetime.now(timezone.utc) - timedelta(days=self._config.default_lookback_days)

    async def _query_with_retry(
        self,
        promql: str,
        start: datetime,
        end: datetime,
    ) -> list[Sample]:
        last_exc: Exception | None = None
        for attempt in range(self._config.retry_attempts):
            try:
                return await self._adapter.query_range(
                    promql,
                    start=start,
                    end=end,
                    step=self._config.range_step,
                )
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "Query attempt %d/%d failed: %s",
                    attempt + 1,
                    self._config.retry_attempts,
                    exc,
                )
                if attempt + 1 < self._config.retry_attempts:
                    await asyncio.sleep(self._config.retry_backoff_seconds * (attempt + 1))

        raise RuntimeError(
            f"PromQL query failed after {self._config.retry_attempts} attempts"
        ) from last_exc


def get_cadvisor_default_safe(field: str, query_type: str) -> str | None:
    from .cadvisor import get_cadvisor_default

    return get_cadvisor_default(field, query_type)
