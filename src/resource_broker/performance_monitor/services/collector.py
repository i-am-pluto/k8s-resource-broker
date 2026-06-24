from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from kubernetes import client as k8s_client
from structlog import get_logger

from resource_broker.broker_api.services.profile_loader import CRD_GROUP, CRD_PLURAL, CRD_VERSION
from resource_broker.common.config import settings
from resource_broker.common.dao.database import get_session
from resource_broker.common.dao.orm_models import PodMetricModel
from resource_broker.common.dao.repositories.metrics import MetricsRepository
from resource_broker.common.k8s_client import create_k8s_api
from resource_broker.performance_monitor.services.metrics_adapter import MetricsAdapter

logger = get_logger(__name__)


def pod_cpu_usage(namespace: str = "", pod: str = "", container: str = "") -> str:
    selectors = [f'namespace="{namespace}"'] if namespace else []
    if pod:
        selectors.append(f'pod="{pod}"')
    if container:
        selectors.append(f'container="{container}"')

    sel_str = ",".join(selectors)
    return f"rate(container_cpu_usage_seconds_total{{{sel_str}}}[5m])"


def pod_mem_usage(namespace: str = "", pod: str = "", container: str = "") -> str:
    selectors = [f'namespace="{namespace}"'] if namespace else []
    if pod:
        selectors.append(f'pod="{pod}"')
    if container:
        selectors.append(f'container="{container}"')

    sel_str = ",".join(selectors)
    return f"container_memory_working_set_bytes{{{sel_str}}}"


class MetricsCollector:
    def __init__(self, adapter: MetricsAdapter) -> None:
        self._adapter = adapter

    async def run_forever(self) -> None:
        logger.info("metrics collector started", interval=settings.scraper_interval_seconds)
        while True:
            try:
                await self._collect()
            except Exception as exc:
                logger.error("metrics collection failed", error=str(exc), exc_info=True)
            await asyncio.sleep(settings.scraper_interval_seconds)

    async def _collect(self) -> None:
        api = create_k8s_api(k8s_client.CustomObjectsApi)
        try:
            crd_list = api.list_namespaced_custom_object(
                group=CRD_GROUP,
                version=CRD_VERSION,
                namespace=settings.k8s_namespace,
                plural=CRD_PLURAL,
            )
        except Exception as exc:
            logger.error("failed to list profiles for collection", error=str(exc))
            return

        profile_names = []
        for item in crd_list.get("items", []):
            meta = item.get("metadata", {})
            profile_names.append(meta.get("name", "unknown"))

        if not profile_names:
            return

        for name in profile_names:
            try:
                await self._collect_for_profile(name)
            except Exception as exc:
                logger.error("collection error for profile", profile=name, error=str(exc))

    async def _collect_for_profile(self, profile_name: str) -> None:
        cpu_q = pod_cpu_usage()
        mem_q = pod_mem_usage()

        cpu_results = await self._adapter.query(cpu_q)
        mem_results = await self._adapter.query(mem_q)

        mem_map: dict[tuple[str, str, str], float] = {}
        for r in mem_results:
            key = (r.metric.get("namespace", ""), r.metric.get("pod", ""), r.metric.get("container", ""))
            mem_map[key] = r.value

        metrics: list[PodMetricModel] = []
        now = datetime.now(UTC)

        for r in cpu_results:
            ns = r.metric.get("namespace", "")
            pod = r.metric.get("pod", "")
            ctr = r.metric.get("container", "")
            mem_val = mem_map.get((ns, pod, ctr))

            metrics.append(
                PodMetricModel(
                    profile_name=profile_name,
                    namespace=ns,
                    pod_name=pod,
                    container=ctr,
                    cpu_usage_cores=r.value,
                    mem_usage_bytes=int(mem_val) if mem_val else None,
                    scraped_at=now,
                )
            )

        if metrics:
            async with get_session() as session:
                metrics_repo = MetricsRepository(session)
                await metrics_repo.bulk_insert(metrics)
            logger.info("metrics stored", profile=profile_name, count=len(metrics))
