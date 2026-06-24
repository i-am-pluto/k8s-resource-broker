from __future__ import annotations

import asyncio
from typing import Any

from structlog import get_logger

from resource_broker.common.dao.repositories.performance import PerformanceRepository
from resource_broker.common.k8s_units import parse_quantity
from resource_broker.performance_monitor.services.alert_sink import Alert, AlertSink, AlertType
from resource_broker.performance_monitor.services.k8s_adapter import K8sAdapter
from resource_broker.performance_monitor.services.metrics_adapter import MetricsAdapter

logger = get_logger(__name__)


def _pod_cpu_usage_query(namespace: str) -> str:
    return f'sum(rate(container_cpu_usage_seconds_total{{namespace="{namespace}",container!=""}}[1m])) by (pod)'


def _pod_mem_usage_query(namespace: str) -> str:
    return f'sum(container_memory_working_set_bytes{{namespace="{namespace}",container!=""}}) by (pod)'


class UsageCollector:
    def __init__(
        self,
        metrics_adapter: MetricsAdapter,
        k8s_adapter: K8sAdapter,
        alert_sink: AlertSink,
        repo: PerformanceRepository,
        namespaces: list[str],
        interval_seconds: int = 60,
        pressure_threshold: float = 0.85,
    ) -> None:
        self._metrics_adapter = metrics_adapter
        self._k8s_adapter = k8s_adapter
        self._alert_sink = alert_sink
        self._repo = repo
        self._namespaces = namespaces
        self._interval_seconds = interval_seconds
        self._pressure_threshold = pressure_threshold

    async def run_forever(self, shutdown_event: asyncio.Event | None = None) -> None:
        while True:
            if shutdown_event is not None and shutdown_event.is_set():
                logger.info("usage_collector shutting down")
                break

            for ns in self._namespaces:
                try:
                    await self._collect_namespace(ns)
                except Exception:
                    logger.exception("usage_collector error collecting namespace", namespace=ns)
                    continue

            await asyncio.sleep(self._interval_seconds)

    async def _collect_namespace(self, namespace: str) -> None:
        cpu_promql = _pod_cpu_usage_query(namespace)
        mem_promql = _pod_mem_usage_query(namespace)

        cpu_results = await self._metrics_adapter.query(cpu_promql)
        mem_results = await self._metrics_adapter.query(mem_promql)

        cpu_map: dict[str, float] = {}
        for sample in cpu_results:
            pod_name = sample.metric.get("pod", "")
            if pod_name:
                cpu_map[pod_name] = sample.value

        mem_map: dict[str, int] = {}
        for sample in mem_results:
            pod_name = sample.metric.get("pod", "")
            if pod_name:
                mem_map[pod_name] = int(sample.value)

        pod_names = set(cpu_map.keys()) | set(mem_map.keys())
        if not pod_names:
            return

        for pod_name in pod_names:
            try:
                info = await self._k8s_adapter.get_configured_resources(namespace, pod_name)
                status = await self._k8s_adapter.get_pod_status(namespace, pod_name)
            except Exception:
                logger.exception("usage_collector error fetching pod info", namespace=namespace, pod_name=pod_name)
                continue

            service_name = (
                info.labels.get("app")
                or info.labels.get("app.kubernetes.io/name")
                or pod_name
            )

            active_service_id = await self._repo.upsert_active_service(
                namespace=namespace,
                service_name=service_name,
                resource_type="k8s-pod",
            )

            active_pod_id = await self._repo.upsert_active_pod(
                active_service_id=active_service_id,
                pod_name=pod_name,
                configured_resource=info.configured_resource,
                pod_status=status.pod_status,
                restart_count=status.restart_count,
                last_terminated_reason=status.last_terminated_reason,
            )

            cpu_val = cpu_map.get(pod_name, 0.0)
            mem_val = mem_map.get(pod_name, 0)

            await self._repo.append_metric(
                active_pod_id=active_pod_id,
                cpu_usage_cores=cpu_val,
                mem_usage_bytes=mem_val,
            )

            await self._check_pressure(
                namespace=namespace,
                pod_name=pod_name,
                service_name=service_name,
                info=info,
                mem_usage_bytes=mem_val,
            )

    async def _check_pressure(
        self,
        namespace: str,
        pod_name: str,
        service_name: str | None,
        info: Any,
        mem_usage_bytes: int,
    ) -> None:
        configured = info.configured_resource
        memory_limit = configured.get("memory_limit")

        if memory_limit is None or memory_limit == "0":
            return

        try:
            mem_limit_bytes = int(parse_quantity(memory_limit))
        except (ValueError, TypeError):
            return

        if mem_limit_bytes <= 0:
            return

        ratio = mem_usage_bytes / mem_limit_bytes
        if ratio >= self._pressure_threshold:
            await self._alert_sink.emit(
                Alert(
                    alert_type=AlertType.PRESSURE,
                    namespace=namespace,
                    pod_name=pod_name,
                    service_name=service_name,
                    reason=f"memory pressure: {ratio:.1%} >= {self._pressure_threshold:.0%}",
                    details={
                        "mem_usage_bytes": mem_usage_bytes,
                        "mem_limit_bytes": mem_limit_bytes,
                        "ratio": ratio,
                    },
                )
            )
