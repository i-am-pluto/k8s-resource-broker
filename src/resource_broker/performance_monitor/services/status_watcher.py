from __future__ import annotations

import asyncio
from typing import Any

from structlog import get_logger

from resource_broker.common.dao.repositories.performance import PerformanceRepository
from resource_broker.performance_monitor.services.alert_sink import Alert, AlertSink, AlertType
from resource_broker.performance_monitor.services.k8s_adapter import K8sAdapter

logger = get_logger(__name__)


class StatusWatcher:
    def __init__(
        self,
        k8s_adapter: K8sAdapter,
        alert_sink: AlertSink,
        repo: PerformanceRepository,
        namespace: str = "",
    ) -> None:
        self._k8s_adapter = k8s_adapter
        self._alert_sink = alert_sink
        self._repo = repo
        self._namespace = namespace

    async def run(self, shutdown_event: asyncio.Event | None = None) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            self._watch_blocking,
            loop,
            shutdown_event,
        )

    def _watch_blocking(self, loop: asyncio.AbstractEventLoop, shutdown_event: asyncio.Event | None) -> None:
        try:
            for event in self._k8s_adapter.watch_pods(self._namespace):
                if shutdown_event is not None and shutdown_event.is_set():
                    break
                asyncio.run_coroutine_threadsafe(
                    self._handle_event(event),
                    loop,
                )
        except Exception:
            logger.exception("watch_pods stream failed")

    async def _handle_event(self, event: dict[str, Any]) -> None:
        try:
            pod = event["object"]
            pod_name = pod.metadata.name
            namespace = pod.metadata.namespace
            pod_phase = pod.status.phase or "Unknown"
            container_statuses = pod.status.container_statuses or []

            restart_count = sum(cs.restart_count for cs in container_statuses if cs.restart_count)
            last_terminated_reason: str | None = None
            for cs in container_statuses:
                if cs.last_state and cs.last_state.terminated and cs.last_state.terminated.reason:
                    last_terminated_reason = cs.last_state.terminated.reason
                    break

            labels = pod.metadata.labels or {}
            service_name = labels.get("app") or labels.get("app.kubernetes.io/name") or pod_name

            active_service_id = await self._repo.upsert_active_service(
                namespace=namespace,
                service_name=service_name,
                resource_type="k8s-pod",
            )
            await self._repo.update_pod_status_only(
                active_service_id=active_service_id,
                pod_name=pod_name,
                pod_status=pod_phase,
                restart_count=restart_count,
                last_terminated_reason=last_terminated_reason,
            )

            if last_terminated_reason == "OOMKilled":
                await self._alert_sink.emit(Alert(
                    alert_type=AlertType.FAILURE,
                    namespace=namespace,
                    pod_name=pod_name,
                    service_name=service_name,
                    reason="OOMKilled",
                    details={"restart_count": restart_count, "pod_status": pod_phase},
                ))
        except Exception:
            logger.exception("failed to handle watch event")
