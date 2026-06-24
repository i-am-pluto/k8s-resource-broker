from __future__ import annotations

from typing import Any

from resource_broker.common.dao.repositories.performance import PerformanceRepository
from resource_broker.performance_monitor.services.k8s_adapter import K8sAdapter, PodStatusInfo


class PerformanceReadApi:
    def __init__(
        self,
        repo: PerformanceRepository,
        k8s_adapter: K8sAdapter | None = None,
    ) -> None:
        self._repo = repo
        self._k8s_adapter = k8s_adapter

    async def get_usage(
        self,
        namespace: str,
        service_name: str,
        lookback_hours: int = 24,
    ) -> dict[str, float]:
        return await self._repo.get_usage(namespace, service_name, lookback_hours)

    async def get_status(
        self,
        namespace: str,
        service_name: str,
    ) -> list[dict[str, Any]]:
        return await self._repo.get_status(namespace, service_name)

    async def get_configured_resources(
        self,
        namespace: str,
        service_name: str,
    ) -> list[dict[str, Any]]:
        return await self._repo.get_configured_resources(namespace, service_name)

    async def get_live_status(
        self,
        namespace: str,
        pod_name: str,
    ) -> PodStatusInfo | None:
        if self._k8s_adapter is None:
            return None
        return await self._k8s_adapter.get_pod_status(namespace, pod_name)
