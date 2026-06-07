from __future__ import annotations

import asyncio
from typing import Any

from kubernetes import client as k8s_client
from kubernetes import watch
from structlog import get_logger

from resource_broker.common.k8s_client import create_k8s_api
from resource_broker.common.services.metrics_adapter import MetricsAdapter
from resource_broker.common.services.profile_loader import ProfileLoader
from resource_broker.config import settings
from resource_broker.watcher.services.collector import MetricsCollector
from resource_broker.watcher.services.patcher import compute_patches

logger = get_logger(__name__)


class PodWatcher:
    def __init__(self, adapter: MetricsAdapter) -> None:
        self._core_api = create_k8s_api(k8s_client.CoreV1Api)
        self._profile_loader = ProfileLoader()
        self._collector = MetricsCollector(adapter)

    async def run(self) -> None:
        logger.info("pod watcher started", namespace=settings.k8s_namespace)
        collector_task = asyncio.create_task(self._collector.run_forever())
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, self._watch_pods)
        finally:
            collector_task.cancel()
            await self._collector._adapter.close()

    def _watch_pods(self) -> None:
        w = watch.Watch()
        for event in w.stream(
            self._core_api.list_namespaced_pod,
            namespace=settings.k8s_namespace,
            timeout_seconds=0,
        ):
            asyncio.run_coroutine_threadsafe(
                self._handle_event(event),
                asyncio.get_event_loop(),
            )

    async def _handle_event(self, event: dict[str, Any]) -> None:
        obj = event.get("object", {})
        evt_type = event.get("type", "")

        if evt_type != "ADDED":
            return

        profile = await self._profile_loader.find_for_pod(obj)
        if profile is None:
            return

        pod_name = obj.get("metadata", {}).get("name", "unknown")
        logger.info("found profile for pod", pod=pod_name, profile=profile.name, mode=profile.mode)

        patches = await compute_patches(profile, obj)
        if not patches:
            return

        if profile.is_enforce_mode():
            try:
                self._core_api.patch_namespaced_pod(
                    name=pod_name,
                    namespace=obj.get("metadata", {}).get("namespace", "default"),
                    body=patches,
                )
                logger.info("pod patched (enforce mode)", pod=pod_name, patches=len(patches))
            except Exception as exc:
                logger.error("failed to patch pod", pod=pod_name, error=str(exc))
        else:
            logger.info(
                "recommendation computed (mode: recommendation)",
                pod=pod_name,
                profile=profile.name,
                patches=patches,
            )
