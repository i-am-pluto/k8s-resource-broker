from __future__ import annotations

import asyncio
from typing import Any

from kubernetes import client as k8s_client, watch as k8s_watch
from structlog import get_logger

from resource_broker.common.services.profile_registry import CRD_GROUP, CRD_PLURAL, CRD_VERSION
from resource_broker.common.services.recommendation_service import RecommendationService

logger = get_logger(__name__)


async def run_crd_watch_loop(
    api: k8s_client.CustomObjectsApi,
    svc: RecommendationService,
) -> None:
    """Background task: watch ResourceProfile CRDs, keep registry + cache in sync."""
    loop = asyncio.get_event_loop()
    while True:
        try:
            await loop.run_in_executor(None, _watch_once, api, svc)
        except Exception:
            logger.exception("crd watch loop crashed, restarting in 5s")
            await asyncio.sleep(5)


def _watch_once(api: k8s_client.CustomObjectsApi, svc: RecommendationService) -> None:
    watcher = k8s_watch.Watch()
    logger.info("crd watch started", group=CRD_GROUP, plural=CRD_PLURAL)
    try:
        for event in watcher.stream(
            api.list_cluster_custom_object,
            group=CRD_GROUP,
            version=CRD_VERSION,
            plural=CRD_PLURAL,
        ):
            _handle_event(event, svc)
    except Exception:
        logger.exception("crd watch stream error")
        raise


def _handle_event(event: dict[str, Any], svc: RecommendationService) -> None:
    event_type: str = event.get("type", "")
    obj: dict[str, Any] = event.get("object", {})
    metadata = obj.get("metadata", {}) or {}
    name = metadata.get("name", "unknown")
    namespace = metadata.get("namespace", "default")

    if event_type in ("ADDED", "MODIFIED"):
        svc.registry.upsert(obj)
        if event_type == "MODIFIED":
            svc.invalidate(name, namespace)
            logger.info("profile modified, cache invalidated", name=name, namespace=namespace)
    elif event_type == "DELETED":
        svc.registry.remove(name, namespace)
        svc.invalidate(name, namespace)
        logger.info("profile deleted, removed from registry", name=name, namespace=namespace)
