from __future__ import annotations

import asyncio
from typing import Any

from kubernetes import client as k8s_client, watch as k8s_watch
from structlog import get_logger

from resource_broker.common.dao.database import get_session
from resource_broker.common.dao.repositories.profiles import ProfileRepository
from resource_broker.common.models.profile import ResourceProfile
from resource_broker.common.services.profile_registry import CRD_GROUP, CRD_PLURAL, CRD_VERSION
from resource_broker.common.services.recommendation_service import RecommendationService

logger = get_logger(__name__)


async def run_crd_watch_loop(
    api: k8s_client.CustomObjectsApi,
    svc: RecommendationService,
) -> None:
    """Background task: watch ResourceProfile CRDs, keep registry + cache + DB in sync."""
    loop = asyncio.get_running_loop()
    while True:
        try:
            await loop.run_in_executor(None, _watch_once, api, svc, loop)
        except Exception:
            logger.exception("crd watch loop crashed, restarting in 5s")
            await asyncio.sleep(5)


def _watch_once(
    api: k8s_client.CustomObjectsApi,
    svc: RecommendationService,
    loop: asyncio.AbstractEventLoop,
) -> None:
    watcher = k8s_watch.Watch()
    logger.info("crd watch started", group=CRD_GROUP, plural=CRD_PLURAL)
    try:
        for event in watcher.stream(
            api.list_cluster_custom_object,
            group=CRD_GROUP,
            version=CRD_VERSION,
            plural=CRD_PLURAL,
        ):
            _handle_event(event, svc, loop)
    except Exception:
        logger.exception("crd watch stream error")
        raise


def _handle_event(
    event: dict[str, Any],
    svc: RecommendationService,
    loop: asyncio.AbstractEventLoop,
) -> None:
    event_type: str = event.get("type", "")
    obj: dict[str, Any] = event.get("object", {})
    metadata = obj.get("metadata", {}) or {}
    name = metadata.get("name", "unknown")
    namespace = metadata.get("namespace", "default")

    profile: ResourceProfile | None = None
    try:
        if event_type in ("ADDED", "MODIFIED"):
            profile = svc.registry.upsert(obj)
            if event_type == "MODIFIED":
                svc.invalidate(name, namespace)
                logger.info("profile modified, cache invalidated", name=name, namespace=namespace)
        elif event_type == "DELETED":
            svc.registry.remove(name, namespace)
            svc.invalidate(name, namespace)
            logger.info("profile deleted, removed from registry", name=name, namespace=namespace)
    except Exception:
        logger.exception("error handling crd event", event_type=event_type, name=name)

    # Fire-and-forget DB persist — a DB failure never blocks the watcher.
    asyncio.run_coroutine_threadsafe(
        _persist_event(event_type, profile, name, namespace),
        loop,
    )


async def _persist_event(
    event_type: str,
    profile: ResourceProfile | None,
    name: str,
    namespace: str,
) -> None:
    """Write CRD event to DB asynchronously.

    ADDED/MODIFIED → record_version():
      - Content hash check first: if profile content unchanged, zero DB write.
      - Advisory lock: if another replica is concurrently writing this profile,
        this replica skips (returns None). The winning replica handles the write.
      - Only when content actually changed does a new SCD Type 2 row get inserted.

    DELETED → soft_expire(): sets is_current=False, valid_to=now().
      History rows are preserved; no data is deleted.
    """
    try:
        async with get_session() as session:
            repo = ProfileRepository(session)
            if event_type in ("ADDED", "MODIFIED") and profile is not None:
                await repo.record_version(profile)
            elif event_type == "DELETED":
                await repo.soft_expire(name, namespace)
    except Exception:
        logger.exception("failed to persist profile event to db", event_type=event_type, name=name)