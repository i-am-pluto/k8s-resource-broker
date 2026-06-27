from __future__ import annotations

import asyncio
from typing import Any

from kubernetes import client as k8s_client, watch as k8s_watch
from structlog import get_logger

from resource_broker.common.dao.database import get_session
from resource_broker.common.dao.repositories.profiles import ProfileSnapshotRepository
from resource_broker.common.models.profile import ResourceProfile
from resource_broker.common.services.profile_registry import CRD_GROUP, CRD_PLURAL, CRD_VERSION
from resource_broker.common.services.recommendation_service import RecommendationService
from resource_broker.common.services.strategy_registry import (
    STRATEGY_CRD_GROUP,
    STRATEGY_CRD_PLURAL,
    STRATEGY_CRD_VERSION,
    StrategyRegistry,
)
from resource_broker.config import settings

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
    namespace = metadata.get("namespace", "")

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

    asyncio.run_coroutine_threadsafe(
        _persist_event(event_type, profile, obj, name, namespace),
        loop,
    )


async def _persist_event(
    event_type: str,
    profile: ResourceProfile | None,
    raw_obj: dict[str, Any],
    name: str,
    namespace: str,
) -> None:
    """Write CRD event to the snapshot table asynchronously.

    A DB failure never blocks the watcher.

    ADDED/MODIFIED: upsert snapshot (hash-gated, no-op if unchanged).
    DELETED:        remove snapshot row.
    """
    try:
        async with get_session() as session:
            snap_repo = ProfileSnapshotRepository(session)
            if event_type in ("ADDED", "MODIFIED") and profile is not None:
                await snap_repo.upsert(raw_obj)
            elif event_type == "DELETED":
                await snap_repo.remove(name, namespace)
    except Exception:
        logger.exception("failed to persist profile event to db", event_type=event_type, name=name)


async def run_resync_loop(
    api: k8s_client.CustomObjectsApi,
    svc: RecommendationService,
    strategy_registry: StrategyRegistry,
) -> None:
    """Periodic full re-list of both Profile and Strategy CRDs.

    Reconciles any events that the watch streams may have missed (e.g., after a
    reconnect gap).  Runs every settings.resync_interval_seconds (default 1 hour).
    """
    interval = settings.resync_interval_seconds
    logger.info("resync loop started", interval_seconds=interval)
    while True:
        await asyncio.sleep(interval)
        try:
            await _resync_profiles(api, svc)
        except Exception:
            logger.exception("profile resync failed")
        try:
            await _resync_strategies(api, strategy_registry, svc)
        except Exception:
            logger.exception("strategy resync failed")


async def _resync_profiles(
    api: k8s_client.CustomObjectsApi,
    svc: RecommendationService,
) -> None:
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None,
        lambda: api.list_cluster_custom_object(
            group=CRD_GROUP, version=CRD_VERSION, plural=CRD_PLURAL,
        ),
    )
    items = result.get("items") or []
    seen: set[str] = set()
    for item in items:
        meta = item.get("metadata", {}) or {}
        name = meta.get("name", "unknown")
        namespace = meta.get("namespace", "")
        seen.add(name)
        already_known = svc.registry.get(name) is not None
        profile = svc.registry.upsert(item)
        if profile is not None:
            if already_known:
                # Treat as MODIFIED — invalidate stale cached recommendations.
                svc.invalidate(name, namespace)
            async with get_session() as session:
                await ProfileSnapshotRepository(session).upsert(item)
    # Expire in-memory entries that have disappeared from the API
    for profile in list(svc.registry.all_profiles()):
        if profile.name not in seen:
            svc.registry.remove(profile.name, profile.namespace)
            svc.invalidate(profile.name, profile.namespace)
            async with get_session() as session:
                await ProfileSnapshotRepository(session).remove(profile.name, profile.namespace)
    logger.info("profile resync complete", count=len(items))


async def _resync_strategies(
    api: k8s_client.CustomObjectsApi,
    strategy_registry: StrategyRegistry,
    svc: RecommendationService,
) -> None:
    from resource_broker.common.dao.repositories.strategies import StrategyRepository
    from resource_broker.common.models.strategy_crd import StrategyCRD
    from resource_broker.watcher.controllers.strategy_watcher import _invalidate_for_strategy

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None,
        lambda: api.list_cluster_custom_object(
            group=STRATEGY_CRD_GROUP,
            version=STRATEGY_CRD_VERSION,
            plural=STRATEGY_CRD_PLURAL,
        ),
    )
    items = result.get("items") or []
    seen: set[str] = set()
    for item in items:
        name = (item.get("metadata") or {}).get("name", "unknown")
        seen.add(name)
        already_known = strategy_registry.get(name) is not None
        strategy = strategy_registry.upsert(item)
        if strategy is not None:
            if already_known:
                _invalidate_for_strategy(name, svc)
            async with get_session() as session:
                await StrategyRepository(session).upsert(strategy, item)
    for strategy in list(strategy_registry.all_strategies()):
        if strategy.name not in seen:
            strategy_registry.remove(strategy.name)
            _invalidate_for_strategy(strategy.name, svc)
            async with get_session() as session:
                await StrategyRepository(session).remove(strategy.name)
    logger.info("strategy resync complete", count=len(items))
