from __future__ import annotations

import asyncio
from typing import Any

from kubernetes import client as k8s_client, watch as k8s_watch
from structlog import get_logger

from resource_broker.common.dao.database import get_session
from resource_broker.common.dao.repositories.strategies import StrategyRepository
from resource_broker.common.models.profile import ResourceProfile
from resource_broker.common.models.strategy_crd import StrategyCRD
from resource_broker.common.services.profile_registry import ProfileRegistry
from resource_broker.common.services.recommendation_service import RecommendationService
from resource_broker.common.services.strategy_registry import (
    STRATEGY_CRD_GROUP,
    STRATEGY_CRD_PLURAL,
    STRATEGY_CRD_VERSION,
    StrategyRegistry,
)

logger = get_logger(__name__)


async def run_strategy_watch_loop(
    api: k8s_client.CustomObjectsApi,
    strategy_registry: StrategyRegistry,
    recommendation_svc: RecommendationService,
) -> None:
    """Background task: watch Strategy CRDs, keep in-memory registry + DB in sync."""
    loop = asyncio.get_running_loop()
    while True:
        try:
            await loop.run_in_executor(
                None,
                _watch_strategy_once,
                api,
                strategy_registry,
                recommendation_svc,
                loop,
            )
        except Exception:
            logger.exception("strategy watch loop crashed, restarting in 5s")
            await asyncio.sleep(5)


def _watch_strategy_once(
    api: k8s_client.CustomObjectsApi,
    strategy_registry: StrategyRegistry,
    recommendation_svc: RecommendationService,
    loop: asyncio.AbstractEventLoop,
) -> None:
    watcher = k8s_watch.Watch()
    logger.info("strategy crd watch started", group=STRATEGY_CRD_GROUP, plural=STRATEGY_CRD_PLURAL)
    try:
        for event in watcher.stream(
            api.list_cluster_custom_object,
            group=STRATEGY_CRD_GROUP,
            version=STRATEGY_CRD_VERSION,
            plural=STRATEGY_CRD_PLURAL,
        ):
            _handle_strategy_event(event, strategy_registry, recommendation_svc, loop)
    except Exception:
        logger.exception("strategy crd watch stream error")
        raise


def _handle_strategy_event(
    event: dict[str, Any],
    strategy_registry: StrategyRegistry,
    recommendation_svc: RecommendationService,
    loop: asyncio.AbstractEventLoop,
) -> None:
    event_type: str = event.get("type", "")
    obj: dict[str, Any] = event.get("object", {})
    name = (obj.get("metadata") or {}).get("name", "unknown")

    strategy: StrategyCRD | None = None
    try:
        if event_type in ("ADDED", "MODIFIED"):
            strategy = strategy_registry.upsert(obj)
            if event_type == "MODIFIED" and strategy is not None:
                # Invalidate recommendation cache for every profile that references
                # this strategy so the next request recomputes with the new config.
                _invalidate_for_strategy(name, recommendation_svc)
                logger.info("strategy modified, cache invalidated", name=name)
        elif event_type == "DELETED":
            _invalidate_for_strategy(name, recommendation_svc)
            strategy_registry.remove(name)
            logger.info("strategy deleted, cache invalidated, removed from registry", name=name)
    except Exception:
        logger.exception("error handling strategy event", event_type=event_type, name=name)

    asyncio.run_coroutine_threadsafe(
        _persist_strategy_event(event_type, strategy, obj, name),
        loop,
    )


def _invalidate_for_strategy(
    strategy_name: str,
    recommendation_svc: RecommendationService,
) -> None:
    """Invalidate cached recommendations for all profiles that reference this strategy."""
    for profile in recommendation_svc.registry.all_profiles():
        if _profile_uses_strategy(profile, strategy_name):
            recommendation_svc.invalidate(profile.name, profile.namespace)


def _profile_uses_strategy(profile: ResourceProfile, strategy_name: str) -> bool:
    if profile.strategy and profile.strategy.algo == strategy_name:
        return True
    return any(
        entry.strategy and entry.strategy.algo == strategy_name
        for entry in profile.fields.values()
    )


async def _persist_strategy_event(
    event_type: str,
    strategy: StrategyCRD | None,
    raw_obj: dict[str, Any],
    name: str,
) -> None:
    """Write-through: persist strategy watch events to strategy_snapshots table."""
    try:
        async with get_session() as session:
            repo = StrategyRepository(session)
            if event_type in ("ADDED", "MODIFIED") and strategy is not None:
                await repo.upsert(strategy, raw_obj)
            elif event_type == "DELETED":
                await repo.remove(name)
    except Exception:
        logger.exception("failed to persist strategy event to db", event_type=event_type, name=name)
