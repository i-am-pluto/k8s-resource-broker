from __future__ import annotations

import threading
from typing import Any

from kubernetes import client as k8s_client
from structlog import get_logger

from resource_broker.common.dao.database import get_session
from resource_broker.common.dao.repositories.strategies import StrategyRepository
from resource_broker.common.models.strategy_crd import StrategyCRD

logger = get_logger(__name__)

STRATEGY_CRD_GROUP = "resource-broker.io"
STRATEGY_CRD_VERSION = "v1alpha1"
STRATEGY_CRD_PLURAL = "strategies"


class StrategyRegistry:
    """Per-replica in-memory store of Strategy CRDs.

    Mirrors ProfileRegistry in structure:
      bootstrap() → k8s API first, DB fallback (strategy_snapshots) if unavailable.
      upsert()/remove() → update in-memory dict; DB writes are the watch loop's
      responsibility (see strategy_watcher._persist_strategy_event).

    Thread-safe: a threading.Lock guards all dict mutations and snapshot reads so
    that executor-thread watch callbacks and event-loop resync tasks never race.
    """

    def __init__(self) -> None:
        self._strategies: dict[str, StrategyCRD] = {}
        self._lock = threading.Lock()

    async def bootstrap(self, api: k8s_client.CustomObjectsApi) -> None:
        """Populate the registry from the Kubernetes API, fall back to DB on failure."""
        try:
            result = api.list_cluster_custom_object(
                group=STRATEGY_CRD_GROUP,
                version=STRATEGY_CRD_VERSION,
                plural=STRATEGY_CRD_PLURAL,
            )
            items = result.get("items") or []
            pairs: list[tuple[StrategyCRD, dict[str, Any]]] = []
            for item in items:
                try:
                    strategy = StrategyCRD.from_crd(item)
                    with self._lock:
                        self._strategies[strategy.name] = strategy
                    pairs.append((strategy, item))
                except Exception:
                    logger.exception(
                        "failed to parse strategy during bootstrap",
                        item=item.get("metadata", {}).get("name"),
                    )
            logger.info("strategy registry bootstrapped from kubernetes", count=len(self._strategies))
            await self._seed_db(pairs)
        except Exception:
            logger.exception("failed to bootstrap strategies from kubernetes, falling back to db")
            await self.load_from_db()

    async def _seed_db(self, pairs: list[tuple[StrategyCRD, dict[str, Any]]]) -> None:
        """Write-through: persist current strategy state to the snapshot table.

        Also purges snapshots for strategies deleted while the broker was offline
        so they cannot be resurrected from the snapshot table on a future k8s outage.
        """
        try:
            current_names = {strategy.name for strategy, _ in pairs}
            async with get_session() as session:
                repo = StrategyRepository(session)
                written = 0
                for strategy, raw_crd in pairs:
                    if await repo.upsert(strategy, raw_crd):
                        written += 1
                await repo.delete_stale(current_names)
            logger.info("strategy db seed complete", total=len(pairs), written=written)
        except Exception:
            logger.exception("failed to seed strategy db on bootstrap")

    async def load_from_db(self) -> None:
        """Bootstrap fallback: populate registry from strategy_snapshots table."""
        try:
            async with get_session() as session:
                repo = StrategyRepository(session)
                strategies = await repo.get_all()
            with self._lock:
                for s in strategies:
                    self._strategies[s.name] = s
            logger.info("strategy registry loaded from db fallback", count=len(strategies))
        except Exception:
            logger.exception("failed to load strategies from db; strategy registry starts empty")

    def get(self, name: str) -> StrategyCRD | None:
        with self._lock:
            return self._strategies.get(name)

    def upsert(self, crd_obj: dict[str, Any]) -> StrategyCRD | None:
        try:
            strategy = StrategyCRD.from_crd(crd_obj)
            with self._lock:
                self._strategies[strategy.name] = strategy
            logger.debug("strategy registry updated", name=strategy.name, algo=strategy.algo)
            return strategy
        except Exception:
            logger.exception("failed to parse strategy for registry update")
            return None

    def remove(self, name: str) -> None:
        with self._lock:
            self._strategies.pop(name, None)
        logger.debug("strategy removed from registry", name=name)

    def all_strategies(self) -> list[StrategyCRD]:
        with self._lock:
            return list(self._strategies.values())
