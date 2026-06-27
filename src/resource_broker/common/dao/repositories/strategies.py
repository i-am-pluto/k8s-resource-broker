from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import delete as sa_delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from structlog import get_logger

from resource_broker.common.dao.orm_models import StrategySnapshotModel
from resource_broker.common.models.strategy_crd import StrategyCRD

logger = get_logger(__name__)


def _raw_hash(raw_crd: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(raw_crd, sort_keys=True).encode()).hexdigest()


class StrategyRepository:
    """CRUD for the strategy_snapshots table.

    This is a simple current-state store (not SCD).  Every write is hash-gated:
    if the incoming CRD bytes match the stored hash, no DB write occurs.
    Strategy CRDs are cluster-scoped so namespace is always stored as "".
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(self, strategy: StrategyCRD, raw_crd: dict[str, Any]) -> bool:
        """Persist or update a strategy snapshot.  Returns True when a row was written.

        Uses INSERT ... ON CONFLICT DO UPDATE to avoid a select-then-insert race
        between concurrent replicas handling the same CRD event.
        """
        incoming_hash = _raw_hash(raw_crd)
        now = datetime.now(UTC)

        stmt = pg_insert(StrategySnapshotModel).values(
            id=uuid.uuid4(),
            namespace="",
            strategy_name=strategy.name,
            strategy_hash=incoming_hash,
            strategy_info=raw_crd,
            updated_at=now,
        )
        do_update = stmt.on_conflict_do_update(
            constraint="uq_strategy_snapshot",
            set_={
                "strategy_hash": stmt.excluded.strategy_hash,
                "strategy_info": stmt.excluded.strategy_info,
                "updated_at": stmt.excluded.updated_at,
            },
            where=StrategySnapshotModel.strategy_hash != stmt.excluded.strategy_hash,
        )
        result = await self._session.execute(do_update)
        written = result.rowcount > 0
        if written:
            logger.debug("strategy snapshot written", name=strategy.name, algo=strategy.algo)
        return written

    async def remove(self, name: str) -> None:
        await self._session.execute(
            sa_delete(StrategySnapshotModel).where(StrategySnapshotModel.strategy_name == name)
        )
        logger.debug("strategy snapshot removed", name=name)

    async def delete_stale(self, current_names: set[str]) -> None:
        """Remove snapshots for strategies that no longer exist in Kubernetes.

        Called after bootstrap upserts so that strategies deleted while the broker
        was offline are not resurrected on the next k8s outage.
        """
        if not current_names:
            await self._session.execute(sa_delete(StrategySnapshotModel))
            return
        await self._session.execute(
            sa_delete(StrategySnapshotModel).where(
                StrategySnapshotModel.strategy_name.not_in(current_names)
            )
        )

    async def get_all(self) -> list[StrategyCRD]:
        """Reconstruct all Strategy objects from stored jsonb snapshots."""
        result = await self._session.execute(select(StrategySnapshotModel))
        rows = result.scalars().all()
        strategies: list[StrategyCRD] = []
        for row in rows:
            try:
                strategies.append(StrategyCRD.from_crd(row.strategy_info))
            except Exception:
                logger.exception("failed to reconstruct strategy from snapshot", name=row.strategy_name)
        return strategies
