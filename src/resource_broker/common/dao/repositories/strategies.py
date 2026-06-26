from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import delete, select, update
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
        """Persist or update a strategy snapshot.  Returns True when a row was written."""
        incoming_hash = _raw_hash(raw_crd)

        existing = (await self._session.execute(
            select(StrategySnapshotModel.id, StrategySnapshotModel.strategy_hash)
            .where(StrategySnapshotModel.strategy_name == strategy.name)
        )).one_or_none()

        if existing is not None and existing.strategy_hash == incoming_hash:
            return False

        now = datetime.now(UTC)
        if existing is not None:
            await self._session.execute(
                update(StrategySnapshotModel)
                .where(StrategySnapshotModel.id == existing.id)
                .values(
                    strategy_hash=incoming_hash,
                    strategy_info=raw_crd,
                    updated_at=now,
                )
            )
        else:
            self._session.add(StrategySnapshotModel(
                id=uuid.uuid4(),
                namespace="",
                strategy_name=strategy.name,
                strategy_hash=incoming_hash,
                strategy_info=raw_crd,
                updated_at=now,
            ))

        logger.debug("strategy snapshot written", name=strategy.name, algo=strategy.algo)
        return True

    async def remove(self, name: str) -> None:
        await self._session.execute(
            delete(StrategySnapshotModel).where(StrategySnapshotModel.strategy_name == name)
        )
        logger.debug("strategy snapshot removed", name=name)

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
