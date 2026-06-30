from collections.abc import Sequence
from datetime import datetime, timedelta

from .base import MetricSample, UsageStore


class HybridUsageStore(UsageStore):
    def __init__(
        self,
        hot: UsageStore,
        cold: UsageStore,
        cold_threshold: timedelta = timedelta(days=7),
    ) -> None:
        self._hot = hot
        self._cold = cold
        self._cold_threshold = cold_threshold

    async def store(self, samples: Sequence[MetricSample]) -> None:
        hot_samples: list[MetricSample] = []
        cold_samples: list[MetricSample] = []
        now = datetime.utcnow()

        for s in samples:
            if now - s.timestamp <= self._cold_threshold:
                hot_samples.append(s)
            else:
                cold_samples.append(s)

        if hot_samples:
            await self._hot.store(hot_samples)
        if cold_samples:
            await self._cold.store(cold_samples)

    async def query(
        self,
        profile: str,
        field: str,
        start: datetime,
        end: datetime,
    ) -> list[MetricSample]:
        results = await self._hot.query(profile, field, start, end)
        results.extend(await self._cold.query(profile, field, start, end))
        results.sort(key=lambda s: s.timestamp)
        return results
