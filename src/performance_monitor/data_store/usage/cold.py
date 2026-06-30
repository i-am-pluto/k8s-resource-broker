import json
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from ..connectors.object_store import S3ObjectStoreConnector
from .base import MetricSample, UsageStore


def _sample_to_dict(s: MetricSample) -> dict[str, Any]:
    return {
        "profile": s.profile,
        "resource_type": s.resource_type,
        "field": s.field,
        "value": s.value,
        "timestamp": s.timestamp.isoformat(),
    }


def _dict_to_sample(d: dict[str, Any]) -> MetricSample:
    return MetricSample(
        profile=d["profile"],
        resource_type=d["resource_type"],
        field=d["field"],
        value=d["value"],
        timestamp=datetime.fromisoformat(d["timestamp"]),
    )


class ObjectStoreUsageStore(UsageStore):
    def __init__(self, connector: S3ObjectStoreConnector, prefix: str = "usage/") -> None:
        self._connector = connector
        self._prefix = prefix

    def _key(self, profile: str, field: str, timestamp: datetime) -> str:
        date_part = timestamp.strftime("%Y/%m/%d")
        return f"{self._prefix}{profile}/{field}/{date_part}/{timestamp.isoformat()}.json"

    async def store(self, samples: Sequence[MetricSample]) -> None:
        client = self._connector.client
        bucket = self._connector.bucket
        for s in samples:
            body = json.dumps(_sample_to_dict(s))
            await client.put_object(
                Bucket=bucket,
                Key=self._key(s.profile, s.field, s.timestamp),
                Body=body,
                ContentType="application/json",
            )

    async def query(
        self,
        profile: str,
        field: str,
        start: datetime,
        end: datetime,
    ) -> list[MetricSample]:
        client = self._connector.client
        bucket = self._connector.bucket
        prefix = f"{self._prefix}{profile}/{field}/"
        results: list[MetricSample] = []

        paginator = client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key: str = obj["Key"]
                resp = await client.get_object(Bucket=bucket, Key=key)
                body = await resp["Body"].read()
                sample = _dict_to_sample(json.loads(body))
                if start <= sample.timestamp <= end:
                    results.append(sample)

        results.sort(key=lambda s: s.timestamp)
        return results
