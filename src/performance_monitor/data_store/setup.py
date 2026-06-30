from dataclasses import dataclass, field
from datetime import timedelta

from .connectors.base import StoreConnector
from .connectors.factory import (
    create_object_store_connector,
    create_postgres_connector,
)
from .usage.base import UsageStore
from .usage.cold import ObjectStoreUsageStore
from .usage.hot import PostgresUsageStore
from .usage.hybrid import HybridUsageStore
from .watermark.base import WatermarkStore
from .watermark.postgres import PostgresWatermarkStore


@dataclass
class DataStore:
    watermark: WatermarkStore
    usage: UsageStore
    connectors: list[StoreConnector] = field(default_factory=list)


@dataclass
class DataStoreConfig:
    pg_dsn: str = ""
    pg_pool_size: int = 5
    pg_max_overflow: int = 10
    usage_ttl_days: int = 30
    object_store_bucket: str = ""
    object_store_endpoint: str | None = None
    object_store_region: str = "us-east-1"
    object_store_access_key: str | None = None
    object_store_secret_key: str | None = None
    cold_threshold_days: int = 7
    usage_backend: str = "postgres"


async def setup_data_store(cfg: DataStoreConfig) -> DataStore:
    pg_connector = create_postgres_connector(
        dsn=cfg.pg_dsn,
        pool_size=cfg.pg_pool_size,
        max_overflow=cfg.pg_max_overflow,
    )
    await pg_connector.connect()

    watermark = PostgresWatermarkStore(pg_connector)

    if cfg.usage_backend == "postgres":
        usage: UsageStore = PostgresUsageStore(
            pg_connector,
            ttl_days=cfg.usage_ttl_days,
        )
        return DataStore(
            watermark=watermark,
            usage=usage,
            connectors=[pg_connector],
        )

    if cfg.usage_backend in ("object_store", "hybrid"):
        obj_connector = create_object_store_connector(
            bucket=cfg.object_store_bucket,
            endpoint_url=cfg.object_store_endpoint,
            region=cfg.object_store_region,
            access_key_id=cfg.object_store_access_key,
            secret_access_key=cfg.object_store_secret_key,
        )
        await obj_connector.connect()

        if cfg.usage_backend == "object_store":
            usage = ObjectStoreUsageStore(obj_connector)
            return DataStore(
                watermark=watermark,
                usage=usage,
                connectors=[pg_connector, obj_connector],
            )

        hot = PostgresUsageStore(pg_connector, ttl_days=cfg.usage_ttl_days)
        cold = ObjectStoreUsageStore(obj_connector)
        usage = HybridUsageStore(hot, cold, cold_threshold=timedelta(days=cfg.cold_threshold_days))
        return DataStore(
            watermark=watermark,
            usage=usage,
            connectors=[pg_connector, obj_connector],
        )

    msg = f"Unknown usage backend: {cfg.usage_backend!r}"
    raise ValueError(msg)


async def teardown_data_store(ds: DataStore) -> None:
    for c in ds.connectors:
        await c.close()
