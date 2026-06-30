from typing import Any

from .object_store import S3ObjectStoreConnector
from .postgres import PostgresConnector


def create_postgres_connector(
    dsn: str,
    pool_size: int = 5,
    max_overflow: int = 10,
) -> PostgresConnector:
    return PostgresConnector(dsn=dsn, pool_size=pool_size, max_overflow=max_overflow)


def create_object_store_connector(
    bucket: str,
    endpoint_url: str | None = None,
    region: str = "us-east-1",
    access_key_id: str | None = None,
    secret_access_key: str | None = None,
) -> S3ObjectStoreConnector:
    return S3ObjectStoreConnector(
        bucket=bucket,
        endpoint_url=endpoint_url,
        region=region,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
    )


_Backends = dict[str, Any]


def create_connector(
    backend: str,
    **kwargs: Any,
) -> Any:
    backends: _Backends = {
        "postgres": create_postgres_connector,
        "s3": create_object_store_connector,
    }
    factory = backends.get(backend)
    if factory is None:
        msg = f"Unknown connector backend: {backend!r} (choices: {', '.join(backends)})"
        raise ValueError(msg)
    return factory(**kwargs)
