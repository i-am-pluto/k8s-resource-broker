from typing import Any

from .base import StoreConnector


class S3ObjectStoreConnector(StoreConnector):
    def __init__(
        self,
        bucket: str,
        endpoint_url: str | None = None,
        region: str = "us-east-1",
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
    ) -> None:
        self._bucket = bucket
        self._endpoint_url = endpoint_url
        self._region = region
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key
        self._client = None

    async def connect(self) -> None:
        import aioboto3

        session = aioboto3.Session(
            aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._secret_access_key,
            region_name=self._region,
        )
        self._client = await session.client("s3", endpoint_url=self._endpoint_url).__aenter__()

    async def close(self) -> None:
        if self._client is not None:
            await self._client.__aexit__(None, None, None)
            self._client = None

    async def health(self) -> bool:
        if self._client is None:
            return False
        try:
            await self._client.head_bucket(Bucket=self._bucket)
            return True
        except Exception:
            return False

    @property
    def client(self) -> Any:
        if self._client is None:
            raise RuntimeError("S3ObjectStoreConnector not connected — call connect() first")
        return self._client

    @property
    def bucket(self) -> str:
        return self._bucket
