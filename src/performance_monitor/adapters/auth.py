from abc import ABC, abstractmethod


class AuthProvider(ABC):

    @abstractmethod
    async def apply_headers(self, headers: dict[str, str]) -> None: ...


class NoAuth(AuthProvider):
    async def apply_headers(self, headers: dict[str, str]) -> None: ...


class BearerTokenAuth(AuthProvider):
    def __init__(self, token: str) -> None:
        self._token = token

    async def apply_headers(self, headers: dict[str, str]) -> None:
        headers["Authorization"] = f"Bearer {self._token}"


class BasicAuth(AuthProvider):
    def __init__(self, username: str, password: str) -> None:
        self._username = username
        self._password = password

    async def apply_headers(self, headers: dict[str, str]) -> None:
        import base64

        raw = f"{self._username}:{self._password}"
        encoded = base64.b64encode(raw.encode()).decode()
        headers["Authorization"] = f"Basic {encoded}"
