from __future__ import annotations

from typing import Any

from cachetools import TTLCache
from kubernetes import client as k8s_client
from structlog import get_logger

from resource_broker.common.config import settings
from resource_broker.common.models.profile import ResourceProfile

logger = get_logger(__name__)

CRD_GROUP = "resource-broker.io"
CRD_VERSION = "v1alpha1"
CRD_PLURAL = "resourceprofiles"


class ProfileLoader:
    def __init__(self, api: k8s_client.CustomObjectsApi | None = None) -> None:
        self._api = api or k8s_client.CustomObjectsApi()
        self._cache: TTLCache[str, ResourceProfile] = TTLCache(maxsize=100, ttl=60)

    def _cache_key(self, name: str, namespace: str) -> str:
        return f"{namespace}/{name}"

    async def get_by_name(self, name: str, namespace: str = "default") -> ResourceProfile | None:
        cache_key = self._cache_key(name, namespace)
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        try:
            crd = self._api.get_namespaced_custom_object(
                group=CRD_GROUP,
                version=CRD_VERSION,
                namespace=namespace,
                plural=CRD_PLURAL,
                name=name,
            )
            profile = ResourceProfile.from_crd(crd)
            self._cache[cache_key] = profile
            return profile
        except k8s_client.exceptions.ApiException as exc:
            if exc.status == 404:
                logger.warning("profile not found", name=name, namespace=namespace)
                return None
            logger.error("failed to fetch profile", name=name, namespace=namespace, error=str(exc))
            return None

    async def find_for_pod(self, pod: dict[str, Any]) -> ResourceProfile | None:
        metadata = pod.get("metadata", {}) or {}
        annotations = metadata.get("annotations", {}) or {}
        profile_name = annotations.get(settings.profile_annotation_key)

        if not profile_name:
            return None

        namespace = metadata.get("namespace", "default")
        return await self.get_by_name(profile_name, namespace)
