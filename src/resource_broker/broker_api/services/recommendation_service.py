# NOTE: pre-spine-redesign on-demand compute path (depends on recommender.services.patcher,
# a cross-service import). Per design spec section 11 this class is slated for removal —
# the webhook should only look up a precomputed service_recommendations row (#23/#25),
# never compute on the hot path.
from __future__ import annotations

import hashlib
import json
from typing import Any

from cachetools import TTLCache
from structlog import get_logger

from resource_broker.broker_api.services.profile_registry import ProfileRegistry
from resource_broker.common.models.profile import ResourceProfile
from resource_broker.recommender.services.patcher import compute_patches

logger = get_logger(__name__)

_DEFAULT_TTL_SECONDS = 300  # 5 minutes
_DEFAULT_MAX_SIZE = 512


def _pod_resource_hash(pod_spec: dict[str, Any]) -> str:
    """Hash only the resource-relevant fields of the pod spec to form a cache key."""
    containers = (pod_spec.get("spec") or {}).get("containers") or []
    relevant = [
        {
            "name": c.get("name"),
            "resources": c.get("resources"),
            "image": c.get("image"),
        }
        for c in containers
    ]
    raw = json.dumps(relevant, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


class RecommendationService:
    """Singleton service: caches computed recommendations, invalidated on CRD change."""

    def __init__(
        self,
        registry: ProfileRegistry,
        ttl: int = _DEFAULT_TTL_SECONDS,
        maxsize: int = _DEFAULT_MAX_SIZE,
    ) -> None:
        self.registry = registry
        self._cache: TTLCache[str, list[dict[str, Any]]] = TTLCache(maxsize=maxsize, ttl=ttl)

    def _cache_key(self, profile: ResourceProfile, pod_spec: dict[str, Any]) -> str:
        pod_hash = _pod_resource_hash(pod_spec)
        return f"{profile.namespace}/{profile.name}:{pod_hash}"

    async def get_patches(
        self,
        profile: ResourceProfile,
        pod_spec: dict[str, Any],
    ) -> list[dict[str, Any]]:
        key = self._cache_key(profile, pod_spec)
        cached = self._cache.get(key)
        if cached is not None:
            logger.debug("recommendation cache hit", profile=profile.name, key=key)
            return cached

        patches = await compute_patches(profile, pod_spec)
        self._cache[key] = patches
        logger.debug(
            "recommendation computed and cached",
            profile=profile.name,
            patch_count=len(patches),
        )
        return patches

    def invalidate(self, name: str, namespace: str) -> None:
        prefix = f"{namespace}/{name}:"
        keys_to_drop = [k for k in list(self._cache.keys()) if k.startswith(prefix)]
        for k in keys_to_drop:
            self._cache.pop(k, None)
        if keys_to_drop:
            logger.info("recommendation cache invalidated", name=name, namespace=namespace, dropped=len(keys_to_drop))

    def invalidate_resource_type(self, resource_type: str) -> None:
        profiles = [p for p in self.registry.all_profiles() if p.resource_type == resource_type]
        for p in profiles:
            self.invalidate(p.name, p.namespace)
        logger.info("recommendation cache invalidated for resource_type", resource_type=resource_type, profiles=len(profiles))

    def invalidate_all(self) -> None:
        self._cache.clear()
        logger.info("recommendation cache fully cleared")
