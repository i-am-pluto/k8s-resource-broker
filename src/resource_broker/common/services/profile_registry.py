from __future__ import annotations

from typing import Any

from kubernetes import client as k8s_client
from structlog import get_logger

from resource_broker.common.models.profile import ResourceProfile

logger = get_logger(__name__)

CRD_GROUP = "resource-broker.io"
CRD_VERSION = "v1alpha1"
CRD_PLURAL = "resourceprofiles"


class ProfileRegistry:
    """In-memory store of all ResourceProfile CRDs, kept current via watch."""

    def __init__(self) -> None:
        self._profiles: dict[str, ResourceProfile] = {}

    def _key(self, name: str, namespace: str) -> str:
        return f"{namespace}/{name}"

    async def bootstrap(self, api: k8s_client.CustomObjectsApi) -> None:
        """List all ResourceProfile CRDs and populate the registry."""
        try:
            result = api.list_cluster_custom_object(
                group=CRD_GROUP,
                version=CRD_VERSION,
                plural=CRD_PLURAL,
            )
            items = result.get("items") or []
            for item in items:
                try:
                    profile = ResourceProfile.from_crd(item)
                    self._profiles[self._key(profile.name, profile.namespace)] = profile
                except Exception:
                    logger.exception("failed to parse profile during bootstrap", item=item.get("metadata", {}).get("name"))
            logger.info("profile registry bootstrapped", count=len(self._profiles))
        except Exception:
            logger.exception("failed to bootstrap profile registry")

    def get(self, name: str, namespace: str = "default") -> ResourceProfile | None:
        return self._profiles.get(self._key(name, namespace))

    def upsert(self, crd_obj: dict[str, Any]) -> ResourceProfile | None:
        try:
            profile = ResourceProfile.from_crd(crd_obj)
            self._profiles[self._key(profile.name, profile.namespace)] = profile
            logger.debug("profile registry updated", name=profile.name, namespace=profile.namespace)
            return profile
        except Exception:
            logger.exception("failed to parse profile for registry update")
            return None

    def remove(self, name: str, namespace: str) -> None:
        self._profiles.pop(self._key(name, namespace), None)
        logger.debug("profile removed from registry", name=name, namespace=namespace)

    def all_profiles(self) -> list[ResourceProfile]:
        return list(self._profiles.values())
