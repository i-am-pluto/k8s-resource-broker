from __future__ import annotations

from typing import Any

from kubernetes import client as k8s_client
from structlog import get_logger

from resource_broker.common.dao.database import get_session
from resource_broker.common.dao.repositories.profiles import ProfileRepository
from resource_broker.common.models.profile import ResourceProfile

logger = get_logger(__name__)

CRD_GROUP = "resource-broker.io"
CRD_VERSION = "v1alpha1"
CRD_PLURAL = "resourceprofiles"


class ProfileRegistry:
    """In-memory store of all ResourceProfile CRDs, kept current via watch.

    On startup bootstrap() tries the Kubernetes API first; if that fails it falls
    back to the DB so the registry is never empty after a restart.  Every CRD
    event that reaches upsert()/remove() is also written through to the DB via
    the watcher, so the DB always reflects the last-known cluster state.
    """

    def __init__(self) -> None:
        self._profiles: dict[str, ResourceProfile] = {}

    def _key(self, name: str, namespace: str) -> str:
        return f"{namespace}/{name}"

    async def bootstrap(self, api: k8s_client.CustomObjectsApi) -> None:
        """Populate registry from Kubernetes API; fall back to DB on failure."""
        try:
            result = api.list_cluster_custom_object(
                group=CRD_GROUP,
                version=CRD_VERSION,
                plural=CRD_PLURAL,
            )
            items = result.get("items") or []
            profiles: list[ResourceProfile] = []
            for item in items:
                try:
                    profile = ResourceProfile.from_crd(item)
                    self._profiles[self._key(profile.name, profile.namespace)] = profile
                    profiles.append(profile)
                except Exception:
                    logger.exception(
                        "failed to parse profile during bootstrap",
                        item=item.get("metadata", {}).get("name"),
                    )
            logger.info("profile registry bootstrapped from kubernetes", count=len(self._profiles))
            # Seed DB so subsequent restarts have a warm fallback even if k8s is unavailable.
            await self._seed_db(profiles)
        except Exception:
            logger.exception("failed to bootstrap from kubernetes, falling back to db")
            await self._load_from_db()

    async def _seed_db(self, profiles: list[ResourceProfile]) -> None:
        """Write-through: persist every profile loaded from Kubernetes into DB."""
        if not profiles:
            return
        try:
            async with get_session() as session:
                repo = ProfileRepository(session)
                for profile in profiles:
                    await repo.upsert(profile)
            logger.info("profile registry seeded db", count=len(profiles))
        except Exception:
            logger.exception("failed to seed profile db on bootstrap")

    async def _load_from_db(self) -> None:
        """Bootstrap fallback: populate registry from last-known DB state."""
        try:
            async with get_session() as session:
                repo = ProfileRepository(session)
                profiles = await repo.get_all_active()
            for profile in profiles:
                self._profiles[self._key(profile.name, profile.namespace)] = profile
            logger.info("profile registry loaded from db fallback", count=len(profiles))
        except Exception:
            logger.exception("failed to load profiles from db; registry starts empty")

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
