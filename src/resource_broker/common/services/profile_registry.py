from __future__ import annotations

from typing import Any

from kubernetes import client as k8s_client
from structlog import get_logger

from resource_broker.common.dao.database import get_session
from resource_broker.common.dao.repositories.profiles import ProfileSnapshotRepository
from resource_broker.common.models.profile import ResourceProfile

logger = get_logger(__name__)

CRD_GROUP = "resource-broker.io"
CRD_VERSION = "v1alpha1"
CRD_PLURAL = "profiles"


class ProfileRegistry:
    """Per-replica in-memory store of ResourceProfile CRDs.

    NOT a distributed cache — each replica builds its own copy independently.
    The DB is the shared persistent store; this dict is local to the process.

    Lifecycle:
      bootstrap() → k8s API first, profile_snapshots DB fallback if unavailable.
      upsert()/remove() → update in-memory dict only.
      DB writes (profile_snapshots) are the caller's responsibility,
      handled by crd_watcher._persist_event.
    """

    def __init__(self) -> None:
        self._profiles: dict[str, ResourceProfile] = {}

    def _key(self, name: str, namespace: str = "") -> str:
        # CRDs are cluster-scoped — namespace is not part of object identity.
        return name

    async def bootstrap(self, api: k8s_client.CustomObjectsApi) -> None:
        """Populate registry from Kubernetes API; fall back to DB on k8s failure."""
        try:
            result = api.list_cluster_custom_object(
                group=CRD_GROUP,
                version=CRD_VERSION,
                plural=CRD_PLURAL,
            )
            items = result.get("items") or []
            profiles: list[ResourceProfile] = []
            raw_crds: list[dict[str, Any]] = []
            for item in items:
                try:
                    profile = ResourceProfile.from_crd(item)
                    self._profiles[self._key(profile.name, profile.namespace)] = profile
                    profiles.append(profile)
                    raw_crds.append(item)
                except Exception:
                    logger.exception(
                        "failed to parse profile during bootstrap",
                        item=item.get("metadata", {}).get("name"),
                    )
            logger.info("profile registry bootstrapped from kubernetes", count=len(self._profiles))
            await self._seed_db(profiles, raw_crds)
        except Exception:
            logger.exception("failed to bootstrap from kubernetes, falling back to db")
            await self._load_from_db()

    async def _seed_db(
        self,
        profiles: list[ResourceProfile],
        raw_crds: list[dict[str, Any]],
    ) -> None:
        """Write-through: persist current profile state to the snapshot table.

        Hash-gated — safe to call from N replicas simultaneously; no-op if unchanged.
        """
        if not profiles:
            return
        try:
            async with get_session() as session:
                snap_repo = ProfileSnapshotRepository(session)
                written = 0
                for raw_crd in raw_crds:
                    if await snap_repo.upsert(raw_crd):
                        written += 1
            logger.info("profile db seed complete", total=len(profiles), written=written)
        except Exception:
            logger.exception("failed to seed profile db on bootstrap")

    async def _load_from_db(self) -> None:
        """Bootstrap fallback: populate registry from profile_snapshots table."""
        try:
            async with get_session() as session:
                repo = ProfileSnapshotRepository(session)
                profiles = await repo.get_all()
            for profile in profiles:
                self._profiles[self._key(profile.name, profile.namespace)] = profile
            logger.info("profile registry loaded from db fallback", count=len(profiles))
        except Exception:
            logger.exception("failed to load profiles from db; registry starts empty")

    def get(self, name: str, namespace: str = "") -> ResourceProfile | None:
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
