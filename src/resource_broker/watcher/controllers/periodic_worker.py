from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from kubernetes import client as k8s_client
from structlog import get_logger

from resource_broker.common.k8s_client import create_k8s_api
from resource_broker.common.models.profile import ResourceProfile
from resource_broker.common.models.strategy_crd import StrategyCRD
from resource_broker.common.services.profile_registry import ProfileRegistry
from resource_broker.common.services.recommendation_service import RecommendationService
from resource_broker.common.services.strategy_registry import StrategyRegistry
from resource_broker.config import settings
from resource_broker.watcher.services.patcher import compute_patches

logger = get_logger(__name__)


def _get_run_every_minutes(
    profile: ResourceProfile,
    strategy_registry: StrategyRegistry,
) -> float | None:
    """Return the shortest run_every_minutes across all strategies referenced by the profile.

    A profile can reference multiple strategies (one default + per-field overrides).
    Taking the minimum ensures no field falls behind its desired cadence.
    Returns None when no referenced strategy has a schedule, disabling periodic checks.
    """
    candidates: list[float] = []

    def _add(algo: str) -> None:
        strat: StrategyCRD | None = strategy_registry.get(algo)
        if strat and strat.run_every_minutes is not None:
            candidates.append(strat.run_every_minutes)

    if profile.strategy:
        _add(profile.strategy.algo)
    for entry in profile.fields.values():
        if entry.strategy:
            _add(entry.strategy.algo)

    return min(candidates) if candidates else None


class PeriodicCheckWorker:
    """Schedule-driven background worker for drift detection.

    Iterates all profiles on every tick (settings.periodic_worker_interval_seconds).
    For each profile whose effective strategy defines a run-every schedule, checks
    whether enough time has elapsed since the last run.  When due, lists all pods
    carrying the profile annotation and re-runs compute_patches.

    In enforce mode the worker re-applies patches via the admission webhook path
    (delete + recreate) — see PodWatcher for details.  In recommendation mode it
    only logs the computed patches.
    """

    def __init__(
        self,
        profile_registry: ProfileRegistry,
        strategy_registry: StrategyRegistry,
        recommendation_svc: RecommendationService,
    ) -> None:
        self._profiles = profile_registry
        self._strategies = strategy_registry
        self._svc = recommendation_svc
        self._core_api: k8s_client.CoreV1Api = create_k8s_api(k8s_client.CoreV1Api)
        # Keyed by profile.name; stores the last time a check ran for that profile.
        self._last_run: dict[str, datetime] = {}

    async def run_forever(self) -> None:
        interval = settings.periodic_worker_interval_seconds
        logger.info("periodic check worker started", tick_seconds=interval)
        while True:
            try:
                await self._tick()
            except Exception:
                logger.exception("periodic check worker tick failed")
            await asyncio.sleep(interval)

    async def _tick(self) -> None:
        now = datetime.now(UTC)
        for profile in self._profiles.all_profiles():
            run_every_minutes = _get_run_every_minutes(profile, self._strategies)
            if run_every_minutes is None:
                continue  # No schedule configured for this profile's strategies

            last = self._last_run.get(profile.name, datetime.min.replace(tzinfo=UTC))
            elapsed_minutes = (now - last).total_seconds() / 60
            if elapsed_minutes < run_every_minutes:
                continue

            self._last_run[profile.name] = now
            logger.info(
                "periodic check due",
                profile=profile.name,
                run_every_minutes=run_every_minutes,
                elapsed_minutes=round(elapsed_minutes, 1),
            )
            try:
                await self._check_profile(profile)
            except Exception:
                logger.exception("periodic check failed", profile=profile.name)

    async def _check_profile(self, profile: ResourceProfile) -> None:
        """List all pods for this profile and re-evaluate resource recommendations."""
        loop = asyncio.get_running_loop()
        annotation_key = settings.profile_annotation_key

        try:
            pod_list = await loop.run_in_executor(
                None,
                lambda: self._core_api.list_pod_for_all_namespaces(
                    label_selector="",
                    # Field selectors cannot filter on annotations; we filter in Python below.
                ),
            )
        except k8s_client.exceptions.ApiException as exc:
            logger.error("failed to list pods for periodic check", profile=profile.name, error=str(exc))
            return

        pods = pod_list.items or []
        matched = [
            p for p in pods
            if (
                (p.metadata.labels or {}).get(annotation_key)
                or (p.metadata.annotations or {}).get(annotation_key)
            ) == profile.name
        ]

        if not matched:
            logger.debug("no pods matched profile in periodic check", profile=profile.name)
            return

        logger.info("periodic check running", profile=profile.name, pod_count=len(matched))

        for pod in matched:
            try:
                await self._check_pod(profile, pod)
            except Exception:
                logger.exception(
                    "periodic pod check failed",
                    profile=profile.name,
                    pod=pod.metadata.name,
                )

    async def _check_pod(self, profile: ResourceProfile, pod: Any) -> None:
        pod_dict = _pod_to_dict(pod)
        ctx: dict[str, Any] = {
            "profile_name": profile.name,
            "pod_spec": pod_dict,
        }
        patches = await compute_patches(profile, pod_dict, context=ctx)
        if not patches:
            return

        pod_name = pod.metadata.name
        pod_namespace = pod.metadata.namespace

        if profile.is_enforce_mode():
            logger.info(
                "periodic enforcement patch",
                pod=pod_name,
                profile=profile.name,
                patches=len(patches),
            )
            # Defer actual enforcement to PodWatcher's recreate path to avoid
            # duplicating the delete/poll/recreate logic here.  Invalidating the
            # cache ensures the next admission webhook call recomputes fresh values.
            self._svc.invalidate(profile.name, profile.namespace)
        else:
            logger.info(
                "periodic recommendation computed",
                pod=pod_name,
                namespace=pod_namespace,
                profile=profile.name,
                patches=patches,
            )


def _pod_to_dict(pod: Any) -> dict[str, Any]:
    """Convert a kubernetes client V1Pod object to a plain dict for patch computation."""
    try:
        from kubernetes.client import ApiClient
        return ApiClient().sanitize_for_serialization(pod)
    except Exception:
        spec = pod.spec
        meta = pod.metadata
        return {
            "metadata": {
                "name": meta.name if meta else "",
                "namespace": meta.namespace if meta else "",
                "annotations": dict(meta.annotations or {}) if meta else {},
                "labels": dict(meta.labels or {}) if meta else {},
            },
            "spec": spec.to_dict() if spec and hasattr(spec, "to_dict") else {},
        }
