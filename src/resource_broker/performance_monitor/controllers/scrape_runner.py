# NOTE: PodWatcher is the pre-spine-redesign combined watcher (metrics collection +
# per-pod profile lookup/enforcement in one class). Per the spine design spec, metrics
# collection is performance-monitor's job (#24) and per-pod enforcement is deferred to
# P3 — so its cross-service imports (broker_api.services.profile_loader,
# recommender.services.patcher) are a known transitional wart, not a new design choice.
# TODO(#24): split MetricsCollector out into its own incremental scrape entrypoint and
# retire/relocate the per-pod enforce path per the design spec.
from __future__ import annotations

import asyncio
import copy
from typing import Any

from kubernetes import client as k8s_client
from kubernetes import watch
from structlog import get_logger

from resource_broker.broker_api.services.profile_loader import ProfileLoader
from resource_broker.common.config import settings
from resource_broker.common.k8s_client import create_k8s_api
from resource_broker.performance_monitor.services.collector import MetricsCollector
from resource_broker.performance_monitor.services.metrics_adapter import MetricsAdapter
from resource_broker.recommender.services.patcher import compute_patches

logger = get_logger(__name__)

_PROCESSED_ANNOTATION = "resource-broker.io/processed"
# Kubernetes auto-injects a projected service account volume with this prefix;
# strip it when building a clean pod spec for recreation so the API server can
# re-inject it fresh (submitting it causes a FieldImmutable 422).
_SA_VOLUME_PREFIX = "kube-api-access-"


def _apply_json_patches(obj: dict[str, Any], patches: list[dict[str, Any]]) -> None:
    """Apply RFC 6902 replace operations to a nested dict in-place."""
    for op in patches:
        if op.get("op") != "replace":
            continue
        parts = [p for p in op["path"].split("/") if p]
        target: Any = obj
        for part in parts[:-1]:
            target = target[int(part)] if isinstance(target, list) else target[part]
        final = parts[-1]
        if isinstance(target, list):
            target[int(final)] = op["value"]
        else:
            target[final] = op["value"]


class PodWatcher:
    def __init__(self, adapter: MetricsAdapter) -> None:
        self._core_api = create_k8s_api(k8s_client.CoreV1Api)
        self._profile_loader = ProfileLoader()
        self._collector = MetricsCollector(adapter)

    async def run(self) -> None:
        logger.info("pod watcher started", watch_namespace=settings.watch_namespace or "<all>")
        collector_task = asyncio.create_task(self._collector.run_forever())
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, self._watch_pods, loop)
        finally:
            collector_task.cancel()
            await self._collector._adapter.close()

    def _watch_pods(self, loop: asyncio.AbstractEventLoop) -> None:
        w = watch.Watch()
        if settings.watch_namespace:
            stream = w.stream(
                self._core_api.list_namespaced_pod,
                namespace=settings.watch_namespace,
                timeout_seconds=0,
            )
        else:
            stream = w.stream(
                self._core_api.list_pod_for_all_namespaces,
                timeout_seconds=0,
            )
        for event in stream:
            asyncio.run_coroutine_threadsafe(
                self._handle_event(event),
                loop,
            )

    async def _handle_event(self, event: dict[str, Any]) -> None:
        try:
            obj = event.get("raw_object", {})
            evt_type = event.get("type", "")

            if evt_type != "ADDED":
                return

            metadata = obj.get("metadata", {}) or {}
            annotations = metadata.get("annotations", {}) or {}

            # Pods recreated by us carry this annotation so we don't loop forever.
            if _PROCESSED_ANNOTATION in annotations:
                return

            profile = await self._profile_loader.find_for_pod(obj)
            if profile is None:
                return

            pod_name = metadata.get("name", "unknown")
            pod_namespace = metadata.get("namespace", "default")
            logger.info("found profile for pod", pod=pod_name, profile=profile.name, mode=profile.mode)

            patches = await compute_patches(profile, obj)
            if not patches:
                return

            if profile.is_enforce_mode():
                await self._enforce_patches(pod_name, pod_namespace, obj, patches)
            else:
                logger.info(
                    "recommendation computed (mode: recommendation)",
                    pod=pod_name,
                    profile=profile.name,
                    patches=patches,
                )
        except Exception:
            logger.exception("unhandled error in pod event handler", event_type=event.get("type"))

    async def _enforce_patches(
        self,
        pod_name: str,
        pod_namespace: str,
        pod: dict[str, Any],
        patches: list[dict[str, Any]],
    ) -> None:
        # ADDED events fire while the pod is still Pending. We must not delete
        # or patch a pod that hasn't started yet — wait for Ready first.
        # We poll for the Ready condition (not just phase=Running) because:
        #   - the smoke test runs `kubectl wait --for=condition=Ready`
        #   - Ready is set slightly after phase=Running
        #   - deleting before Ready causes kubectl wait to fail with "pod deleted"
        loop = asyncio.get_running_loop()
        for _ in range(15):  # up to 30s (15 × 2s)
            await asyncio.sleep(2)
            try:
                pod_info = await loop.run_in_executor(
                    None, self._core_api.read_namespaced_pod, pod_name, pod_namespace
                )
            except k8s_client.exceptions.ApiException:
                logger.info("pod gone before enforcement", pod=pod_name)
                return
            if pod_info.status.phase == "Running":
                conditions = pod_info.status.conditions or []
                if any(c.type == "Ready" and c.status == "True" for c in conditions):
                    break
        else:
            logger.warning("pod did not reach Ready in 30s, skipping enforcement", pod=pod_name)
            return

        # Primary strategy: in-place resource patch (requires InPlacePodVerticalScaling
        # feature gate on both API server and kubelet).
        try:
            self._core_api.patch_namespaced_pod(
                name=pod_name,
                namespace=pod_namespace,
                body=patches,
            )
            logger.info("pod patched in-place (enforce mode)", pod=pod_name, patches=len(patches))
            return
        except k8s_client.exceptions.ApiException as exc:
            if exc.status != 422:
                logger.error("failed to patch pod", pod=pod_name, error=str(exc))
                return

        # Fallback: delete-and-recreate with the corrected spec.
        # 422 = API server rejected the resource change because InPlacePodVerticalScaling
        # is not enabled. We recreate the pod with the desired resources instead,
        # mirroring what Kubernetes VPA does in its "Recreate" update mode.
        logger.info(
            "in-place resize unavailable (422); recreating pod with enforced resources",
            pod=pod_name,
        )
        await self._recreate_with_patches(pod_name, pod_namespace, pod, patches)

    async def _recreate_with_patches(
        self,
        pod_name: str,
        pod_namespace: str,
        pod: dict[str, Any],
        patches: list[dict[str, Any]],
    ) -> None:
        metadata = pod.get("metadata", {}) or {}

        # Build a clean pod body: copy the spec but strip server-managed fields
        # that would be rejected or re-injected anyway on creation.
        spec = copy.deepcopy(pod.get("spec", {}))
        spec.pop("nodeName", None)
        spec.pop("hostname", None)
        # Remove the projected service-account token volume — Kubernetes re-injects it.
        spec["volumes"] = [
            v for v in spec.get("volumes", [])
            if not v.get("name", "").startswith(_SA_VOLUME_PREFIX)
        ]
        for container in spec.get("containers", []):
            container["volumeMounts"] = [
                m for m in container.get("volumeMounts", [])
                if not m.get("name", "").startswith(_SA_VOLUME_PREFIX)
            ]

        new_pod: dict[str, Any] = {
            "apiVersion": pod.get("apiVersion", "v1"),
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": pod_namespace,
                "labels": metadata.get("labels") or {},
                "annotations": {
                    **(metadata.get("annotations") or {}),
                    _PROCESSED_ANNOTATION: "true",
                },
            },
            "spec": spec,
        }
        _apply_json_patches(new_pod, patches)

        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None, self._core_api.delete_namespaced_pod, pod_name, pod_namespace
            )
            logger.info("pod deleted for recreation", pod=pod_name)
        except k8s_client.exceptions.ApiException as exc:
            logger.error("failed to delete pod for recreation", pod=pod_name, error=str(exc))
            return

        # delete_namespaced_pod only sets deletionTimestamp — the pod object stays in
        # etcd while kubelet terminates containers and drains finalizers. Creating a new
        # pod with the same name while the old one is still "Terminating" returns a 409
        # "object is being deleted: already exists". Poll until we get a 404.
        for _ in range(30):  # up to 60s
            await asyncio.sleep(2)
            try:
                await loop.run_in_executor(
                    None, self._core_api.read_namespaced_pod, pod_name, pod_namespace
                )
                # Pod still exists (Terminating) — keep waiting
            except k8s_client.exceptions.ApiException as exc:
                if exc.status == 404:
                    break
                # Unexpected API error — log but stop waiting
                logger.warning("unexpected error while waiting for pod deletion", pod=pod_name, error=str(exc))
                break
        else:
            logger.error("pod not fully deleted after 60s, aborting recreation", pod=pod_name)
            return

        try:
            await loop.run_in_executor(
                None, self._core_api.create_namespaced_pod, pod_namespace, new_pod
            )
            logger.info(
                "pod recreated with enforced resources",
                pod=pod_name,
                patches=len(patches),
            )
        except k8s_client.exceptions.ApiException as exc:
            logger.error("failed to recreate pod", pod=pod_name, error=str(exc))
