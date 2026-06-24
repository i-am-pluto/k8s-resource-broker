from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Iterator

from kubernetes import client as k8s_client
from kubernetes import watch

from resource_broker.common.k8s_client import create_k8s_api
from resource_broker.common.resource_types.configured_resource import configured_resource_registry


@dataclass
class PodStatusInfo:
    pod_name: str
    namespace: str
    pod_status: str  # e.g. "Running", "Pending", "Failed", "Succeeded"
    restart_count: int
    last_terminated_reason: str | None  # e.g. "OOMKilled", None


@dataclass
class PodConfiguredResourceInfo:
    pod_name: str
    namespace: str
    resource_type: str  # "k8s-pod" for now
    configured_resource: dict[str, Any]  # shaped via ConfiguredResource.to_dict()
    labels: dict[str, str] = field(default_factory=dict)


class K8sAdapter(ABC):
    """Answers status + configured-resources fetch, plus raw pod-watch events.
    Concrete k8s client is never used directly by anything outside this module."""

    @abstractmethod
    async def get_pod_status(self, namespace: str, pod_name: str) -> PodStatusInfo: ...

    @abstractmethod
    async def get_configured_resources(
        self, namespace: str, pod_name: str, resource_type: str = "k8s-pod"
    ) -> PodConfiguredResourceInfo: ...

    @abstractmethod
    def watch_pods(self, namespace: str = "") -> Iterator[dict[str, Any]]:
        """Sync generator yielding raw k8s watch event dicts
        ({"type": "ADDED"|"MODIFIED"|"DELETED", "raw_object": {...}}).
        Callers consume this in an executor thread (it blocks on the watch stream)."""
        ...


class KubernetesApiAdapter(K8sAdapter):
    """Concrete Kubernetes API adapter for pod status, configured resources, and watching."""

    def __init__(self) -> None:
        self._core_api = create_k8s_api(k8s_client.CoreV1Api)

    async def get_pod_status(self, namespace: str, pod_name: str) -> PodStatusInfo:
        """Fetch pod status info by reading the pod from the Kubernetes API."""
        loop = asyncio.get_running_loop()
        pod = await loop.run_in_executor(
            None,
            self._core_api.read_namespaced_pod,
            pod_name,
            namespace,
        )

        # Extract pod status phase
        pod_status = pod.status.phase or "Unknown"

        # Sum restart_count across all containers
        restart_count = 0
        last_terminated_reason: str | None = None
        if pod.status.container_statuses:
            # First pass: sum restart counts from all containers
            for container_status in pod.status.container_statuses:
                if container_status.restart_count:
                    restart_count += container_status.restart_count
            # Second pass: find the first container with a terminated reason
            for container_status in pod.status.container_statuses:
                if (
                    container_status.last_state
                    and container_status.last_state.terminated
                    and container_status.last_state.terminated.reason
                ):
                    last_terminated_reason = container_status.last_state.terminated.reason
                    break

        return PodStatusInfo(
            pod_name=pod_name,
            namespace=namespace,
            pod_status=pod_status,
            restart_count=restart_count,
            last_terminated_reason=last_terminated_reason,
        )

    async def get_configured_resources(
        self, namespace: str, pod_name: str, resource_type: str = "k8s-pod"
    ) -> PodConfiguredResourceInfo:
        """Fetch pod and extract configured resource requests/limits."""
        loop = asyncio.get_running_loop()
        pod = await loop.run_in_executor(
            None,
            self._core_api.read_namespaced_pod,
            pod_name,
            namespace,
        )

        # Get the first container's resource requests and limits
        cpu_request = None
        memory_request = None
        cpu_limit = None
        memory_limit = None
        ephemeral_storage = None

        if pod.spec.containers:
            first_container = pod.spec.containers[0]
            if first_container.resources:
                # Handle requests (can be None)
                if first_container.resources.requests:
                    cpu_request = first_container.resources.requests.get("cpu")
                    memory_request = first_container.resources.requests.get("memory")

                # Handle limits (can be None)
                if first_container.resources.limits:
                    cpu_limit = first_container.resources.limits.get("cpu")
                    memory_limit = first_container.resources.limits.get("memory")
                    ephemeral_storage = first_container.resources.limits.get("ephemeral-storage")

        # Build the configured resource using the registry
        resource_cls = configured_resource_registry.get(resource_type)
        configured_resource = resource_cls(
            cpu_request=cpu_request,
            memory_request=memory_request,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            ephemeral_storage=ephemeral_storage,
        ).to_dict()

        # Extract labels
        labels = pod.metadata.labels or {}

        return PodConfiguredResourceInfo(
            pod_name=pod_name,
            namespace=namespace,
            resource_type=resource_type,
            configured_resource=configured_resource,
            labels=labels,
        )

    def watch_pods(self, namespace: str = "") -> Iterator[dict[str, Any]]:
        """Watch pod events using Kubernetes watch API.

        Mirrors the pattern from scrape_runner.py's _watch_pods method.
        Returns raw k8s watch event dicts.
        """
        w = watch.Watch()
        if namespace:
            stream = w.stream(
                self._core_api.list_namespaced_pod,
                namespace=namespace,
                timeout_seconds=0,
            )
        else:
            stream = w.stream(
                self._core_api.list_pod_for_all_namespaces,
                timeout_seconds=0,
            )
        for event in stream:
            yield event
