from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from resource_broker.performance_monitor.services.k8s_adapter import (
    KubernetesApiAdapter,
    PodConfiguredResourceInfo,
    PodStatusInfo,
)


class MockContainerStatus:
    """Mock for kubernetes.client.V1ContainerStatus."""

    def __init__(
        self,
        name: str = "app",
        restart_count: int = 0,
        last_state: Any = None,
    ) -> None:
        self.name = name
        self.restart_count = restart_count
        self.last_state = last_state


class MockTerminatedState:
    """Mock for kubernetes.client.V1ContainerStateTerminated."""

    def __init__(self, reason: str | None = None) -> None:
        self.reason = reason


class MockContainerState:
    """Mock for kubernetes.client.V1ContainerState."""

    def __init__(self, terminated: MockTerminatedState | None = None) -> None:
        self.terminated = terminated


class MockPodStatus:
    """Mock for kubernetes.client.V1PodStatus."""

    def __init__(
        self,
        phase: str = "Running",
        container_statuses: list[MockContainerStatus] | None = None,
    ) -> None:
        self.phase = phase
        self.container_statuses = container_statuses or []


class MockResourceRequirements:
    """Mock for kubernetes.client.V1ResourceRequirements."""

    def __init__(
        self,
        requests: dict[str, str] | None = None,
        limits: dict[str, str] | None = None,
    ) -> None:
        self.requests = requests
        self.limits = limits


class MockContainer:
    """Mock for kubernetes.client.V1Container."""

    def __init__(
        self,
        name: str = "app",
        resources: MockResourceRequirements | None = None,
    ) -> None:
        self.name = name
        self.resources = resources


class MockPodSpec:
    """Mock for kubernetes.client.V1PodSpec."""

    def __init__(self, containers: list[MockContainer] | None = None) -> None:
        self.containers = containers or []


class MockObjectMeta:
    """Mock for kubernetes.client.V1ObjectMeta."""

    def __init__(
        self,
        name: str = "test-pod",
        namespace: str = "default",
        labels: dict[str, str] | None = None,
    ) -> None:
        self.name = name
        self.namespace = namespace
        self.labels = labels


class MockPod:
    """Mock for kubernetes.client.V1Pod."""

    def __init__(
        self,
        name: str = "test-pod",
        namespace: str = "default",
        phase: str = "Running",
        container_statuses: list[MockContainerStatus] | None = None,
        containers: list[MockContainer] | None = None,
        labels: dict[str, str] | None = None,
    ) -> None:
        self.metadata = MockObjectMeta(name=name, namespace=namespace, labels=labels)
        self.status = MockPodStatus(phase=phase, container_statuses=container_statuses)
        self.spec = MockPodSpec(containers=containers)


@pytest.fixture
def mock_core_api() -> MagicMock:
    """Fixture providing a mocked CoreV1Api."""
    return MagicMock()


@pytest.mark.asyncio
async def test_get_pod_status_running_pod(mock_core_api: MagicMock) -> None:
    """Test getting pod status for a running pod."""
    mock_pod = MockPod(
        name="test-pod",
        namespace="default",
        phase="Running",
        container_statuses=[
            MockContainerStatus(name="app", restart_count=2),
        ],
    )
    mock_core_api.read_namespaced_pod.return_value = mock_pod

    with patch("resource_broker.performance_monitor.services.k8s_adapter.create_k8s_api") as mock_create:
        mock_create.return_value = mock_core_api

        adapter = KubernetesApiAdapter()
        status = await adapter.get_pod_status("default", "test-pod")

        assert status.pod_name == "test-pod"
        assert status.namespace == "default"
        assert status.pod_status == "Running"
        assert status.restart_count == 2
        assert status.last_terminated_reason is None


@pytest.mark.asyncio
async def test_get_pod_status_with_terminated_reason(mock_core_api: MagicMock) -> None:
    """Test pod status with a terminated container reason."""
    terminated = MockContainerState(terminated=MockTerminatedState(reason="OOMKilled"))
    mock_pod = MockPod(
        name="test-pod",
        namespace="default",
        phase="Failed",
        container_statuses=[
            MockContainerStatus(name="app", restart_count=1, last_state=terminated),
        ],
    )
    mock_core_api.read_namespaced_pod.return_value = mock_pod

    with patch("resource_broker.performance_monitor.services.k8s_adapter.create_k8s_api") as mock_create:
        mock_create.return_value = mock_core_api

        adapter = KubernetesApiAdapter()
        status = await adapter.get_pod_status("default", "test-pod")

        assert status.pod_status == "Failed"
        assert status.restart_count == 1
        assert status.last_terminated_reason == "OOMKilled"


@pytest.mark.asyncio
async def test_get_pod_status_multiple_containers(mock_core_api: MagicMock) -> None:
    """Test pod status with multiple containers (restart counts should sum)."""
    mock_pod = MockPod(
        name="test-pod",
        namespace="default",
        phase="Running",
        container_statuses=[
            MockContainerStatus(name="app", restart_count=2),
            MockContainerStatus(name="sidecar", restart_count=3),
        ],
    )
    mock_core_api.read_namespaced_pod.return_value = mock_pod

    with patch("resource_broker.performance_monitor.services.k8s_adapter.create_k8s_api") as mock_create:
        mock_create.return_value = mock_core_api

        adapter = KubernetesApiAdapter()
        status = await adapter.get_pod_status("default", "test-pod")

        assert status.restart_count == 5


@pytest.mark.asyncio
async def test_get_pod_status_multiple_containers_first_terminated_reason(mock_core_api: MagicMock) -> None:
    """Test that last_terminated_reason picks the FIRST container with a terminated state, not the last.

    Regression test for bug where missing break statement caused the loop to overwrite
    last_terminated_reason on each match, resulting in the last container's reason instead of first.
    """
    first_terminated = MockContainerState(terminated=MockTerminatedState(reason="Error"))
    second_terminated = MockContainerState(terminated=MockTerminatedState(reason="OOMKilled"))

    mock_pod = MockPod(
        name="test-pod",
        namespace="default",
        phase="Failed",
        container_statuses=[
            MockContainerStatus(name="app", restart_count=1, last_state=first_terminated),
            MockContainerStatus(name="sidecar", restart_count=2, last_state=second_terminated),
        ],
    )
    mock_core_api.read_namespaced_pod.return_value = mock_pod

    with patch("resource_broker.performance_monitor.services.k8s_adapter.create_k8s_api") as mock_create:
        mock_create.return_value = mock_core_api

        adapter = KubernetesApiAdapter()
        status = await adapter.get_pod_status("default", "test-pod")

        # Should pick the first container's terminated reason ("Error"), not the second ("OOMKilled")
        assert status.last_terminated_reason == "Error"
        assert status.restart_count == 3


@pytest.mark.asyncio
async def test_get_pod_status_no_container_statuses(mock_core_api: MagicMock) -> None:
    """Test pod status when container_statuses is None or empty."""
    mock_pod = MockPod(
        name="test-pod",
        namespace="default",
        phase="Pending",
        container_statuses=None,
    )
    mock_core_api.read_namespaced_pod.return_value = mock_pod

    with patch("resource_broker.performance_monitor.services.k8s_adapter.create_k8s_api") as mock_create:
        mock_create.return_value = mock_core_api

        adapter = KubernetesApiAdapter()
        status = await adapter.get_pod_status("default", "test-pod")

        assert status.pod_status == "Pending"
        assert status.restart_count == 0
        assert status.last_terminated_reason is None


@pytest.mark.asyncio
async def test_get_configured_resources_with_requests_and_limits(mock_core_api: MagicMock) -> None:
    """Test getting configured resources with requests and limits."""
    mock_pod = MockPod(
        name="test-pod",
        namespace="default",
        containers=[
            MockContainer(
                name="app",
                resources=MockResourceRequirements(
                    requests={"cpu": "500m", "memory": "512Mi"},
                    limits={"cpu": "1", "memory": "1Gi", "ephemeral-storage": "2Gi"},
                ),
            ),
        ],
        labels={"app": "myapp", "version": "1.0"},
    )
    mock_core_api.read_namespaced_pod.return_value = mock_pod

    with patch("resource_broker.performance_monitor.services.k8s_adapter.create_k8s_api") as mock_create:
        mock_create.return_value = mock_core_api

        adapter = KubernetesApiAdapter()
        info = await adapter.get_configured_resources("default", "test-pod")

        assert info.pod_name == "test-pod"
        assert info.namespace == "default"
        assert info.resource_type == "k8s-pod"
        assert info.configured_resource["cpu_request"] == "500m"
        assert info.configured_resource["memory_request"] == "512Mi"
        assert info.configured_resource["cpu_limit"] == "1"
        assert info.configured_resource["memory_limit"] == "1Gi"
        assert info.configured_resource["ephemeral_storage"] == "2Gi"
        assert info.labels == {"app": "myapp", "version": "1.0"}


@pytest.mark.asyncio
async def test_get_configured_resources_no_requests_or_limits(mock_core_api: MagicMock) -> None:
    """Test defensive handling when requests/limits are None."""
    mock_pod = MockPod(
        name="test-pod",
        namespace="default",
        containers=[
            MockContainer(
                name="app",
                resources=MockResourceRequirements(
                    requests=None,
                    limits=None,
                ),
            ),
        ],
    )
    mock_core_api.read_namespaced_pod.return_value = mock_pod

    with patch("resource_broker.performance_monitor.services.k8s_adapter.create_k8s_api") as mock_create:
        mock_create.return_value = mock_core_api

        adapter = KubernetesApiAdapter()
        info = await adapter.get_configured_resources("default", "test-pod")

        assert info.configured_resource["cpu_request"] is None
        assert info.configured_resource["memory_request"] is None
        assert info.configured_resource["cpu_limit"] is None
        assert info.configured_resource["memory_limit"] is None
        assert info.configured_resource["ephemeral_storage"] is None


@pytest.mark.asyncio
async def test_get_configured_resources_no_resources_at_all(mock_core_api: MagicMock) -> None:
    """Test when container has no resources object."""
    mock_pod = MockPod(
        name="test-pod",
        namespace="default",
        containers=[
            MockContainer(
                name="app",
                resources=None,
            ),
        ],
    )
    mock_core_api.read_namespaced_pod.return_value = mock_pod

    with patch("resource_broker.performance_monitor.services.k8s_adapter.create_k8s_api") as mock_create:
        mock_create.return_value = mock_core_api

        adapter = KubernetesApiAdapter()
        info = await adapter.get_configured_resources("default", "test-pod")

        assert info.configured_resource["cpu_request"] is None
        assert info.configured_resource["memory_request"] is None


@pytest.mark.asyncio
async def test_get_configured_resources_no_containers(mock_core_api: MagicMock) -> None:
    """Test when pod has no containers."""
    mock_pod = MockPod(
        name="test-pod",
        namespace="default",
        containers=[],
    )
    mock_core_api.read_namespaced_pod.return_value = mock_pod

    with patch("resource_broker.performance_monitor.services.k8s_adapter.create_k8s_api") as mock_create:
        mock_create.return_value = mock_core_api

        adapter = KubernetesApiAdapter()
        info = await adapter.get_configured_resources("default", "test-pod")

        assert info.configured_resource["cpu_request"] is None
        assert info.configured_resource["memory_request"] is None


@pytest.mark.asyncio
async def test_get_configured_resources_no_labels(mock_core_api: MagicMock) -> None:
    """Test configured resources when pod has no labels."""
    mock_pod = MockPod(
        name="test-pod",
        namespace="default",
        containers=[
            MockContainer(
                name="app",
                resources=MockResourceRequirements(
                    requests={"cpu": "100m"},
                    limits=None,
                ),
            ),
        ],
        labels=None,
    )
    mock_core_api.read_namespaced_pod.return_value = mock_pod

    with patch("resource_broker.performance_monitor.services.k8s_adapter.create_k8s_api") as mock_create:
        mock_create.return_value = mock_core_api

        adapter = KubernetesApiAdapter()
        info = await adapter.get_configured_resources("default", "test-pod")

        assert info.labels == {}


def test_watch_pods_with_namespace(mock_core_api: MagicMock) -> None:
    """Test watch_pods with a specific namespace."""
    mock_event = {"type": "ADDED", "raw_object": {"metadata": {"name": "test-pod"}}}
    mock_stream = [mock_event]

    with patch("resource_broker.performance_monitor.services.k8s_adapter.create_k8s_api") as mock_create:
        with patch("resource_broker.performance_monitor.services.k8s_adapter.watch.Watch") as mock_watch_class:
            mock_create.return_value = mock_core_api
            mock_watch_instance = MagicMock()
            mock_watch_class.return_value = mock_watch_instance
            mock_watch_instance.stream.return_value = iter(mock_stream)

            adapter = KubernetesApiAdapter()
            events = list(adapter.watch_pods(namespace="default"))

            assert len(events) == 1
            assert events[0]["type"] == "ADDED"
            # Verify that list_namespaced_pod was called (not list_pod_for_all_namespaces)
            mock_watch_instance.stream.assert_called_once()
            call_args = mock_watch_instance.stream.call_args
            assert call_args[0][0] == mock_core_api.list_namespaced_pod
            assert call_args[1]["namespace"] == "default"


def test_watch_pods_all_namespaces(mock_core_api: MagicMock) -> None:
    """Test watch_pods without namespace (all namespaces)."""
    mock_event1 = {"type": "ADDED", "raw_object": {"metadata": {"name": "pod1"}}}
    mock_event2 = {"type": "MODIFIED", "raw_object": {"metadata": {"name": "pod2"}}}
    mock_stream = [mock_event1, mock_event2]

    with patch("resource_broker.performance_monitor.services.k8s_adapter.create_k8s_api") as mock_create:
        with patch("resource_broker.performance_monitor.services.k8s_adapter.watch.Watch") as mock_watch_class:
            mock_create.return_value = mock_core_api
            mock_watch_instance = MagicMock()
            mock_watch_class.return_value = mock_watch_instance
            mock_watch_instance.stream.return_value = iter(mock_stream)

            adapter = KubernetesApiAdapter()
            events = list(adapter.watch_pods())

            assert len(events) == 2
            assert events[0]["type"] == "ADDED"
            assert events[1]["type"] == "MODIFIED"
            # Verify that list_pod_for_all_namespaces was called
            mock_watch_instance.stream.assert_called_once()
            call_args = mock_watch_instance.stream.call_args
            assert call_args[0][0] == mock_core_api.list_pod_for_all_namespaces


def test_watch_pods_empty_namespace_string(mock_core_api: MagicMock) -> None:
    """Test that empty string namespace is treated as all namespaces."""
    with patch("resource_broker.performance_monitor.services.k8s_adapter.create_k8s_api") as mock_create:
        with patch("resource_broker.performance_monitor.services.k8s_adapter.watch.Watch") as mock_watch_class:
            mock_create.return_value = mock_core_api
            mock_watch_instance = MagicMock()
            mock_watch_class.return_value = mock_watch_instance
            mock_watch_instance.stream.return_value = iter([])

            adapter = KubernetesApiAdapter()
            list(adapter.watch_pods(namespace=""))

            # Verify that list_pod_for_all_namespaces was called (empty string is falsy)
            call_args = mock_watch_instance.stream.call_args
            assert call_args[0][0] == mock_core_api.list_pod_for_all_namespaces
