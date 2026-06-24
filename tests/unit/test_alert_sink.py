from __future__ import annotations

import pytest
import structlog

from resource_broker.common.models.alert import Alert, AlertSink, AlertType
from resource_broker.performance_monitor.services.alert_sink import LogAlertSink


def test_alert_sink_is_abstract() -> None:
    with pytest.raises(TypeError, match="Can't instantiate abstract class"):
        AlertSink()


@pytest.mark.asyncio
async def test_log_alert_sink_emit_pressure_alert() -> None:
    sink = LogAlertSink()
    alert = Alert(
        alert_type=AlertType.PRESSURE,
        namespace="default",
        pod_name="test-pod",
        service_name="test-service",
        reason="Memory usage exceeded threshold",
        details={"current_usage": "512Mi", "limit": "256Mi"},
    )

    with structlog.testing.capture_logs() as cap_logs:
        await sink.emit(alert)

    assert len(cap_logs) == 1
    log_entry = cap_logs[0]
    assert log_entry["event"] == "performance_monitor_alert"
    assert log_entry["alert_type"] == "pressure"
    assert log_entry["namespace"] == "default"
    assert log_entry["pod_name"] == "test-pod"
    assert log_entry["service_name"] == "test-service"
    assert log_entry["reason"] == "Memory usage exceeded threshold"
    assert log_entry["current_usage"] == "512Mi"
    assert log_entry["limit"] == "256Mi"


@pytest.mark.asyncio
async def test_log_alert_sink_emit_failure_alert() -> None:
    sink = LogAlertSink()
    alert = Alert(
        alert_type=AlertType.FAILURE,
        namespace="production",
        pod_name="worker-42",
        service_name="worker-service",
        reason="OOMKilled",
        details={"exit_code": 137, "signal": "SIGKILL"},
    )

    with structlog.testing.capture_logs() as cap_logs:
        await sink.emit(alert)

    assert len(cap_logs) == 1
    log_entry = cap_logs[0]
    assert log_entry["event"] == "performance_monitor_alert"
    assert log_entry["alert_type"] == "failure"
    assert log_entry["namespace"] == "production"
    assert log_entry["pod_name"] == "worker-42"
    assert log_entry["service_name"] == "worker-service"
    assert log_entry["reason"] == "OOMKilled"
    assert log_entry["exit_code"] == 137
    assert log_entry["signal"] == "SIGKILL"


@pytest.mark.asyncio
async def test_log_alert_sink_emit_with_none_service_name() -> None:
    sink = LogAlertSink()
    alert = Alert(
        alert_type=AlertType.FAILURE,
        namespace="default",
        pod_name="orphan-pod",
        service_name=None,
        reason="FailedScheduling",
        details={},
    )

    with structlog.testing.capture_logs() as cap_logs:
        await sink.emit(alert)

    assert len(cap_logs) == 1
    log_entry = cap_logs[0]
    assert log_entry["event"] == "performance_monitor_alert"
    assert log_entry["alert_type"] == "failure"
    assert log_entry["service_name"] is None
    assert log_entry["reason"] == "FailedScheduling"


@pytest.mark.asyncio
async def test_log_alert_sink_emit_with_empty_details() -> None:
    sink = LogAlertSink()
    alert = Alert(
        alert_type=AlertType.PRESSURE,
        namespace="monitoring",
        pod_name="monitor-pod",
        service_name="monitoring-service",
        reason="High pressure detected",
        details={},
    )

    with structlog.testing.capture_logs() as cap_logs:
        await sink.emit(alert)

    assert len(cap_logs) == 1
    log_entry = cap_logs[0]
    assert log_entry["event"] == "performance_monitor_alert"
    assert log_entry["alert_type"] == "pressure"
    assert log_entry["reason"] == "High pressure detected"
    assert "current_usage" not in log_entry
