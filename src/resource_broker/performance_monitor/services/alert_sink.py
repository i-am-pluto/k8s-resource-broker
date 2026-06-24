from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from structlog import get_logger

logger = get_logger(__name__)


class AlertType(StrEnum):
    PRESSURE = "pressure"   # mem_usage/mem_limit >= threshold
    FAILURE = "failure"     # OOMKilled / FailedScheduling / eviction


@dataclass
class Alert:
    alert_type: AlertType
    namespace: str
    pod_name: str
    service_name: str | None
    reason: str
    details: dict[str, Any] = field(default_factory=dict)


class AlertSink(ABC):
    @abstractmethod
    async def emit(self, alert: Alert) -> None: ...


class LogAlertSink(AlertSink):
    async def emit(self, alert: Alert) -> None:
        logger.warning(
            "performance_monitor_alert",
            alert_type=alert.alert_type.value,
            namespace=alert.namespace,
            pod_name=alert.pod_name,
            service_name=alert.service_name,
            reason=alert.reason,
            **alert.details,
        )
