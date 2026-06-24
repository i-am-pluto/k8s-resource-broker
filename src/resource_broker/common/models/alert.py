"""Domain models for the performance monitor's alert system.

AlertType categorises alerts (pressure, failure), Alert carries the
payload, and AlertSink is the abstract output port that concrete sinks
(e.g. LogAlertSink) implement.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class AlertType(StrEnum):
    PRESSURE = "pressure"
    FAILURE = "failure"


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
