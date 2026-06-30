"""TODO: LogAlertSink — concrete AlertSink that emits alerts via structlog.

The domain models (AlertType, Alert, AlertSink) have moved to
common/models/alert.py. Only the LogAlertSink implementation remains here.
"""

from __future__ import annotations

from structlog import get_logger

from resource_broker.common.models.alert import Alert, AlertSink

logger = get_logger(__name__)


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
