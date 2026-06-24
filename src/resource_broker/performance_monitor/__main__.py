from __future__ import annotations

import asyncio

import typer
from structlog import get_logger

from resource_broker.common.config import settings
from resource_broker.common.dao.database import get_session
from resource_broker.common.dao.repositories.performance import PerformanceRepository
from resource_broker.common.logging import configure_logging
from resource_broker.performance_monitor.controllers.scrape_runner import PodWatcher
from resource_broker.performance_monitor.services.alert_sink import LogAlertSink
from resource_broker.performance_monitor.services.k8s_adapter import KubernetesApiAdapter
from resource_broker.performance_monitor.services.metrics_factory import create_metrics_adapter
from resource_broker.performance_monitor.services.status_watcher import StatusWatcher
from resource_broker.performance_monitor.services.usage_collector import UsageCollector

app = typer.Typer(
    name="performance-monitor",
    help="Cron-style pod performance metrics scrape loop (issue #24)",
    no_args_is_help=True,
)

logger = get_logger(__name__)


@app.command()
def scrape() -> None:
    """Run the pod performance scrape loop. TODO(#24): replace with incremental watermark-based collector."""
    configure_logging()
    watcher = PodWatcher()
    asyncio.run(watcher.run())


@app.command()
def run() -> None:
    """Run performance monitor daemon: StatusWatcher + UsageCollector concurrently."""
    configure_logging()
    asyncio.run(_run_async())


async def _run_async() -> None:
    import signal

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_event.set)

    namespaces = settings.performance_monitor_namespaces or [settings.k8s_namespace]

    k8s_adapter = KubernetesApiAdapter()
    metrics_adapter = create_metrics_adapter(settings)
    alert_sink = LogAlertSink()

    async with get_session() as session:
        repo = PerformanceRepository(session)
        watcher = StatusWatcher(
            k8s_adapter=k8s_adapter,
            alert_sink=alert_sink,
            repo=repo,
            namespace=namespaces[0] if len(namespaces) == 1 else "",
        )
        collector = UsageCollector(
            metrics_adapter=metrics_adapter,
            k8s_adapter=k8s_adapter,
            alert_sink=alert_sink,
            repo=repo,
            namespaces=namespaces,
            interval_seconds=settings.scraper_interval_seconds,
            pressure_threshold=settings.pressure_threshold,
        )

        try:
            await asyncio.gather(
                watcher.run(shutdown_event),
                collector.run_forever(shutdown_event),
            )
        finally:
            await metrics_adapter.close()


def main() -> None:
    app()


if __name__ == "__main__":
    main()
