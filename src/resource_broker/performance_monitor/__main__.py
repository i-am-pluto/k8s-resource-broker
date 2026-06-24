from __future__ import annotations

import asyncio

import typer
from structlog import get_logger

from resource_broker.common.logging import configure_logging
from resource_broker.performance_monitor.controllers.scrape_runner import PodWatcher

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


def main() -> None:
    app()


if __name__ == "__main__":
    main()
