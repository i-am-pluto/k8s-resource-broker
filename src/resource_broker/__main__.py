from __future__ import annotations

import asyncio

import typer
import uvicorn
from structlog import get_logger

from resource_broker.common.logging import configure_logging
from resource_broker.config import settings

app = typer.Typer(
    name="k8s-resource-broker",
    help="Profile-driven Kubernetes resource patching with pluggable recommendation algorithms",
    no_args_is_help=True,
)

logger = get_logger(__name__)


@app.command()
def serve(
    host: str = typer.Option(None, help="Bind address"),
    port: int = typer.Option(None, help="Bind port"),
    workers: int = typer.Option(None, help="Number of workers"),
) -> None:
    configure_logging()
    h = host or settings.api_host
    p = port or settings.api_port
    w = workers or settings.api_workers

    logger.info("starting API server", host=h, port=p, workers=w)
    uvicorn.run(
        "resource_broker.api.app:app",
        host=h,
        port=p,
        workers=w,
        log_level=settings.log_level.value.lower(),
        reload=settings.environment == "development",
    )


@app.command()
def controller() -> None:
    configure_logging()
    from resource_broker.common.services.metrics_factory import create_metrics_adapter
    from resource_broker.watcher.controllers.watcher import PodWatcher

    adapter = create_metrics_adapter(settings)
    watcher = PodWatcher(adapter=adapter)
    asyncio.run(watcher.run())


@app.command()
def scrape() -> None:
    """Alias for controller. Metrics collection is owned by the watcher process."""
    controller()


def main() -> None:
    app()


if __name__ == "__main__":
    main()
