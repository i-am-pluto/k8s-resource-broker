from __future__ import annotations

import typer
import uvicorn
from structlog import get_logger

from resource_broker.common.config import settings
from resource_broker.common.logging import configure_logging

app = typer.Typer(
    name="broker-api",
    help="Long-running server: webhook lookup, CRD cache, health/profile routes (issue #23)",
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
        "resource_broker.broker_api.app:app",
        host=h,
        port=p,
        workers=w,
        log_level=settings.log_level.value.lower(),
        reload=settings.environment == "development",
    )


def main() -> None:
    app()


if __name__ == "__main__":
    main()
