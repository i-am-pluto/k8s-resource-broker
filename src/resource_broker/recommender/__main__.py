from __future__ import annotations

import typer
from structlog import get_logger

from resource_broker.common.logging import configure_logging

app = typer.Typer(
    name="recommender",
    help="Cron-style percentile recommendation engine + Deployment watch (issue #25)",
    no_args_is_help=True,
)

logger = get_logger(__name__)


@app.command()
def engine() -> None:
    """Run the engine cron loop. TODO(#25): wire up recommender.controllers.runner."""
    configure_logging()
    raise NotImplementedError("engine cron loop lands with issue #25")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
