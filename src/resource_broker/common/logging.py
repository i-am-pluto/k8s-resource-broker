from __future__ import annotations

import logging
import sys

import structlog

from resource_broker.common.config import settings


def configure_logging() -> None:
    """Configure structlog with standard library integration."""
    timestamper = structlog.processors.TimeStamper(fmt="iso")
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        timestamper,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    if settings.environment == "development":
        renderer: structlog.types.Processor = structlog.dev.ConsoleRenderer()
    else:
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(settings.log_level.value)
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            _skip_unwanted_loggers,
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(settings.log_level.value)

    for name in ("uvicorn.access", "uvicorn.error", "httpx"):
        logging.getLogger(name).setLevel(logging.WARNING)


def _skip_unwanted_loggers(  # noqa: ARG001
    _logger: logging.Logger,
    _method_name: str,
    event_dict: structlog.types.EventDict,
) -> structlog.types.EventDict:
    """Suppress noisy third-party loggers."""
    name = event_dict.get("logger", "")
    if "uvicorn.access" in name and event_dict.get("status_code") == 200:
        raise structlog.DropEvent
    return event_dict
