"""Structured logging shared by all processor stages."""

import logging

import structlog


def configure_logging(*, json: bool = False, level: int = logging.INFO) -> None:
    """Configure structlog once per process.

    Args:
        json: Render records as JSON lines instead of human-readable console output.
        level: Minimum level to emit.
    """
    renderer = (
        structlog.processors.JSONRenderer() if json else structlog.dev.ConsoleRenderer()
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.typing.FilteringBoundLogger:
    """Return a logger bound to a component name."""
    return structlog.get_logger(component=name)
