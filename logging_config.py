"""
VortexAI - Structured Logging
Configures logs as JSON lines instead of plain text, so they can be parsed
by log aggregation tools (Datadog, CloudWatch, ELK, etc.) in a real deployment.

Usage in any script:
    from logging_config import get_json_logger
    logger = get_json_logger("vortex-consumer")
    logger.info("Processed event", extra={"event_id": 123, "valid": True})
"""

import json
import logging
from datetime import datetime, timezone


class JSONFormatter(logging.Formatter):
    """Formats each log record as a single JSON line."""

    # Standard LogRecord attributes we don't want duplicated in the "extra" section
    RESERVED = {
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "message", "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Include any custom fields passed via `extra={...}`
        extras = {k: v for k, v in record.__dict__.items() if k not in self.RESERVED}
        if extras:
            log_entry.update(extras)

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry)


def get_json_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Returns a logger configured to emit structured JSON lines to stdout."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid duplicate handlers if this is called more than once for the same logger
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JSONFormatter())
        logger.addHandler(handler)
        logger.propagate = False

    return logger