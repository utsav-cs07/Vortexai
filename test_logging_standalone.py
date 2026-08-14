"""
Standalone test for structured JSON logging.
Run directly: python test_logging_standalone.py
No Docker, Kafka, or Qdrant needed - this only tests the logging format itself.
"""

from logging_config import get_json_logger

logger = get_json_logger("vortex-test")

logger.info("Pipeline started")
logger.info("Processed event", extra={"event_id": 42, "valid": True})
logger.warning("Slow response detected", extra={"latency_ms": 850, "topic": "raw-events-topic"})
logger.error("Validation failed", extra={"reason": "missing title", "event_id": 99})

try:
    1 / 0
except ZeroDivisionError:
    logger.exception("Unexpected error during processing", extra={"stage": "test"})

print("\nIf you see JSON lines above (not plain text), structured logging is working correctly.")