"""
VortexAI - Bronze Layer Writer
Consumes 'validated-events-topic' and persists raw validated events to Parquet,
partitioned by date/hour. Flushes every 50 events or 30 seconds, whichever comes first.
Uses confluent-kafka (librdkafka-based) for reliability on Windows/Python 3.13.
"""

import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Consumer

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logging_config import get_json_logger

logger = get_json_logger("vortex-bronze")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
SOURCE_TOPIC = "validated-events-topic"
BRONZE_ROOT = "storage/bronze"

BATCH_SIZE = 50
FLUSH_INTERVAL_SECONDS = 30


def build_consumer() -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "vortex-bronze-writer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([SOURCE_TOPIC])
    return consumer


def flush_batch(buffer: list[dict]) -> None:
    if not buffer:
        return

    df = pd.DataFrame(buffer)

    now = datetime.now(timezone.utc)
    partition_dir = os.path.join(
        BRONZE_ROOT, f"date={now.strftime('%Y-%m-%d')}", f"hour={now.strftime('%H')}"
    )
    os.makedirs(partition_dir, exist_ok=True)

    filename = f"batch_{now.strftime('%H%M%S')}_{now.microsecond}.parquet"
    filepath = os.path.join(partition_dir, filename)

    df.to_parquet(filepath, engine="pyarrow", index=False)
    logger.info(f"Flushed {len(buffer)} events -> {filepath}")


def main() -> None:
    logger.info("Starting Bronze writer...")
    consumer = build_consumer()
    logger.info(f"Consuming '{SOURCE_TOPIC}' -> writing Parquet under '{BRONZE_ROOT}/'")

    buffer: list[dict] = []
    last_flush = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is not None and not msg.error():
                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                    payload["_kafka_key"] = msg.key().decode("utf-8") if msg.key() else "unknown"
                    payload["_ingested_at"] = datetime.now(timezone.utc).isoformat()
                    buffer.append(payload)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.warning(f"Skipping unparseable message in Bronze writer: {e}")
            elif msg is not None and msg.error():
                logger.error(f"Consumer error: {msg.error()}")

            should_flush_by_size = len(buffer) >= BATCH_SIZE
            should_flush_by_time = (time.time() - last_flush) >= FLUSH_INTERVAL_SECONDS and buffer

            if should_flush_by_size or should_flush_by_time:
                flush_batch(buffer)
                buffer = []
                last_flush = time.time()

    except KeyboardInterrupt:
        logger.info("Shutting down. Flushing remaining buffer...")
        flush_batch(buffer)
        consumer.close()


if __name__ == "__main__":
    main()