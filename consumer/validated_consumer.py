"""
VortexAI - Consumer (Hacker News edition)
Reads raw events from 'raw-events-topic', validates them with Pydantic,
routes valid events to 'validated-events-topic' and invalid ones to 'dlq-topic'.
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from confluent_kafka import Consumer, Producer
from pydantic import ValidationError

from schemas import HNStoryEvent
from logging_config import get_json_logger

logger = get_json_logger("vortex-consumer")


KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
SOURCE_TOPIC = "raw-events-topic"
VALIDATED_TOPIC = "validated-events-topic"
DLQ_TOPIC = "dlq-topic"


def build_consumer() -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "vortex-validation-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([SOURCE_TOPIC])
    return consumer


def build_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def send(producer: Producer, topic: str, key: str, value: dict) -> None:
    producer.produce(topic, key=key.encode("utf-8"), value=json.dumps(value).encode("utf-8"))
    producer.poll(0)


def main() -> None:
    logger.info("Starting consumer...")
    consumer = build_consumer()
    producer = build_producer()
    logger.info(f"Consuming '{SOURCE_TOPIC}' -> valid: '{VALIDATED_TOPIC}' | invalid: '{DLQ_TOPIC}'")

    valid_count, dlq_count = 0, 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            key = msg.key().decode("utf-8") if msg.key() else "unknown"

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                send(producer, DLQ_TOPIC, key, {
                    "reason": f"malformed_json: {str(e)}",
                    "original_event": msg.value().decode("utf-8", errors="replace"),
                })
                dlq_count += 1
                continue

            try:
                validated = HNStoryEvent(**payload)
                send(producer, VALIDATED_TOPIC, key, validated.model_dump())
                valid_count += 1
            except ValidationError as e:
                send(producer, DLQ_TOPIC, key, {
                    "reason": f"validation_failed: {e.errors()[0]['msg']}",
                    "original_event": payload,
                })
                dlq_count += 1

            total = valid_count + dlq_count
            if total % 20 == 0:
                logger.info(
                    f"Processed {total} | Valid: {valid_count} | DLQ: {dlq_count}",
                    extra={"total_processed": total, "valid_count": valid_count, "dlq_count": dlq_count},
                )

    except KeyboardInterrupt:
        logger.info(f"Shutting down. Final tally — Valid: {valid_count} | DLQ: {dlq_count}")
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()