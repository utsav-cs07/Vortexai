"""
VortexAI - Producer
Streams Wikimedia's public recent-changes SSE feed into Kafka topic 'raw-events-topic'.
Uses confluent-kafka (librdkafka-based) for reliability on Windows/Python 3.13.
"""

import json
import logging
import time

import requests
import sseclient
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("vortex-producer")

WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
KAFKA_TOPIC = "raw-events-topic"


def build_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed: {err}")


def stream_events(producer: Producer) -> None:
    headers = {
        "Accept": "text/event-stream",
        "User-Agent": "VortexAI/1.0 (Learning project; contact: your-email@example.com)",
    }
    response = requests.get(WIKIMEDIA_STREAM_URL, stream=True, headers=headers, timeout=15)
    response.raise_for_status()
    client = sseclient.SSEClient(response)

    sent, skipped = 0, 0

    for event in client.events():
        if event.event != "message" or not event.data:
            continue

        try:
            payload = json.loads(event.data)
        except json.JSONDecodeError:
            skipped += 1
            continue

        if payload.get("type") not in ("edit", "new"):
            skipped += 1
            continue

        key = payload.get("wiki", "unknown")

        producer.produce(
            KAFKA_TOPIC,
            key=key.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)  # trigger delivery callbacks without blocking
        sent += 1

        if sent % 5 == 0:
            logger.info(f"Sent {sent} events | Skipped {skipped}")
        if (sent + skipped) % 20 == 0:
            logger.info(f"...heartbeat: {sent} sent, {skipped} skipped so far")


def main() -> None:
    logger.info("Connecting to Kafka...")
    producer = build_producer()
    logger.info(f"Connected. Streaming from {WIKIMEDIA_STREAM_URL} -> topic '{KAFKA_TOPIC}'")

    while True:
        try:
            stream_events(producer)
        except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError) as e:
            logger.warning(f"Stream dropped ({e}). Reconnecting in 5s...")
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Shutting down producer.")
            producer.flush()
            break


if __name__ == "__main__":
    main()