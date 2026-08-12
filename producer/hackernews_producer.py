"""
VortexAI - Producer (Hacker News edition)
Polls Hacker News's free public API for new stories and pushes them to
Kafka topic 'raw-events-topic'. No auth required.
"""

import json
import logging
import time

import requests
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("vortex-producer")

import os

HN_BASE = "https://hacker-news.firebaseio.com/v0"
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_TOPIC = "raw-events-topic"
POLL_INTERVAL_SECONDS = 10


def build_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed: {err}")


def fetch_new_story_ids() -> list:
    resp = requests.get(f"{HN_BASE}/newstories.json", timeout=10)
    resp.raise_for_status()
    return resp.json()


def fetch_item(item_id):
    resp = requests.get(f"{HN_BASE}/item/{item_id}.json", timeout=10)
    resp.raise_for_status()
    return resp.json()


def main() -> None:
    logger.info("Connecting to Kafka...")
    producer = build_producer()
    logger.info(f"Connected. Polling Hacker News -> topic '{KAFKA_TOPIC}'")

    seen_ids = set()
    sent = 0

    # Prime with current new-story IDs so we don't flood on first run with the full backlog
    seen_ids.update(fetch_new_story_ids())
    logger.info(f"Primed with {len(seen_ids)} existing story IDs. Watching for new ones...")

    while True:
        try:
            current_ids = fetch_new_story_ids()
            fresh_ids = [i for i in current_ids if i not in seen_ids]

            for item_id in fresh_ids:
                item = fetch_item(item_id)
                if not item or item.get("type") != "story":
                    seen_ids.add(item_id)
                    continue

                producer.produce(
                    KAFKA_TOPIC,
                    key=str(item.get("by", "unknown")).encode("utf-8"),
                    value=json.dumps(item).encode("utf-8"),
                    callback=delivery_report,
                )
                producer.poll(0)
                seen_ids.add(item_id)
                sent += 1

                if sent % 5 == 0:
                    logger.info(f"Sent {sent} stories so far")

            time.sleep(POLL_INTERVAL_SECONDS)

        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed ({e}). Retrying in 10s...")
            time.sleep(10)
        except KeyboardInterrupt:
            logger.info("Shutting down producer.")
            producer.flush()
            break


if __name__ == "__main__":
    main()