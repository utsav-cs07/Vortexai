"""
VortexAI - Hacker News Backfill (one-time bootstrap utility)
Pulls the last N historical HN items (not just brand-new ones) using
concurrent requests, so you have a meaningful dataset immediately instead
of waiting hours for live polling to accumulate enough data.
Run once, then let hackernews_producer.py handle ongoing live updates.
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("vortex-backfill")

HN_BASE = "https://hacker-news.firebaseio.com/v0"
KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
KAFKA_TOPIC = "raw-events-topic"

BACKFILL_COUNT = 1500   # how many recent items to pull
MAX_WORKERS = 20        # concurrent requests


def build_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def fetch_max_item_id() -> int:
    resp = requests.get(f"{HN_BASE}/maxitem.json", timeout=10)
    resp.raise_for_status()
    return resp.json()


def fetch_item(item_id: int):
    try:
        resp = requests.get(f"{HN_BASE}/item/{item_id}.json", timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException:
        return None


def main() -> None:
    producer = build_producer()

    max_id = fetch_max_item_id()
    ids_to_fetch = list(range(max_id - BACKFILL_COUNT, max_id))
    logger.info(f"Backfilling {len(ids_to_fetch)} historical items (IDs {ids_to_fetch[0]}-{ids_to_fetch[-1]})...")

    sent, skipped = 0, 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_item, i): i for i in ids_to_fetch}

        for future in as_completed(futures):
            item = future.result()
            if not item or item.get("type") != "story" or item.get("deleted") or item.get("dead"):
                skipped += 1
                continue

            producer.produce(
                KAFKA_TOPIC,
                key=str(item.get("by", "unknown")).encode("utf-8"),
                value=json.dumps(item).encode("utf-8"),
            )
            producer.poll(0)
            sent += 1

            if sent % 100 == 0:
                logger.info(f"Sent {sent} so far ({skipped} skipped non-stories)...")

    producer.flush()
    logger.info(f"Backfill complete. Sent {sent} stories, skipped {skipped} non-story/deleted items.")


if __name__ == "__main__":
    main()