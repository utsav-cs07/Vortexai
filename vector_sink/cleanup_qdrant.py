"""
VortexAI - Qdrant Cleanup (one-time maintenance utility)
Deletes the existing collection (which has duplicate points from before
hash-based deterministic IDs were introduced) and re-syncs cleanly from Silver.
"""

import logging
import os

from qdrant_client import QdrantClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("vortex-cleanup")

QDRANT_HOST = "127.0.0.1"
QDRANT_PORT = 6333
COLLECTION_NAME = "vortex_events"
SYNCED_HASHES_PATH = "storage/silver/.synced_hashes.json"


def main() -> None:
    client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

    existing = [c.name for c in client.get_collections().collections]
    if COLLECTION_NAME in existing:
        logger.info(f"Deleting collection '{COLLECTION_NAME}' (removing duplicates)...")
        client.delete_collection(COLLECTION_NAME)
    else:
        logger.info("Collection doesn't exist yet, nothing to delete.")

    if os.path.exists(SYNCED_HASHES_PATH):
        os.remove(SYNCED_HASHES_PATH)
        logger.info("Cleared sync tracking file so all rows re-embed cleanly.")

    logger.info("Cleanup done. Now run: python vector_sink/qdrant_sync.py")


if __name__ == "__main__":
    main()