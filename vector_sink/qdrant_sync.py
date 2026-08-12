"""
VortexAI - Vector Sink
Reads Silver Parquet, embeds the cleaned text snippets with all-MiniLM-L6-v2,
and upserts them into Qdrant. Run as a batch job after silver_transform.py.
"""

import hashlib
import json
import logging
import os
import uuid

import pandas as pd
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams
from sentence_transformers import SentenceTransformer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("vortex-vector-sink")

SILVER_PATH = "storage/silver/silver_events.parquet"
SYNCED_HASHES_PATH = "storage/silver/.synced_hashes.json"
QDRANT_HOST = os.environ.get("QDRANT_HOST", "127.0.0.1")
QDRANT_PORT = int(os.environ.get("QDRANT_PORT", "6333"))
COLLECTION_NAME = "vortex_events"
EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384  # fixed output size for all-MiniLM-L6-v2
BATCH_SIZE = 64


def text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def load_synced_hashes() -> set:
    if os.path.exists(SYNCED_HASHES_PATH):
        with open(SYNCED_HASHES_PATH, "r") as f:
            return set(json.load(f))
    return set()


def save_synced_hashes(hashes: set) -> None:
    with open(SYNCED_HASHES_PATH, "w") as f:
        json.dump(list(hashes), f)


def ensure_collection(client: QdrantClient) -> None:
    existing = [c.name for c in client.get_collections().collections]
    if COLLECTION_NAME not in existing:
        logger.info(f"Creating Qdrant collection '{COLLECTION_NAME}'...")
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=EMBEDDING_DIM, distance=Distance.COSINE),
        )
    else:
        logger.info(f"Collection '{COLLECTION_NAME}' already exists.")


def load_silver() -> pd.DataFrame:
    if not os.path.exists(SILVER_PATH):
        logger.warning(f"No Silver data found at {SILVER_PATH}. Run silver_transform.py first.")
        return pd.DataFrame()
    return pd.read_parquet(SILVER_PATH)


def sync_to_qdrant(df: pd.DataFrame) -> None:
    if df.empty:
        logger.info("Nothing to sync.")
        return

    synced_hashes = load_synced_hashes()
    df = df.copy()
    df["content_hash"] = df["text"].apply(text_hash)

    new_rows = df[~df["content_hash"].isin(synced_hashes)]

    logger.info(f"Silver has {len(df)} total rows; {len(new_rows)} are new since last sync.")

    if new_rows.empty:
        logger.info("Nothing new to embed. Qdrant is already up to date.")
        return

    logger.info(f"Loading embedding model '{EMBEDDING_MODEL_NAME}'...")
    model = SentenceTransformer(EMBEDDING_MODEL_NAME)

    client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
    ensure_collection(client)

    texts = new_rows["text"].tolist()
    logger.info(f"Embedding {len(texts)} new snippets...")
    embeddings = model.encode(texts, batch_size=BATCH_SIZE, show_progress_bar=True)

    points = []
    for row, vector in zip(new_rows.itertuples(index=False), embeddings):
        points.append(
            PointStruct(
                id=str(uuid.uuid5(uuid.NAMESPACE_DNS, row.content_hash)),  # deterministic ID from content hash
                vector=vector.tolist(),
                payload={
                    "text": row.text,
                    "title": getattr(row, "title", None),
                    "id": getattr(row, "id", None),
                    "by": getattr(row, "by", None),
                    "url": getattr(row, "url", None),
                    "score": getattr(row, "score", None),
                    "time": getattr(row, "time", None),
                },
            )
        )

    logger.info(f"Upserting {len(points)} points into Qdrant collection '{COLLECTION_NAME}' in chunks...")
    UPSERT_CHUNK_SIZE = 500
    for i in range(0, len(points), UPSERT_CHUNK_SIZE):
        chunk = points[i:i + UPSERT_CHUNK_SIZE]
        client.upsert(collection_name=COLLECTION_NAME, points=chunk)
        logger.info(f"  Upserted {min(i + UPSERT_CHUNK_SIZE, len(points))}/{len(points)}")

    synced_hashes.update(new_rows["content_hash"].tolist())
    save_synced_hashes(synced_hashes)

    count = client.count(collection_name=COLLECTION_NAME).count
    logger.info(f"Sync complete. Collection now holds {count} total points.")


def main() -> None:
    df = load_silver()
    sync_to_qdrant(df)


if __name__ == "__main__":
    main()