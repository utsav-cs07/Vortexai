"""
VortexAI - Groundedness Guardrail
Given a query, retrieves top-k matches from Qdrant and checks whether each
result is actually semantically grounded (cosine similarity above threshold)
rather than a weak/spurious match.
"""

import logging

from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("vortex-groundedness")

QDRANT_HOST = "127.0.0.1"
QDRANT_PORT = 6333
COLLECTION_NAME = "vortex_events"
EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"

# Qdrant already returns cosine similarity as the "score" when the collection
# uses Distance.COSINE, so we reuse that score directly as our groundedness signal.
GROUNDEDNESS_THRESHOLD = 0.50
TOP_K = 5


def query_with_groundedness(query_text: str, top_k: int = TOP_K, threshold: float = GROUNDEDNESS_THRESHOLD):
    model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

    query_vector = model.encode(query_text).tolist()

    results = client.query_points(
        collection_name=COLLECTION_NAME,
        query=query_vector,
        limit=top_k,
    ).points

    evaluated = []
    for r in results:
        grounded = r.score >= threshold
        evaluated.append({
            "text": r.payload.get("text"),
            "wiki": r.payload.get("wiki"),
            "cosine_score": round(r.score, 4),
            "grounded": grounded,
        })

    return evaluated


def print_report(query_text: str, results: list[dict]) -> None:
    logger.info(f"Query: '{query_text}'")
    logger.info(f"{'SCORE':<8} {'GROUNDED':<10} TEXT")
    for r in results:
        flag = "YES" if r["grounded"] else "NO (ungrounded)"
        logger.info(f"{r['cosine_score']:<8} {flag:<10} {r['text'][:80]}")


if __name__ == "__main__":
    # Quick manual test — replace with any query relevant to what's in your Silver data
    test_query = "Wikipedia article notability tag removed"
    results = query_with_groundedness(test_query)
    print_report(test_query, results)