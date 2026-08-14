"""
VortexAI - Hybrid Search + Dynamic Groundedness Thresholding
Combines dense vector similarity (Qdrant/cosine) with BM25 keyword search,
fused via Reciprocal Rank Fusion (RRF). This catches cases pure dense search
misses -- e.g. short/bare-name queries with little semantic content but an
exact keyword match.

Instead of one fixed similarity threshold for every query, the groundedness
cutoff is calculated per-query from that query's own fused-score distribution
(mean + z * stdev). This adapts to how "spread out" or "clustered" a
particular query's results are, rather than applying one static number to
every query regardless of shape.
"""

from dashboard.app import QDRANT_API_KEY
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logging_config import get_json_logger

logger = get_json_logger("vortex-hybrid-search")

import pandas as pd
from rank_bm25 import BM25Okapi
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

SILVER_PATH = "storage/silver/silver_events.parquet"
QDRANT_HOST = os.environ.get("QDRANT_HOST", "127.0.0.1")
QDRANT_PORT = int(os.environ.get("QDRANT_PORT", "6333"))
COLLECTION_NAME = "vortex_events"
EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"

RRF_K = 60          # standard RRF smoothing constant (used in most published hybrid search implementations)
CANDIDATE_POOL = 20  # candidates pulled from each method before fusing
TOP_K = 5
DYNAMIC_Z = 0.5      # how many std-devs above the mean a result must clear, RELATIVE to its own query's pool

# Absolute floors: at least one of these must ALSO be cleared, regardless of how
# the pool is shaped. Without this, a query with zero good matches can still
# have its "least-bad" result pass a purely relative threshold, since roughly
# half of any distribution sits above its own mean by definition.
# Calibrated from observed real/fake match score gaps in this project's data.
ABSOLUTE_DENSE_FLOOR = 0.30   # true matches observed >= ~0.44; unrelated content observed <= ~0.17
ABSOLUTE_BM25_FLOOR = 5.0     # strong near-exact keyword matches observed >= ~8.2; weak/spurious overlap <= ~2.5

_bm25_index = None
_bm25_corpus_df = None


def _tokenize(text: str) -> list[str]:
    return text.lower().split()


def build_bm25_index():
    """Builds a keyword-search index over Silver's text column. Call once; cached globally after that."""
    global _bm25_index, _bm25_corpus_df
    df = pd.read_parquet(SILVER_PATH)
    df = df.dropna(subset=["text"]).reset_index(drop=True)
    tokenized = [_tokenize(t) for t in df["text"].tolist()]
    _bm25_index = BM25Okapi(tokenized)
    _bm25_corpus_df = df
    logger.info("Built BM25 index", extra={"row_count": len(df)})
    return _bm25_index, _bm25_corpus_df


def get_bm25_index():
    if _bm25_index is None:
        build_bm25_index()
    return _bm25_index, _bm25_corpus_df


def dense_search(model: SentenceTransformer, client: QdrantClient, query_text: str, top_k: int = CANDIDATE_POOL):
    query_vector = model.encode(query_text).tolist()
    return client.query_points(collection_name=COLLECTION_NAME, query=query_vector, limit=top_k).points


def keyword_search(query_text: str, top_k: int = CANDIDATE_POOL):
    index, df = get_bm25_index()
    scores = index.get_scores(_tokenize(query_text))
    ranked_idx = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:top_k]
    return [(df.iloc[i]["text"], float(scores[i]), df.iloc[i]) for i in ranked_idx]


def reciprocal_rank_fusion(dense_results, keyword_results, k: int = RRF_K):
    """
    Combines two ranked lists into one fused ranking. Each item's fused score is
    the sum of 1/(k + rank) across whichever list(s) it appears in -- so an item
    ranked highly by *either* method contributes meaningfully, and one ranked
    highly by *both* rises to the top.
    """
    fused_scores: dict[str, float] = {}
    item_lookup: dict[str, dict] = {}

    for rank, r in enumerate(dense_results):
        text = r.payload.get("text")
        if not text:
            continue
        fused_scores[text] = fused_scores.get(text, 0.0) + 1.0 / (k + rank + 1)
        item_lookup[text] = {"payload": r.payload, "dense_score": r.score, "bm25_score": None}

    for rank, (text, bm25_score, row) in enumerate(keyword_results):
        fused_scores[text] = fused_scores.get(text, 0.0) + 1.0 / (k + rank + 1)
        if text not in item_lookup:
            item_lookup[text] = {"payload": row.to_dict(), "dense_score": None, "bm25_score": None}
        item_lookup[text]["bm25_score"] = bm25_score

    return fused_scores, item_lookup


def compute_dynamic_threshold(scores: list[float], z: float = DYNAMIC_Z) -> float:
    """
    Per-query threshold: mean + z*stdev of THIS query's own fused-score pool.
    A tightly-clustered pool (all candidates similar) needs a result to stand
    out more than average to be trusted; a widely-spread pool with one clear
    winner will naturally separate that winner from the rest.
    """
    if len(scores) < 2:
        return 0.0
    mean = statistics.mean(scores)
    stdev = statistics.pstdev(scores)
    return mean + z * stdev


def hybrid_query_with_groundedness(query_text: str, top_k: int = TOP_K):
    model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    client = QdrantClient(
        url=QDRANT_HOST,
        api_key=QDRANT_API_KEY
    )

    dense_results = dense_search(model, client, query_text)
    keyword_results = keyword_search(query_text)

    fused_scores, item_lookup = reciprocal_rank_fusion(dense_results, keyword_results)

    all_scores = list(fused_scores.values())
    dynamic_threshold = compute_dynamic_threshold(all_scores)

    ranked = sorted(fused_scores.items(), key=lambda kv: kv[1], reverse=True)[:top_k]

    evaluated = []
    for text, fused_score in ranked:
        info = item_lookup[text]
        dense_score = info["dense_score"]
        bm25_score = info["bm25_score"]

        clears_dynamic = fused_score >= dynamic_threshold
        clears_absolute_floor = (
            (dense_score is not None and dense_score >= ABSOLUTE_DENSE_FLOOR)
            or (bm25_score is not None and bm25_score >= ABSOLUTE_BM25_FLOOR)
        )
        grounded = clears_dynamic and clears_absolute_floor

        evaluated.append({
            "text": text,
            "fused_score": round(fused_score, 5),
            "dense_score": round(dense_score, 4) if dense_score is not None else None,
            "bm25_score": round(bm25_score, 4) if bm25_score is not None else None,
            "grounded": grounded,
            "dynamic_threshold": round(dynamic_threshold, 5),
            "payload": info["payload"],
        })

    logger.info(
        "Hybrid query complete",
        extra={"query": query_text, "candidates_fused": len(fused_scores), "dynamic_threshold": round(dynamic_threshold, 5)},
    )
    return evaluated