# """
# VortexAI - Telemetry Dashboard
# Live view of the pipeline: Kafka topic depths, Bronze/Silver row counts,
# Qdrant sync status, and an interactive groundedness query tester.

# Run with: streamlit run dashboard/app.py
# """

# import glob
# import json
# import os
# import re
# import sys
# import time
# from datetime import datetime

# import pandas as pd
# import pyarrow.parquet as pq
# import streamlit as st
# from confluent_kafka import Consumer, TopicPartition
# from qdrant_client import QdrantClient
# from sentence_transformers import SentenceTransformer

# PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.insert(0, os.path.join(PROJECT_ROOT, "vector_sink"))
# sys.path.insert(0, PROJECT_ROOT)
# from hybrid_search import hybrid_query_with_groundedness

# import os
# os.environ["HF_HUB_OFFLINE"] = "1"

# KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
# TOPICS = ["raw-events-topic", "validated-events-topic", "dlq-topic"]

# BRONZE_ROOT = "storage/bronze"
# SILVER_PATH = "storage/silver/silver_events.parquet"
# SYNCED_HASHES_PATH = "storage/silver/.synced_hashes.json"

# QDRANT_HOST = os.environ.get("QDRANT_HOST", "127.0.0.1")
# QDRANT_PORT = int(os.environ.get("QDRANT_PORT", "6333"))
# COLLECTION_NAME = "vortex_events"
# EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"
# GROUNDEDNESS_THRESHOLD = 0.50
# TOP_K = 5

# st.set_page_config(page_title="VortexAI Telemetry", layout="wide", page_icon="🌀")

# st.markdown("""
# <style>
#     [data-testid="stMetric"] {
#         background-color: #131A21;
#         border: 1px solid #1F2A33;
#         border-left: 3px solid #2DD4BF;
#         border-radius: 6px;
#         padding: 14px 16px;
#     }
#     [data-testid="stMetricValue"] {
#         font-family: 'SF Mono', Consolas, monospace;
#         font-size: 1.9rem;
#     }
#     [data-testid="stMetricLabel"] {
#         color: #94A3B8;
#         text-transform: uppercase;
#         letter-spacing: 0.05em;
#         font-size: 0.72rem;
#     }
#     div[data-testid="stDataFrame"] {
#         border: 1px solid #1F2A33;
#         border-radius: 6px;
#     }
#     .health-banner {
#         border-radius: 8px;
#         padding: 16px 20px;
#         margin-bottom: 18px;
#         font-family: 'SF Mono', Consolas, monospace;
#         font-size: 1.05rem;
#         display: flex;
#         align-items: center;
#         gap: 12px;
#     }
#     .health-ok { background-color: rgba(45, 212, 191, 0.10); border: 1px solid #2DD4BF; color: #2DD4BF; }
#     .health-warn { background-color: rgba(245, 158, 11, 0.10); border: 1px solid #F59E0B; color: #F59E0B; }
#     .health-bad { background-color: rgba(239, 68, 68, 0.10); border: 1px solid #EF4444; color: #EF4444; }
# </style>
# """, unsafe_allow_html=True)


# # ---------- Cached, expensive resources (load once, reuse across reruns) ----------

# @st.cache_resource
# def get_embedding_model():
#     return SentenceTransformer(EMBEDDING_MODEL_NAME)


# @st.cache_resource
# def get_qdrant_client():
#     return QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)


# @st.cache_resource
# def get_kafka_probe_consumer():
#     return Consumer({
#         "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
#         "group.id": "vortex-dashboard-probe",
#         "enable.auto.commit": False,
#     })


# # ---------- Data collection ----------

# @st.cache_data(ttl=4)
# def get_topic_message_count(topic: str) -> int:
#     """Total messages ever written to a topic, via partition watermark offsets (no consuming)."""
#     consumer = get_kafka_probe_consumer()
#     try:
#         metadata = consumer.list_topics(topic, timeout=2)
#         if topic not in metadata.topics or metadata.topics[topic].error is not None:
#             return 0
#         partitions = metadata.topics[topic].partitions.keys()
#         total = 0
#         for p in partitions:
#             low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=2, cached=False)
#             total += (high - low)
#         return total
#     except Exception:
#         return 0


# @st.cache_data(ttl=4)
# def get_bronze_stats() -> dict:
#     files = glob.glob(os.path.join(BRONZE_ROOT, "date=*", "hour=*", "*.parquet"))
#     total_rows = 0
#     for f in files:
#         try:
#             total_rows += pq.ParquetFile(f).metadata.num_rows
#         except Exception:
#             continue
#     return {"file_count": len(files), "row_count": total_rows}


# @st.cache_data(ttl=4)
# def get_silver_stats() -> dict:
#     if not os.path.exists(SILVER_PATH):
#         return {"row_count": 0, "last_modified": None}
#     row_count = pq.ParquetFile(SILVER_PATH).metadata.num_rows
#     mtime = datetime.fromtimestamp(os.path.getmtime(SILVER_PATH))
#     return {"row_count": row_count, "last_modified": mtime}


# @st.cache_data(ttl=4)
# def get_qdrant_stats() -> dict:
#     client = get_qdrant_client()
#     try:
#         count = client.count(collection_name=COLLECTION_NAME).count
#     except Exception:
#         count = 0

#     synced_count = 0
#     last_sync = None
#     if os.path.exists(SYNCED_HASHES_PATH):
#         with open(SYNCED_HASHES_PATH, "r") as f:
#             synced_count = len(json.load(f))
#         last_sync = datetime.fromtimestamp(os.path.getmtime(SYNCED_HASHES_PATH))

#     return {"point_count": count, "synced_hash_count": synced_count, "last_sync": last_sync}


# def build_hn_url(item_id) -> str:
#     """Construct the real Hacker News discussion link for an item ID."""
#     if not item_id:
#         return ""
#     return f"https://news.ycombinator.com/item?id={int(item_id)}"


# def split_title_and_details(payload: dict) -> tuple[str, str]:
#     """Split the stored snippet into a clean article title and edit-detail text."""
#     title = (payload.get("title") or "").strip()
#     text = (payload.get("text") or "").strip()

#     if title and text.startswith(title + ":"):
#         details = text[len(title) + 1:].strip()
#     elif ": " in text:
#         maybe_title, details = text.split(": ", 1)
#         title = title or maybe_title.strip()
#         details = details.strip()
#     else:
#         details = "" if title else text

#     return title, details


# def clean_event_details(details: str) -> str:
#     """Strip leftover bracketed section markers and common bot-tool tags for readability."""
#     if not details:
#         return "(no edit summary provided)"

#     # Remove bracketed section markers like "[External links]" (leading or inline)
#     details = re.sub(r"\[[^\]]{0,60}\]\s*", "", details)
#     # Remove common bot/tool signatures, e.g. "#IABot (v2.0.9.5)" or "using HotCat"
#     details = re.sub(r"#\w+\s*\([^)]*\)", "", details)
#     details = re.sub(r"\busing HotCat\b", "", details, flags=re.IGNORECASE)
#     # Collapse extra whitespace left behind
#     details = re.sub(r"\s+", " ", details).strip(" -;:")

#     return details if details else "(no edit summary provided)"


# def run_groundedness_query(query_text: str, top_k: int = TOP_K):
#     results = hybrid_query_with_groundedness(query_text, top_k=top_k)

#     rows = []
#     for r in results:
#         title, raw_details = split_title_and_details(r["payload"])
#         rows.append({
#             "Dense": f"{r['dense_score']:.4f}" if r["dense_score"] is not None else "—",
#             "BM25": f"{r['bm25_score']:.4f}" if r["bm25_score"] is not None else "—",
#             "Fused": f"{r['fused_score']:.5f}",
#             "Threshold": f"{r['dynamic_threshold']:.5f}",
#             "Grounded": "Yes" if r["grounded"] else "No",
#             "Article Title": title or "(untitled)",
#             "Event / Edit Details": clean_event_details(raw_details),
#             "Author": r["payload"].get("by"),
#             "Hacker News Link": build_hn_url(r["payload"].get("id")),
#         })
#     return rows


# # ---------- UI ----------

# st.title("VortexAI — Real-Time Pipeline Telemetry")

# col1, col2 = st.columns([1, 3])
# with col1:
#     auto_refresh = st.checkbox("Auto-refresh (every 5s)", value=True)
# with col2:
#     if st.button("Refresh now"):
#         st.rerun()

# # --- Compute all stats up front so the health banner can synthesize them ---
# topic_counts = {t: get_topic_message_count(t) for t in TOPICS}
# bronze_stats = get_bronze_stats()
# silver_stats = get_silver_stats()
# qdrant_stats = get_qdrant_stats()

# processed = topic_counts["validated-events-topic"] + topic_counts["dlq-topic"]
# dlq_rate = (topic_counts["dlq-topic"] / processed * 100) if processed > 0 else 0
# sync_gap = max(silver_stats["row_count"] - qdrant_stats["synced_hash_count"], 0)

# # --- Health banner: one glance, one verdict ---
# if processed == 0:
#     banner_class, icon, message = "health-warn", "🟡", "Waiting for data — pipeline scripts not yet producing events."
# elif dlq_rate > 15:
#     banner_class, icon, message = "health-bad", "🔴", f"Elevated DLQ rejection rate ({dlq_rate:.1f}%) — check validation upstream."
# elif sync_gap > 0:
#     banner_class, icon, message = "health-warn", "🟡", f"{sync_gap} Silver rows pending Qdrant sync — run qdrant_sync.py."
# else:
#     banner_class, icon, message = "health-ok", "🟢", f"Pipeline healthy — {processed} events processed, {dlq_rate:.1f}% DLQ rate, vector store in sync."

# st.markdown(
#     f'<div class="health-banner {banner_class}">{icon}&nbsp;&nbsp;<strong>{message}</strong></div>',
#     unsafe_allow_html=True,
# )
# st.caption(f"Last refreshed: {datetime.now().strftime('%H:%M:%S')}")

# st.divider()

# # --- Kafka topic depths ---
# st.subheader("Kafka Pipeline Flow")

# k1, k2, k3 = st.columns(3)
# k1.metric("Raw Events", topic_counts["raw-events-topic"])
# k2.metric("Validated Events", topic_counts["validated-events-topic"])
# k3.metric("DLQ (Rejected)", topic_counts["dlq-topic"])

# if processed > 0:
#     st.progress(min(dlq_rate / 100, 1.0), text=f"DLQ rejection rate: {dlq_rate:.1f}% of processed events")
# else:
#     st.info("No events processed yet — start the pipeline scripts to see live stats.")

# st.divider()

# # --- Medallion storage ---
# st.subheader("Medallion Storage")

# b1, b2, b3 = st.columns(3)
# b1.metric("Bronze Files", bronze_stats["file_count"])
# b2.metric("Bronze Rows", bronze_stats["row_count"])
# b3.metric("Silver Rows (cleaned)", silver_stats["row_count"])

# if silver_stats["last_modified"]:
#     st.caption(f"Silver last rebuilt: {silver_stats['last_modified'].strftime('%Y-%m-%d %H:%M:%S')}")
# else:
#     st.caption("Silver has not been generated yet — run silver_transform.py")

# st.divider()

# # --- Vector sync ---
# st.subheader("Vector Sink (Qdrant)")

# q1, q2 = st.columns(2)
# q1.metric("Points in Qdrant", qdrant_stats["point_count"])
# q2.metric("Synced (hash-tracked)", qdrant_stats["synced_hash_count"])

# if sync_gap > 0:
#     st.warning(f"{sync_gap} Silver rows not yet synced to Qdrant — run qdrant_sync.py")
# elif qdrant_stats["last_sync"]:
#     st.caption(f"Last synced: {qdrant_stats['last_sync'].strftime('%Y-%m-%d %H:%M:%S')}")

# st.divider()

# def get_sample_queries(n: int = 5) -> list[str]:
#     """Pull a few real titles from Silver data to use as guaranteed-relevant example queries."""
#     if not os.path.exists(SILVER_PATH):
#         return []
#     try:
#         df = pd.read_parquet(SILVER_PATH, columns=["title"])
#         df = df.dropna().drop_duplicates()
#         if df.empty:
#             return []
#         sample = df.sample(min(n, len(df)), random_state=None)
#         return sample["title"].tolist()
#     except Exception:
#         return []


# # --- Groundedness tester ---
# st.subheader("Groundedness Guardrail — Live Tester")
# st.caption("Query your live vector store and see which results pass the cosine-similarity groundedness check.")
# st.caption("Not sure what to search? Your data changes every time the pipeline runs, so here are real examples pulled from what's actually in your collection right now:")

# if "sample_queries" not in st.session_state:
#     st.session_state["sample_queries"] = get_sample_queries(5)
# sample_queries = st.session_state["sample_queries"]

# if sample_queries:
#     cols = st.columns(len(sample_queries) + 1)
#     for i, sample in enumerate(sample_queries):
#         label = sample[:30] + "..." if len(sample) > 30 else sample
#         if cols[i].button(label, key=f"sample_{i}"):
#             st.session_state["query_input"] = sample
#     if cols[-1].button("🔀 New examples"):
#         st.session_state["sample_queries"] = get_sample_queries(5)
#         st.rerun()

# if "query_input" not in st.session_state:
#     st.session_state["query_input"] = sample_queries[0] if sample_queries else "artificial intelligence"

# query_text = st.text_input("Enter a test query", key="query_input")
# if st.button("Run query") and query_text.strip():
#     if qdrant_stats["point_count"] == 0:
#         st.error("Qdrant collection is empty — run qdrant_sync.py first.")
#     else:
#         with st.spinner("Running hybrid search (dense + BM25) and fusing results..."):
#             results = run_groundedness_query(query_text)

#         df = pd.DataFrame(results, columns=[
#             "Dense", "BM25", "Fused", "Threshold", "Grounded", "Article Title", "Event / Edit Details", "Author", "Hacker News Link"
#         ])

#         st.caption(
#             "**Dense** = semantic similarity (embeddings) · **BM25** = keyword overlap · "
#             "**Fused** = combined rank (Reciprocal Rank Fusion) · **Threshold** = per-query dynamic cutoff "
#             "(mean + 0.5×stdev of this query's own result spread, plus an absolute floor on Dense or BM25)"
#         )

#         st.dataframe(
#             df,
#             width="stretch",
#             hide_index=True,
#             column_config={
#                 "Dense": st.column_config.TextColumn("Dense", width="small"),
#                 "BM25": st.column_config.TextColumn("BM25", width="small"),
#                 "Fused": st.column_config.TextColumn("Fused", width="small"),
#                 "Threshold": st.column_config.TextColumn("Threshold", width="small"),
#                 "Grounded": st.column_config.TextColumn("Grounded", width="small"),
#                 "Article Title": st.column_config.TextColumn("Article Title", width="medium"),
#                 "Event / Edit Details": st.column_config.TextColumn("Event / Edit Details", width="large"),
#                 "Author": st.column_config.TextColumn("Author", width="small"),
#                 "Hacker News Link": st.column_config.LinkColumn("Hacker News Link", display_text="Open →", width="small"),
#             },
#         )

# st.divider()
# st.caption("VortexAI — real-time streaming lakehouse with validated RAG ingestion")

# # --- Auto-refresh loop ---
# if auto_refresh:
#     time.sleep(5)
#     st.rerun()







"""
VortexAI - Telemetry Dashboard
Live view of the pipeline: Kafka topic depths, Bronze/Silver row counts,
Qdrant sync status, and an interactive groundedness query tester.

Run with: streamlit run dashboard/app.py
"""

import glob
import json
import os
import re
import sys
import time
from datetime import datetime

import pandas as pd
import pyarrow.parquet as pq
import streamlit as st
from confluent_kafka import Consumer, TopicPartition
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "vector_sink"))
sys.path.insert(0, PROJECT_ROOT)
from hybrid_search import hybrid_query_with_groundedness

import os
os.environ["HF_HUB_OFFLINE"] = "1"

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
TOPICS = ["raw-events-topic", "validated-events-topic", "dlq-topic"]

BRONZE_ROOT = "storage/bronze"
SILVER_PATH = "storage/silver/silver_events.parquet"
SYNCED_HASHES_PATH = "storage/silver/.synced_hashes.json"

QDRANT_HOST = os.environ.get("QDRANT_HOST", "127.0.0.1")
QDRANT_PORT = int(os.environ.get("QDRANT_PORT", "6333"))
COLLECTION_NAME = "vortex_events"
EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"
GROUNDEDNESS_THRESHOLD = 0.50
TOP_K = 5

st.set_page_config(page_title="VortexAI Telemetry", layout="wide", page_icon="🌀")

st.markdown("""
<style>
    [data-testid="stMetric"] {
        background-color: #131A21;
        border: 1px solid #1F2A33;
        border-left: 3px solid #2DD4BF;
        border-radius: 6px;
        padding: 14px 16px;
    }
    [data-testid="stMetricValue"] {
        font-family: 'SF Mono', Consolas, monospace;
        font-size: 1.9rem;
    }
    [data-testid="stMetricLabel"] {
        color: #94A3B8;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-size: 0.72rem;
    }
    div[data-testid="stDataFrame"] {
        border: 1px solid #1F2A33;
        border-radius: 6px;
    }
    .health-banner {
        border-radius: 8px;
        padding: 16px 20px;
        margin-bottom: 18px;
        font-family: 'SF Mono', Consolas, monospace;
        font-size: 1.05rem;
        display: flex;
        align-items: center;
        gap: 12px;
    }
    .health-ok { background-color: rgba(45, 212, 191, 0.10); border: 1px solid #2DD4BF; color: #2DD4BF; }
    .health-warn { background-color: rgba(245, 158, 11, 0.10); border: 1px solid #F59E0B; color: #F59E0B; }
    .health-bad { background-color: rgba(239, 68, 68, 0.10); border: 1px solid #EF4444; color: #EF4444; }
    .section-badge {
        color: #94A3B8;
        font-size: 0.85rem;
        margin-top: -8px;
        margin-bottom: 14px;
    }
</style>
""", unsafe_allow_html=True)


# ---------- Cached, expensive resources (load once, reuse across reruns) ----------

@st.cache_resource
def get_embedding_model():
    return SentenceTransformer(EMBEDDING_MODEL_NAME)


@st.cache_resource
def get_qdrant_client():
    return QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)


@st.cache_resource
def get_kafka_probe_consumer():
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "vortex-dashboard-probe",
        "enable.auto.commit": False,
    })


# ---------- Data collection ----------

@st.cache_data(ttl=4)
def get_topic_message_count(topic: str) -> int:
    """Total messages ever written to a topic, via partition watermark offsets (no consuming)."""
    consumer = get_kafka_probe_consumer()
    try:
        metadata = consumer.list_topics(topic, timeout=2)
        if topic not in metadata.topics or metadata.topics[topic].error is not None:
            return 0
        partitions = metadata.topics[topic].partitions.keys()
        total = 0
        for p in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=2, cached=False)
            total += (high - low)
        return total
    except Exception:
        return 0


@st.cache_data(ttl=4)
def get_bronze_stats() -> dict:
    files = glob.glob(os.path.join(BRONZE_ROOT, "date=*", "hour=*", "*.parquet"))
    total_rows = 0
    for f in files:
        try:
            total_rows += pq.ParquetFile(f).metadata.num_rows
        except Exception:
            continue
    return {"file_count": len(files), "row_count": total_rows}


@st.cache_data(ttl=4)
def get_silver_stats() -> dict:
    if not os.path.exists(SILVER_PATH):
        return {"row_count": 0, "last_modified": None}
    row_count = pq.ParquetFile(SILVER_PATH).metadata.num_rows
    mtime = datetime.fromtimestamp(os.path.getmtime(SILVER_PATH))
    return {"row_count": row_count, "last_modified": mtime}


@st.cache_data(ttl=4)
def get_qdrant_stats() -> dict:
    client = get_qdrant_client()
    try:
        count = client.count(collection_name=COLLECTION_NAME).count
    except Exception:
        count = 0

    synced_count = 0
    last_sync = None
    if os.path.exists(SYNCED_HASHES_PATH):
        with open(SYNCED_HASHES_PATH, "r") as f:
            synced_count = len(json.load(f))
        last_sync = datetime.fromtimestamp(os.path.getmtime(SYNCED_HASHES_PATH))

    return {"point_count": count, "synced_hash_count": synced_count, "last_sync": last_sync}


def build_hn_url(item_id) -> str:
    """Construct the real Hacker News discussion link for an item ID."""
    if not item_id:
        return ""
    return f"https://news.ycombinator.com/item?id={int(item_id)}"


def split_title_and_details(payload: dict) -> tuple[str, str]:
    """Split the stored snippet into a clean article title and edit-detail text."""
    title = (payload.get("title") or "").strip()
    text = (payload.get("text") or "").strip()

    if title and text.startswith(title + ":"):
        details = text[len(title) + 1:].strip()
    elif ": " in text:
        maybe_title, details = text.split(": ", 1)
        title = title or maybe_title.strip()
        details = details.strip()
    else:
        details = "" if title else text

    return title, details


def clean_event_details(details: str) -> str:
    """Strip leftover bracketed section markers and common bot-tool tags for readability."""
    if not details:
        return "(no edit summary provided)"

    # Remove bracketed section markers like "[External links]" (leading or inline)
    details = re.sub(r"\[[^\]]{0,60}\]\s*", "", details)
    # Remove common bot/tool signatures, e.g. "#IABot (v2.0.9.5)" or "using HotCat"
    details = re.sub(r"#\w+\s*\([^)]*\)", "", details)
    details = re.sub(r"\busing HotCat\b", "", details, flags=re.IGNORECASE)
    # Collapse extra whitespace left behind
    details = re.sub(r"\s+", " ", details).strip(" -;:")

    return details if details else "(no edit summary provided)"


def run_groundedness_query(query_text: str, top_k: int = TOP_K):
    results = hybrid_query_with_groundedness(query_text, top_k=top_k)

    rows = []
    for r in results:
        title, raw_details = split_title_and_details(r["payload"])
        rows.append({
            "Dense": f"{r['dense_score']:.4f}" if r["dense_score"] is not None else "—",
            "BM25": f"{r['bm25_score']:.4f}" if r["bm25_score"] is not None else "—",
            "Fused": f"{r['fused_score']:.5f}",
            "Threshold": f"{r['dynamic_threshold']:.5f}",
            "Grounded": "Yes" if r["grounded"] else "No",
            "Article Title": title or "(untitled)",
            "Event / Edit Details": clean_event_details(raw_details),
            "Author": r["payload"].get("by"),
            "Hacker News Link": build_hn_url(r["payload"].get("id")),
        })
    return rows


# ---------- UI ----------

st.title("🌀 VortexAI — Real-Time Pipeline Telemetry")

col1, col2 = st.columns([1, 3])
with col1:
    auto_refresh = st.checkbox("Auto-refresh (every 5s)", value=True, help="Automatically polls Kafka watermark offsets and disk storage stats every 5 seconds.")
with col2:
    if st.button("Refresh now", help="Manually re-fetch latest partition counts and lakehouse file sizes immediately."):
        st.rerun()

# --- Compute all stats up front so the health banner can synthesize them ---
topic_counts = {t: get_topic_message_count(t) for t in TOPICS}
bronze_stats = get_bronze_stats()
silver_stats = get_silver_stats()
qdrant_stats = get_qdrant_stats()

processed = topic_counts["validated-events-topic"] + topic_counts["dlq-topic"]
dlq_rate = (topic_counts["dlq-topic"] / processed * 100) if processed > 0 else 0
sync_gap = max(silver_stats["row_count"] - qdrant_stats["synced_hash_count"], 0)

# --- Health banner: one glance, one verdict ---
if processed == 0:
    banner_class, icon, message = "health-warn", "🟡", "Waiting for data — pipeline scripts not yet producing events."
elif dlq_rate > 15:
    banner_class, icon, message = "health-bad", "🔴", f"Elevated DLQ rejection rate ({dlq_rate:.1f}%) — check validation upstream."
elif sync_gap > 0:
    banner_class, icon, message = "health-warn", "🟡", f"{sync_gap} Silver rows pending Qdrant sync — run qdrant_sync.py."
else:
    banner_class, icon, message = "health-ok", "🟢", f"Pipeline healthy — {processed} events processed, {dlq_rate:.1f}% DLQ rate, vector store in sync."

st.markdown(
    f'<div class="health-banner {banner_class}">{icon}&nbsp;&nbsp;<strong>{message}</strong></div>',
    unsafe_allow_html=True,
)
st.caption(f"Last refreshed: {datetime.now().strftime('%H:%M:%S')}")

st.divider()

# --- Kafka topic depths ---
st.subheader(
    "1. Kafka Streaming Ingestion Flow",
    help="Real-time distributed log message broker. Tracks incoming streaming events, validation checkpoints, and quarantine routing."
)
st.markdown('<div class="section-badge">⚡ Ingests live data stream, applies Pydantic validation, and isolates corrupted payloads</div>', unsafe_allow_html=True)

k1, k2, k3 = st.columns(3)
k1.metric(
    "Raw Events", 
    topic_counts["raw-events-topic"],
    help="Topic: 'raw-events-topic'\nTotal unvalidated JSON payloads ingested directly from the producer stream."
)
k2.metric(
    "Validated Events", 
    topic_counts["validated-events-topic"],
    help="Topic: 'validated-events-topic'\nEvents that strictly conformed to the HNStoryEvent Pydantic schema and passed downstream."
)
k3.metric(
    "DLQ (Rejected)", 
    topic_counts["dlq-topic"],
    help="Topic: 'dlq-topic' (Dead Letter Queue)\nCorrupted or malformed events quarantined with error traces for inspection."
)

if processed > 0:
    st.progress(
        min(dlq_rate / 100, 1.0), 
        text=f"DLQ rejection rate: {dlq_rate:.1f}% of processed events (Target: < 5%)"
    )
else:
    st.info("No events processed yet — start the pipeline scripts to see live stats.")

st.divider()

# --- Medallion storage ---
st.subheader(
    "2. Medallion Lakehouse Storage",
    help="Multi-hop storage architecture. Batches validated Kafka events into immutable raw Parquet files (Bronze), then cleans and deduplicates them (Silver)."
)
st.markdown('<div class="section-badge">📦 Partitioned columnar persistence (Date/Hour) for analytics, deduplication, and compaction</div>', unsafe_allow_html=True)

b1, b2, b3 = st.columns(3)
b1.metric(
    "Bronze Files", 
    bronze_stats["file_count"],
    help="Directory: 'storage/bronze/date=*/hour=*'\nCount of individual Parquet batch files written every 50 events or 30 seconds."
)
b2.metric(
    "Bronze Rows", 
    bronze_stats["row_count"],
    help="Total cumulative raw records stored across all partitioned Bronze Parquet files."
)
b3.metric(
    "Silver Rows (Cleaned)", 
    silver_stats["row_count"],
    help="File: 'storage/silver/silver_events.parquet'\nDeduplicated, type-cast, and curated dataset ready for vector embedding and analytics."
)

if silver_stats["last_modified"]:
    st.caption(f"Silver last rebuilt: {silver_stats['last_modified'].strftime('%Y-%m-%d %H:%M:%S')}")
else:
    st.caption("Silver has not been generated yet — run silver_transform.py")

st.divider()

# --- Vector sync ---
st.subheader(
    "3. Vector Sink & RAG Memory (Qdrant)",
    help="Vector database storing 384-dimensional dense semantic embeddings + sparse BM25 keyword tokens for RAG retrieval."
)
st.markdown('<div class="section-badge">🧠 Synchronizes curated Silver rows into Qdrant using incremental SHA-256 hash tracking</div>', unsafe_allow_html=True)

q1, q2 = st.columns(2)
q1.metric(
    "Points in Qdrant", 
    qdrant_stats["point_count"],
    help="Collection: 'vortex_events'\nTotal vector points currently indexed and searchable in Qdrant."
)
q2.metric(
    "Synced (Hash-Tracked)", 
    qdrant_stats["synced_hash_count"],
    help="State: 'storage/silver/.synced_hashes.json'\nNumber of unique rows whose hash has been upserted, preventing duplicate embeddings."
)

if sync_gap > 0:
    st.warning(f"⚠️ {sync_gap} Silver rows not yet synced to Qdrant — run qdrant_sync.py")
elif qdrant_stats["last_sync"]:
    st.caption(f"Last synced: {qdrant_stats['last_sync'].strftime('%Y-%m-%d %H:%M:%S')}")

st.divider()

def get_sample_queries(n: int = 5) -> list[str]:
    """Pull a few real titles from Silver data to use as guaranteed-relevant example queries."""
    if not os.path.exists(SILVER_PATH):
        return []
    try:
        df = pd.read_parquet(SILVER_PATH, columns=["title"])
        df = df.dropna().drop_duplicates()
        if df.empty:
            return []
        sample = df.sample(min(n, len(df)), random_state=None)
        return sample["title"].tolist()
    except Exception:
        return []


# --- Groundedness tester ---
st.subheader(
    "4. Groundedness Guardrail — Live Retrieval Tester",
    help="Real-time hybrid search engine. Evaluates whether retrieved knowledge is semantically grounded before passing to an LLM."
)
st.markdown(
    '<div class="section-badge">🛡️ Performs Dense + BM25 Hybrid Search & filters hallucinations using dynamic statistical thresholds</div>', 
    unsafe_allow_html=True
)

st.caption("💡 **How it works:** Query your live vector store to inspect Reciprocal Rank Fusion (RRF) scores and see if results pass the groundedness firewall.")
st.caption("Click any live sample query below (dynamically pulled from your stored dataset) or type a custom prompt:")

if "sample_queries" not in st.session_state:
    st.session_state["sample_queries"] = get_sample_queries(5)
sample_queries = st.session_state["sample_queries"]

if sample_queries:
    cols = st.columns(len(sample_queries) + 1)
    for i, sample in enumerate(sample_queries):
        label = sample[:30] + "..." if len(sample) > 30 else sample
        if cols[i].button(label, key=f"sample_{i}", help=f"Test groundedness for: '{sample}'"):
            st.session_state["query_input"] = sample
    if cols[-1].button("🔀 New examples", help="Randomly sample 5 different article titles from Silver storage."):
        st.session_state["sample_queries"] = get_sample_queries(5)
        st.rerun()

if "query_input" not in st.session_state:
    st.session_state["query_input"] = sample_queries[0] if sample_queries else "artificial intelligence"

query_text = st.text_input(
    "Enter a test query", 
    key="query_input",
    help="Input a search topic or question to test vector similarity and dynamic threshold scoring against Qdrant."
)

if st.button("Run query", help="Executes dense semantic + BM25 hybrid search and computes groundedness verdict.") and query_text.strip():
    if qdrant_stats["point_count"] == 0:
        st.error("Qdrant collection is empty — run qdrant_sync.py first.")
    else:
        with st.spinner("Running hybrid search (dense + BM25) and fusing results..."):
            results = run_groundedness_query(query_text)

        df = pd.DataFrame(results, columns=[
            "Dense", "BM25", "Fused", "Threshold", "Grounded", "Article Title", "Event / Edit Details", "Author", "Hacker News Link"
        ])

        st.caption(
            "📊 **Metric Explanations:** "
            "**Dense** = Semantic similarity (MiniLM embeddings) · "
            "**BM25** = Exact keyword overlap · "
            "**Fused** = Reciprocal Rank Fusion score · "
            "**Threshold** = Dynamic statistical cutoff (mean + 0.5×stdev) · "
            "**Grounded** = 'Yes' indicates result meets confidence threshold to prevent LLM hallucinations."
        )

        st.dataframe(
            df,
            width="stretch",
            hide_index=True,
            column_config={
                "Dense": st.column_config.TextColumn("Dense", help="Cosine similarity of dense vector embeddings", width="small"),
                "BM25": st.column_config.TextColumn("BM25", help="Lexical keyword match score", width="small"),
                "Fused": st.column_config.TextColumn("Fused", help="Reciprocal Rank Fusion score combining Dense & BM25", width="small"),
                "Threshold": st.column_config.TextColumn("Threshold", help="Dynamically computed cutoff score for this query", width="small"),
                "Grounded": st.column_config.TextColumn("Grounded", help="Passed firewall validation? (Yes = Grounded, No = Hallucination Risk)", width="small"),
                "Article Title": st.column_config.TextColumn("Article Title", width="medium"),
                "Event / Edit Details": st.column_config.TextColumn("Event / Edit Details", width="large"),
                "Author": st.column_config.TextColumn("Author", width="small"),
                "Hacker News Link": st.column_config.LinkColumn("Hacker News Link", display_text="Open →", width="small"),
            },
        )

st.divider()
st.caption("🌀 VortexAI — Real-Time Streaming Lakehouse with Validated RAG Ingestion & Guardrails")

# --- Auto-refresh loop ---
if auto_refresh:
    time.sleep(5)
    st.rerun()