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
import time
from datetime import datetime

import pandas as pd
import pyarrow.parquet as pq
import streamlit as st
from confluent_kafka import Consumer, TopicPartition
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

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

st.set_page_config(page_title="VortexAI Telemetry", layout="wide")


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


def run_groundedness_query(query_text: str, top_k: int = TOP_K, threshold: float = GROUNDEDNESS_THRESHOLD):
    model = get_embedding_model()
    client = get_qdrant_client()
    query_vector = model.encode(query_text).tolist()

    results = client.query_points(
        collection_name=COLLECTION_NAME,
        query=query_vector,
        limit=top_k,
    ).points

    rows = []
    for r in results:
        title, raw_details = split_title_and_details(r.payload)
        rows.append({
            "Score": f"{r.score:.4f}",
            "Grounded": "Yes" if r.score >= threshold else "No",
            "Article Title": title or "(untitled)",
            "Event / Edit Details": clean_event_details(raw_details),
            "Author": r.payload.get("by"),
            "Hacker News Link": build_hn_url(r.payload.get("id")),
        })
    return rows


# ---------- UI ----------

st.title("VortexAI — Real-Time Pipeline Telemetry")
st.caption(f"Last refreshed: {datetime.now().strftime('%H:%M:%S')}")

col1, col2 = st.columns([1, 3])
with col1:
    auto_refresh = st.checkbox("Auto-refresh (every 5s)", value=True)
with col2:
    if st.button("Refresh now"):
        st.rerun()

st.divider()

# --- Kafka topic depths ---
st.subheader("Kafka Pipeline Flow")
topic_counts = {t: get_topic_message_count(t) for t in TOPICS}

k1, k2, k3 = st.columns(3)
k1.metric("Raw Events", topic_counts["raw-events-topic"])
k2.metric("Validated Events", topic_counts["validated-events-topic"])
k3.metric("DLQ (Rejected)", topic_counts["dlq-topic"])

processed = topic_counts["validated-events-topic"] + topic_counts["dlq-topic"]
if processed > 0:
    rejection_rate = topic_counts["dlq-topic"] / processed * 100
    st.progress(min(rejection_rate / 100, 1.0), text=f"DLQ rejection rate: {rejection_rate:.1f}% of processed events")
else:
    st.info("No events processed yet — start the pipeline scripts to see live stats.")

st.divider()

# --- Medallion storage ---
st.subheader("Medallion Storage")
bronze_stats = get_bronze_stats()
silver_stats = get_silver_stats()

b1, b2, b3 = st.columns(3)
b1.metric("Bronze Files", bronze_stats["file_count"])
b2.metric("Bronze Rows", bronze_stats["row_count"])
b3.metric("Silver Rows (cleaned)", silver_stats["row_count"])

if silver_stats["last_modified"]:
    st.caption(f"Silver last rebuilt: {silver_stats['last_modified'].strftime('%Y-%m-%d %H:%M:%S')}")
else:
    st.caption("Silver has not been generated yet — run silver_transform.py")

st.divider()

# --- Vector sync ---
st.subheader("Vector Sink (Qdrant)")
qdrant_stats = get_qdrant_stats()

q1, q2 = st.columns(2)
q1.metric("Points in Qdrant", qdrant_stats["point_count"])
q2.metric("Synced (hash-tracked)", qdrant_stats["synced_hash_count"])

if silver_stats["row_count"] > qdrant_stats["synced_hash_count"]:
    pending = silver_stats["row_count"] - qdrant_stats["synced_hash_count"]
    st.warning(f"{pending} Silver rows not yet synced to Qdrant — run qdrant_sync.py")
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
st.subheader("Groundedness Guardrail — Live Tester")
st.caption("Query your live vector store and see which results pass the cosine-similarity groundedness check.")
st.caption("Not sure what to search? Your data changes every time the pipeline runs, so here are real examples pulled from what's actually in your collection right now:")

if "sample_queries" not in st.session_state:
    st.session_state["sample_queries"] = get_sample_queries(5)
sample_queries = st.session_state["sample_queries"]

if sample_queries:
    cols = st.columns(len(sample_queries) + 1)
    for i, sample in enumerate(sample_queries):
        label = sample[:30] + "..." if len(sample) > 30 else sample
        if cols[i].button(label, key=f"sample_{i}"):
            st.session_state["query_input"] = sample
    if cols[-1].button("🔀 New examples"):
        st.session_state["sample_queries"] = get_sample_queries(5)
        st.rerun()

if "query_input" not in st.session_state:
    st.session_state["query_input"] = sample_queries[0] if sample_queries else "artificial intelligence"

query_text = st.text_input("Enter a test query", key="query_input")
if st.button("Run query") and query_text.strip():
    if qdrant_stats["point_count"] == 0:
        st.error("Qdrant collection is empty — run qdrant_sync.py first.")
    else:
        with st.spinner("Embedding query and searching Qdrant..."):
            results = run_groundedness_query(query_text)

        df = pd.DataFrame(results, columns=[
            "Score", "Grounded", "Article Title", "Event / Edit Details", "Author", "Hacker News Link"
        ])

        st.dataframe(
            df,
            width="stretch",
            hide_index=True,
            column_config={
                "Score": st.column_config.TextColumn("Score", width="small"),
                "Grounded": st.column_config.TextColumn("Grounded", width="small"),
                "Article Title": st.column_config.TextColumn("Article Title", width="medium"),
                "Event / Edit Details": st.column_config.TextColumn("Event / Edit Details", width="large"),
                "Author": st.column_config.TextColumn("Author", width="small"),
                "Hacker News Link": st.column_config.LinkColumn("Hacker News Link", display_text="Open →", width="small"),
            },
        )

st.divider()
st.caption("VortexAI — real-time streaming lakehouse with validated RAG ingestion")

# --- Auto-refresh loop ---
if auto_refresh:
    time.sleep(5)
    st.rerun()