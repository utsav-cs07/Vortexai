"""
VortexAI - Silver Layer Transform (Hacker News edition)
Reads Bronze Parquet, strips HTML from story text, builds a clean searchable
snippet per story, deduplicates, and writes to Silver Parquet.
"""

import glob
import html
import logging
import os
import re
from urllib.parse import urlparse

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("vortex-silver")

BRONZE_ROOT = "storage/bronze"
SILVER_ROOT = "storage/silver"

HTML_TAG_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")


def clean_html_text(text) -> str:
    """Strip HTML tags and unescape entities from HN's text field."""
    if text is None or (isinstance(text, float) and pd.isna(text)) or text == "":
        return ""
    text = str(text)
    text = HTML_TAG_RE.sub(" ", text)
    text = html.unescape(text)
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text


def domain_of(url) -> str:
    if url is None or (isinstance(url, float) and pd.isna(url)) or url == "":
        return ""
    try:
        return urlparse(str(url)).netloc.replace("www.", "")
    except Exception:
        return ""


def build_snippet(title, text, url) -> str:
    title = "" if (title is None or (isinstance(title, float) and pd.isna(title))) else str(title)
    cleaned_text = clean_html_text(text)
    if cleaned_text:
        return f"{title}: {cleaned_text}"
    resolved_url = domain_of(url)
    if resolved_url:
        return f"{title} (link: {resolved_url})"
    return title


def load_bronze() -> pd.DataFrame:
    files = glob.glob(os.path.join(BRONZE_ROOT, "date=*", "hour=*", "*.parquet"))
    if not files:
        logger.warning("No Bronze files found.")
        return pd.DataFrame()

    logger.info(f"Loading {len(files)} Bronze file(s)...")
    dfs = [pd.read_parquet(f) for f in files]
    return pd.concat(dfs, ignore_index=True)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["text"] = df.apply(
        lambda row: build_snippet(row.get("title", ""), row.get("text", ""), row.get("url", "")),
        axis=1,
    )

    before = len(df)
    df = df.drop_duplicates(subset=["text"], keep="last")
    df = df[df["text"].str.len() >= 5]
    after = len(df)
    logger.info(f"Deduplication: {before} -> {after} rows ({before - after} removed)")

    keep_cols = ["text", "title", "id", "by", "url", "score", "time", "_ingested_at"]
    keep_cols = [c for c in keep_cols if c in df.columns]
    return df[keep_cols].reset_index(drop=True)


def main() -> None:
    logger.info("Starting Silver transform...")
    bronze_df = load_bronze()

    if bronze_df.empty:
        logger.info("Nothing to process. Run the Bronze writer first.")
        return

    silver_df = transform(bronze_df)

    os.makedirs(SILVER_ROOT, exist_ok=True)
    output_path = os.path.join(SILVER_ROOT, "silver_events.parquet")
    silver_df.to_parquet(output_path, engine="pyarrow", index=False)

    logger.info(f"Wrote {len(silver_df)} cleaned rows -> {output_path}")
    logger.info("Sample rows:")
    for _, row in silver_df.head(3).iterrows():
        logger.info(f"  - {row['text'][:100]}")


if __name__ == "__main__":
    main()