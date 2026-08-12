"""
VortexAI - Bronze Compaction (one-time maintenance utility)
Merges many small Parquet files within each date/hour partition into a single
larger file. Run this whenever Bronze accumulates lots of tiny files
(e.g. after a backlog catch-up).
"""

import glob
import logging
import os

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("vortex-compact")

BRONZE_ROOT = "storage/bronze"


def compact_partition(partition_dir: str) -> None:
    files = glob.glob(os.path.join(partition_dir, "batch_*.parquet"))
    if len(files) <= 1:
        return  # nothing to compact

    logger.info(f"Compacting {len(files)} files in {partition_dir}...")

    dfs = [pd.read_parquet(f) for f in files]
    merged = pd.concat(dfs, ignore_index=True)

    compacted_path = os.path.join(partition_dir, "compacted.parquet")
    merged.to_parquet(compacted_path, engine="pyarrow", index=False)

    # Only delete originals after the merged file writes successfully
    for f in files:
        os.remove(f)

    logger.info(f"-> {len(merged)} total rows written to {compacted_path}")


def main() -> None:
    partition_dirs = glob.glob(os.path.join(BRONZE_ROOT, "date=*", "hour=*"))

    if not partition_dirs:
        logger.info("No Bronze partitions found. Nothing to compact.")
        return

    for partition_dir in partition_dirs:
        compact_partition(partition_dir)

    logger.info("Compaction complete.")


if __name__ == "__main__":
    main()