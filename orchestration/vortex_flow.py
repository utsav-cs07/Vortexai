"""
VortexAI - Orchestration
Wraps silver_transform.py and qdrant_sync.py as a Prefect flow with automatic
retries, logging, and optional scheduling.

Run once immediately:
    python orchestration/vortex_flow.py

Run on a repeating schedule (every 5 minutes, keeps running until Ctrl+C):
    python orchestration/vortex_flow.py serve
"""

import importlib.util
import os
import sys

from prefect import flow, get_run_logger, task

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_module(name: str, relative_path: str):
    """Import a standalone script (not a package) as a module by file path."""
    path = os.path.join(PROJECT_ROOT, relative_path)
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@task(name="silver-transform", retries=2, retry_delay_seconds=30, log_prints=True)
def run_silver_transform() -> None:
    logger = get_run_logger()
    logger.info("Starting Silver transform task...")
    silver_module = load_module("silver_transform_module", "storage/silver_transform.py")
    silver_module.main()
    logger.info("Silver transform task complete.")


@task(name="qdrant-sync", retries=2, retry_delay_seconds=30, log_prints=True)
def run_qdrant_sync() -> None:
    logger = get_run_logger()
    logger.info("Starting Qdrant sync task...")
    qdrant_module = load_module("qdrant_sync_module", "vector_sink/qdrant_sync.py")
    qdrant_module.main()
    logger.info("Qdrant sync task complete.")


@flow(name="vortexai-silver-to-qdrant-refresh", log_prints=True)
def vortex_refresh_flow() -> None:
    """End-to-end refresh: rebuild Silver from Bronze, then sync new rows to Qdrant."""
    run_silver_transform()
    run_qdrant_sync()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "serve":
        print("Starting scheduled Prefect flow — runs every 5 minutes. Press Ctrl+C to stop.")
        vortex_refresh_flow.serve(
            name="vortexai-refresh-every-5min",
            interval=300,
        )
    else:
        vortex_refresh_flow()