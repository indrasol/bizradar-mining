"""SAM.gov Ingestion Pipeline — Orchestrator.

Runs inside Azure Container App Job (caj-sam-gov-scheduler).
Triggered on a cron schedule or manually via az CLI.

Pipeline steps:
  1. data_extraction.py   — Download CSV from SAM.gov via Playwright
  2. blob_upload.py       — Archive raw CSV to Azure Blob Storage
  3. dedup_and_chunk.py   — Deduplicate against Supabase, chunk new rows
  4. ingest.py            — POST chunks to OpenRAG API in parallel

Mirrors the legacy incremental_ingest.py orchestration flow, replacing
OpenSearch docker exec with Supabase queries and OpenRAG HTTP API calls.

Usage:
    python -m sam_gov.services.acj_cron.main
"""

import logging
import sys
import time
from datetime import datetime

from .data_extraction import download_csv
from .blob_upload import upload_to_blob
from .dedup_and_chunk import dedup_and_chunk
from .ingest_openrag import ingest_chunks

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _configure_logging() -> None:
    """Configure root logger for structured ACJ output."""
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(name)s — %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    # Quiet noisy libraries
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def main() -> None:
    _configure_logging()

    logger.info("=" * 60)
    logger.info("SAM.GOV INGESTION PIPELINE")
    logger.info("=" * 60)
    start = time.time()

    # ------------------------------------------------------------------
    # Step 1: Download CSV from SAM.gov
    # ------------------------------------------------------------------
    logger.info("Step 1/4 — Downloading CSV from SAM.gov...")
    try:
        csv_path = download_csv()
    except Exception as exc:
        logger.error(f"Step 1 FAILED — data extraction: {exc}")
        sys.exit(1)

    size_mb = csv_path.stat().st_size / (1024 * 1024)
    logger.info(f"  Downloaded: {csv_path.name} ({size_mb:.1f} MB)")

    # ------------------------------------------------------------------
    # Step 2: Archive raw CSV to blob storage
    # ------------------------------------------------------------------
    logger.info("Step 2/4 — Uploading raw CSV to blob storage...")
    run_tag = f"inc_{datetime.now().strftime('%Y%m%d_%H%M')}"
    try:
        blob_info = upload_to_blob(csv_path, run_id=run_tag)
    except Exception as exc:
        logger.error(f"Step 2 FAILED — blob upload: {exc}")
        logger.warning("CSV is still on local disk; continuing with dedup...")
        blob_info = None

    if blob_info:
        logger.info(
            f"  Archived: {blob_info['blob_name']} "
            f"(SHA-256: {blob_info['csv_sha256'][:16]}...)"
        )

    # ------------------------------------------------------------------
    # Step 3: Deduplicate against Supabase and chunk new rows
    # ------------------------------------------------------------------
    logger.info("Step 3/4 — Deduplicating and chunking...")
    try:
        result = dedup_and_chunk(csv_path, run_tag=run_tag)
    except Exception as exc:
        logger.error(f"Step 3 FAILED — dedup/chunk: {exc}")
        sys.exit(1)

    if result.new_rows == 0:
        elapsed = time.time() - start
        logger.info("No new notices — everything is up to date.")
        logger.info(f"Total time: {elapsed / 60:.1f} min")
        logger.info("=" * 60)
        return

    # ------------------------------------------------------------------
    # Step 4: Ingest chunks into OpenRAG
    # ------------------------------------------------------------------
    logger.info("Step 4/4 — Ingesting into OpenRAG...")
    try:
        summary = ingest_chunks(result.chunks, run_tag=run_tag)
    except Exception as exc:
        logger.error(f"Step 4 FAILED — ingestion: {exc}")
        sys.exit(1)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    elapsed = time.time() - start

    logger.info("-" * 60)
    logger.info("PIPELINE SUMMARY")
    logger.info(f"  Total rows in CSV:    {result.total_rows}")
    logger.info(f"  Active rows:          {result.active_rows}")
    logger.info(f"  Already indexed:      {result.duplicate_rows}")
    logger.info(f"  New rows ingested:    {result.new_rows}")
    logger.info(f"  Chunks sent:          {summary.total_chunks}")
    logger.info(f"  Chunks succeeded:     {summary.succeeded}")
    logger.info(f"  Chunks failed:        {summary.failed}")
    if blob_info:
        logger.info(f"  Blob archive:         {blob_info['blob_name']}")
    logger.info(f"  Total time:           {elapsed / 60:.1f} min")
    logger.info("-" * 60)

    if summary.failed > 0:
        # Log which chunks failed for debugging
        for r in summary.results:
            if not r.success:
                logger.error(
                    f"  FAILED chunk {r.chunk_index}: {r.error} "
                    f"(notice_ids: {r.notice_ids[:3]}{'...' if len(r.notice_ids) > 3 else ''})"
                )
        logger.info("DONE WITH GAPS — check failed chunks above")
    else:
        logger.info("DONE — all new notices ingested")

    logger.info("=" * 60)

    # Exit with non-zero if any chunks failed, so ACJ marks the job as failed
    # and the replicaRetryLimit kicks in
    if summary.failed > 0:
        sys.exit(1)


# ---------------------------------------------------------------------------
# __main__ support for `python -m sam_gov.services.acj_cron.main`
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()