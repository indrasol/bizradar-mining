"""Step 4: Ingest chunks into OpenRAG via its public API.

Responsibilities:
  - Take chunk payloads from dedup_and_chunk.py
  - Write each chunk to a temp .txt file
  - POST each file to OpenRAG's /v1/documents/ingest endpoint (multipart)
  - Poll /v1/tasks/{task_id} until completion or failure
  - Run chunks in parallel with a configurable concurrency limit
  - Return a summary of successes, failures, and task IDs

This script does NOT download, deduplicate, chunk, or upload to blob.
"""

import asyncio
import io
import logging
import os
import time
from dataclasses import dataclass, field

import aiohttp

from .dedup_and_chunk import ChunkPayload

logger = logging.getLogger(__name__)

OPENRAG_BASE_URL = os.getenv("OPENRAG_BASE_URL", "http://localhost:8000")
OPENRAG_API_KEY = os.environ.get("OPENRAG_API_KEY", "")
MAX_CONCURRENCY = int(os.getenv("INGEST_MAX_CONCURRENCY", "5"))
TASK_POLL_INTERVAL = int(os.getenv("TASK_POLL_INTERVAL_SECONDS", "10"))
TASK_POLL_TIMEOUT = int(os.getenv("TASK_POLL_TIMEOUT_SECONDS", "600"))
MAX_RETRIES = int(os.getenv("INGEST_MAX_RETRIES", "3"))
RETRY_BACKOFF_BASE = 2  # exponential backoff: 2^attempt seconds


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ChunkResult:
    """Outcome of ingesting a single chunk."""

    chunk_index: int
    success: bool
    task_id: str | None = None
    task_status: str | None = None
    error: str | None = None
    notice_ids: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0


@dataclass
class IngestionSummary:
    """Summary of the full ingestion run."""

    total_chunks: int
    succeeded: int
    failed: int
    duration_seconds: float
    results: list[ChunkResult] = field(default_factory=list)


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _auth_headers() -> dict:
    """Build authentication headers for OpenRAG's public API."""
    if not OPENRAG_API_KEY:
        raise RuntimeError(
            "OPENRAG_API_KEY must be set. "
            "This should be injected via ACJ environment variables."
        )
    return {"X-API-Key": OPENRAG_API_KEY}


async def _upload_chunk(
    session: aiohttp.ClientSession,
    chunk: ChunkPayload,
    run_tag: str,
) -> str:
    """Upload a single chunk as a .txt file to OpenRAG. Returns task_id."""
    url = f"{OPENRAG_BASE_URL}/v1/documents/ingest"
    filename = f"{run_tag}_chunk_{chunk.chunk_index:04d}.txt"

    # Build multipart form data with the chunk content as a .txt file
    form = aiohttp.FormData()
    form.add_field(
        "file",
        io.BytesIO(chunk.content.encode("utf-8")),
        filename=filename,
        content_type="text/plain",
    )

    async with session.post(url, data=form) as resp:
        if resp.status not in (200, 201, 202):
            body = await resp.text()
            raise RuntimeError(
                f"Upload failed for chunk {chunk.chunk_index}: "
                f"HTTP {resp.status} - {body[:500]}"
            )
        data = await resp.json()

    task_id = data.get("task_id")
    if not task_id:
        raise RuntimeError(
            f"No task_id in response for chunk {chunk.chunk_index}: {data}"
        )

    logger.debug(f"Chunk {chunk.chunk_index} uploaded, task_id={task_id}")
    return task_id


async def _poll_task(session: aiohttp.ClientSession, task_id: str) -> str:
    """Poll a task until it reaches a terminal state. Returns final status."""
    url = f"{OPENRAG_BASE_URL}/v1/tasks/{task_id}"
    deadline = time.monotonic() + TASK_POLL_TIMEOUT

    while time.monotonic() < deadline:
        async with session.get(url) as resp:
            resp.raise_for_status()
            data = await resp.json()

        status = data.get("status", "unknown")
        if status in ("completed", "failed", "cancelled"):
            return status

        await asyncio.sleep(TASK_POLL_INTERVAL)

    raise TimeoutError(
        f"Task {task_id} did not complete within {TASK_POLL_TIMEOUT}s"
    )


# ---------------------------------------------------------------------------
# Per-chunk pipeline (upload + poll + retry)
# ---------------------------------------------------------------------------

async def _ingest_one_chunk(
    semaphore: asyncio.Semaphore,
    session: aiohttp.ClientSession,
    chunk: ChunkPayload,
    run_tag: str,
) -> ChunkResult:
    """Ingest a single chunk with retries, respecting the concurrency limit."""
    async with semaphore:
        start = time.monotonic()
        last_error = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                task_id = await _upload_chunk(session, chunk, run_tag)
                status = await _poll_task(session, task_id)

                duration = time.monotonic() - start

                if status == "completed":
                    logger.info(
                        f"Chunk {chunk.chunk_index} ingested "
                        f"({chunk.row_count} rows, {duration:.1f}s)"
                    )
                    return ChunkResult(
                        chunk_index=chunk.chunk_index,
                        success=True,
                        task_id=task_id,
                        task_status=status,
                        notice_ids=chunk.notice_ids,
                        duration_seconds=round(duration, 1),
                    )

                # Task finished but not successfully
                last_error = f"Task {task_id} ended with status: {status}"
                logger.warning(
                    f"Chunk {chunk.chunk_index} attempt {attempt} "
                    f"failed: {last_error}"
                )

            except Exception as exc:
                last_error = str(exc)
                logger.warning(
                    f"Chunk {chunk.chunk_index} attempt {attempt} "
                    f"error: {last_error}"
                )

            # Exponential backoff before retry
            if attempt < MAX_RETRIES:
                backoff = RETRY_BACKOFF_BASE ** attempt
                logger.debug(f"Retrying chunk {chunk.chunk_index} in {backoff}s")
                await asyncio.sleep(backoff)

        # All retries exhausted
        duration = time.monotonic() - start
        logger.error(
            f"Chunk {chunk.chunk_index} failed after {MAX_RETRIES} attempts: "
            f"{last_error}"
        )
        return ChunkResult(
            chunk_index=chunk.chunk_index,
            success=False,
            error=last_error,
            notice_ids=chunk.notice_ids,
            duration_seconds=round(duration, 1),
        )


# ---------------------------------------------------------------------------
# Main ingestion entry point
# ---------------------------------------------------------------------------

async def _run_ingestion(
    chunks: list[ChunkPayload], run_tag: str
) -> IngestionSummary:
    """Ingest all chunks in parallel with bounded concurrency."""
    start = time.monotonic()
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    headers = _auth_headers()
    timeout = aiohttp.ClientTimeout(total=TASK_POLL_TIMEOUT + 120)

    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        tasks = [
            _ingest_one_chunk(semaphore, session, chunk, run_tag)
            for chunk in chunks
        ]
        results = await asyncio.gather(*tasks)

    duration = time.monotonic() - start
    succeeded = sum(1 for r in results if r.success)
    failed = sum(1 for r in results if not r.success)

    logger.info(
        f"Ingestion complete: {succeeded}/{len(results)} chunks succeeded, "
        f"{failed} failed, total time {duration:.1f}s"
    )

    return IngestionSummary(
        total_chunks=len(results),
        succeeded=succeeded,
        failed=failed,
        duration_seconds=round(duration, 1),
        results=list(results),
    )


def ingest_chunks(chunks: list[ChunkPayload], run_tag: str) -> IngestionSummary:
    """Synchronous wrapper for the async ingestion pipeline.

    Args:
        chunks:  List of ChunkPayload objects from dedup_and_chunk.py
        run_tag: Unique prefix for this run (used in filenames sent to OpenRAG)

    Returns:
        IngestionSummary with per-chunk results.
    """
    if not chunks:
        logger.info("No chunks to ingest")
        return IngestionSummary(
            total_chunks=0, succeeded=0, failed=0, duration_seconds=0.0
        )

    logger.info(
        f"Starting ingestion of {len(chunks)} chunks "
        f"(concurrency={MAX_CONCURRENCY}, run_tag={run_tag})"
    )
    return asyncio.run(_run_ingestion(chunks, run_tag))