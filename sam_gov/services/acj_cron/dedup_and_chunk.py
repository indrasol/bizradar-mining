"""Step 2: Deduplicate and chunk new SAM.gov opportunities.

Responsibilities:
  - Read the processed CSV (output of data_extraction.py)
  - Query Supabase for all existing notice IDs
  - Filter the dataframe to new (unseen) rows only
  - Chunk new rows into fixed-size slices
  - Serialize each chunk into the key-value text format OpenRAG expects
  - Delete the CSV after chunking to reclaim container disk space
  - Return a list of chunk payloads ready for API ingestion

Logic mirrors the legacy incremental_ingest.py filter_csv() and
chunk_filtered_csv() functions, with Supabase replacing OpenSearch.

This script does NOT download, upload to blob, or call OpenRAG's API.
"""

import logging
import math
import os
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

ROWS_PER_CHUNK = int(os.getenv("ROWS_PER_CHUNK", "500"))

# Supabase connection — _BIZ suffix matches Bicep env var naming convention,
# with non-suffixed fallback for local development
SUPABASE_URL = os.environ.get(
    "SUPABASE_URL_BIZ",
    os.environ.get("SUPABASE_URL", ""),
)
SUPABASE_SERVICE_KEY = os.environ.get(
    "SUPABASE_SERVICE_KEY_BIZ",
    os.environ.get("SUPABASE_SERVICE_KEY", ""),
)
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "documents")
SUPABASE_NOTICE_ID_COLUMN = os.getenv("SUPABASE_NOTICE_ID_COLUMN", "notice_id")

# Encoding fallback chain (matches data_extraction.py)
ENCODINGS = ("utf-8", "latin-1", "cp1252", "iso-8859-1")

# Supabase PostgREST pagination limit
_PAGE_SIZE = 10_000


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ChunkPayload:
    """A single chunk ready to be POSTed to OpenRAG."""

    chunk_index: int
    row_start: int
    row_end: int
    row_count: int
    content: str
    notice_ids: list[str] = field(default_factory=list)


@dataclass
class DeduplicationResult:
    """Summary of the dedup + chunk step."""

    total_rows: int
    active_rows: int
    duplicate_rows: int
    new_rows: int
    num_chunks: int
    chunks: list[ChunkPayload] = field(default_factory=list)


# ---------------------------------------------------------------------------
# CSV reading
# ---------------------------------------------------------------------------

def _read_csv(path: Path) -> pd.DataFrame:
    """Read CSV with encoding fallback chain."""
    for enc in ENCODINGS:
        try:
            return pd.read_csv(path, encoding=enc, low_memory=False, on_bad_lines="skip")
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not decode {path} with any supported encoding")


# ---------------------------------------------------------------------------
# Deduplication: fetch existing notice IDs from Supabase
# ---------------------------------------------------------------------------

def _fetch_existing_notice_ids() -> set[str]:
    """Fetch all notice IDs already ingested, directly from Supabase.

    Uses the PostgREST API to paginate through the documents table,
    selecting only the notice_id column. Typically completes in a few
    seconds even for 80k+ rows since we're fetching a single text column.

    Requires SUPABASE_URL_BIZ and SUPABASE_SERVICE_KEY_BIZ environment variables.
    """
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise RuntimeError(
            "SUPABASE_URL_BIZ and SUPABASE_SERVICE_KEY_BIZ must be set for deduplication. "
            "These should be injected via ACJ environment variables."
        )

    base_url = (
        f"{SUPABASE_URL.rstrip('/')}/rest/v1/{SUPABASE_TABLE}"
        f"?select={SUPABASE_NOTICE_ID_COLUMN}"
    )
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    }

    logger.info(
        f"Fetching existing notice IDs from Supabase "
        f"(table: {SUPABASE_TABLE}, column: {SUPABASE_NOTICE_ID_COLUMN})"
    )

    all_ids: set[str] = set()
    offset = 0

    while True:
        url = f"{base_url}&offset={offset}&limit={_PAGE_SIZE}"

        try:
            resp = requests.get(url, headers=headers, timeout=120)
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise RuntimeError(
                f"Supabase query failed at offset {offset}: {exc}"
            ) from exc

        rows = resp.json()
        if not rows:
            break

        for row in rows:
            val = row.get(SUPABASE_NOTICE_ID_COLUMN)
            if val is not None and str(val).strip():
                all_ids.add(str(val).strip())

        # If we got fewer rows than page size, we've reached the end
        if len(rows) < _PAGE_SIZE:
            break

        offset += _PAGE_SIZE

    logger.info(f"  {len(all_ids)} notice IDs already indexed")
    return all_ids


# ---------------------------------------------------------------------------
# Row serialization
# ---------------------------------------------------------------------------

def _row_to_text(columns: list[str], row) -> str:
    """Format a single DataFrame row as key-value text for embedding.

    Output format (one line per non-empty field):
        NoticeId: ABC123
        Title: Road Construction Project
        ...
    """
    parts = []
    for col in columns:
        val = row[col]
        if pd.notna(val) and str(val).strip():
            parts.append(f"{col}: {val}")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def dedup_and_chunk(csv_path: Path, run_tag: str = "") -> DeduplicationResult:
    """Deduplicate the CSV against Supabase records and chunk new rows.

    Mirrors the legacy incremental_ingest.py flow:
      filter_csv()          -> dedup against known IDs
      chunk_filtered_csv()  -> serialize to text chunks

    Args:
        csv_path: Path to the processed CSV from data_extraction.py
                  (already filtered to active rows, reversed).
        run_tag:  Prefix for chunk identification (e.g. 'inc_20260421_0000').
                  Used in logging; chunks are held in memory, not written to disk.

    Returns:
        DeduplicationResult containing chunk payloads ready for ingestion.
        If there are zero new rows, chunks will be an empty list.
        The CSV is deleted after chunking to reclaim disk space.
    """
    # -- Read and count --
    logger.info("Filtering CSV...")
    df = _read_csv(csv_path)
    total = len(df)

    # Active filter (data_extraction.py already does this, but guard against
    # cases where the CSV came from blob replay without pre-filtering)
    if "Active" in df.columns:
        df = df[df["Active"].astype(str).str.strip().str.lower() == "yes"]
    active = len(df)

    logger.info(f"  {total} total rows, {active} active")

    # -- Validate NoticeId column --
    if "NoticeId" not in df.columns:
        raise ValueError(
            f"CSV is missing 'NoticeId' column. Found columns: {list(df.columns)[:10]}"
        )

    # -- Dedup against Supabase --
    existing_ids = _fetch_existing_notice_ids()

    df["_nid"] = df["NoticeId"].astype(str).str.strip()
    df = df[~df["_nid"].isin(existing_ids)]
    # Keep cleaned notice IDs for chunk metadata
    notice_id_series = df["_nid"].reset_index(drop=True)
    df = df.drop(columns=["_nid"]).reset_index(drop=True)

    new = len(df)
    already_indexed = active - new
    logger.info(f"  {new} new notices to ingest (filtered {already_indexed} already indexed)")

    if new == 0:
        csv_path.unlink(missing_ok=True)
        logger.info("  Deleted CSV, nothing to chunk")
        return DeduplicationResult(
            total_rows=total,
            active_rows=active,
            duplicate_rows=already_indexed,
            new_rows=0,
            num_chunks=0,
            chunks=[],
        )

    # -- Chunk new rows --
    logger.info("Chunking new notices...")
    columns = list(df.columns)
    num_chunks = math.ceil(new / ROWS_PER_CHUNK)
    prefix = f"{run_tag}_" if run_tag else ""
    logger.info(f"  {new} rows -> {num_chunks} chunks (prefix: {prefix})")

    chunks: list[ChunkPayload] = []
    for i in range(num_chunks):
        start = i * ROWS_PER_CHUNK
        end = min(start + ROWS_PER_CHUNK, new)
        chunk_df = df.iloc[start:end]

        # Serialize rows to text
        records = [_row_to_text(columns, row) for _, row in chunk_df.iterrows()]
        content = "\n\n".join(records)

        # Collect notice IDs for this chunk (useful for logging and retries)
        chunk_nids = notice_id_series.iloc[start:end].tolist()

        chunks.append(
            ChunkPayload(
                chunk_index=i + 1,
                row_start=start,
                row_end=end,
                row_count=end - start,
                content=content,
                notice_ids=chunk_nids,
            )
        )

    # Clean up CSV to reclaim disk space inside the container
    csv_path.unlink(missing_ok=True)
    logger.info(f"  Created {num_chunks} chunks, deleted CSV")

    return DeduplicationResult(
        total_rows=total,
        active_rows=active,
        duplicate_rows=already_indexed,
        new_rows=new,
        num_chunks=num_chunks,
        chunks=chunks,
    )