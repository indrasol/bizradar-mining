"""Step 3: Upload raw CSV to Azure Blob Storage.

Responsibilities:
  - Compute a SHA-256 hash of the CSV for integrity and idempotency
  - Upload the raw CSV to blob storage for auditability and replay
  - Organize files under sam_gov_opportunities/{date_time}/ for traceability
  - Return the blob reference, hash, and metadata for downstream logging

This is a safety net: if dedup or ingestion fails, the raw file
is already archived and the pipeline can re-run from step 2 without
re-scraping SAM.gov.

Logic mirrors the legacy csv_enqueue_servicebus._upload_blob function.

This script does NOT download, deduplicate, chunk, or ingest.
"""

import hashlib
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# Env var names match the legacy _BIZ suffix convention from config.py
STORAGE_ACCOUNT_URL = os.environ.get(
    "AZURE_STORAGE_ACCOUNT_URL_BIZ",
    os.environ.get("AZURE_STORAGE_ACCOUNT_URL", ""),
)
STORAGE_CONTAINER = os.environ.get(
    "AZURE_STORAGE_CONTAINER_BIZ",
    os.environ.get("AZURE_STORAGE_CONTAINER", ""),
)


# ---------------------------------------------------------------------------
# Helpers (ported from csv_enqueue_servicebus.py)
# ---------------------------------------------------------------------------

def _sha256_file(path: Path) -> str:
    """Compute SHA-256 hex digest of a file, streaming 1 MB at a time."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Main upload function
# ---------------------------------------------------------------------------

def upload_to_blob(csv_path: Path, run_id: str | None = None) -> dict:
    """Upload a CSV file to Azure Blob Storage.

    Mirrors the legacy _upload_blob logic from csv_enqueue_servicebus.py:
      - Computes SHA-256 hash before upload
      - Uses DefaultAzureCredential (managed identity in ACJ)
      - Stores under sam_gov_opportunities/{date_time}/{filename}
      - Overwrites if blob already exists (idempotent re-runs)

    Args:
        csv_path: Local path to the CSV file.
        run_id:   Unique identifier for this pipeline run. Falls back to
                  PIPELINE_RUN_ID_BIZ env var, then UTC timestamp.

    Returns:
        Dict with keys: run_id, blob_name, blob_url, csv_sha256, size_mb
    """
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobClient

    if not STORAGE_ACCOUNT_URL:
        raise RuntimeError(
            "AZURE_STORAGE_ACCOUNT_URL_BIZ (or AZURE_STORAGE_ACCOUNT_URL) must be set. "
            "This should be injected via ACJ environment variables."
        )
    if not STORAGE_CONTAINER:
        raise RuntimeError(
            "AZURE_STORAGE_CONTAINER_BIZ (or AZURE_STORAGE_CONTAINER) must be set."
        )

    # Run ID resolution: explicit arg > env var > UTC timestamp
    # Matches legacy: env_str("PIPELINE_RUN_ID_BIZ", "") with timestamp fallback
    if run_id is None:
        run_id = (
            os.environ.get("PIPELINE_RUN_ID_BIZ", "").strip()
            or os.environ.get("PIPELINE_RUN_ID", "").strip()
            or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        )

    # Compute SHA-256 for integrity verification and idempotency
    csv_sha256 = _sha256_file(csv_path)
    logger.info(f"CSV SHA-256: {csv_sha256}")

    # Readable date folder: sam_gov_opportunities/2026-04-23_000000/filename.csv
    date_tag = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    filename = csv_path.name
    blob_name = f"sam_gov_opportunities/{date_tag}/{filename}"
    blob_url = f"{STORAGE_ACCOUNT_URL.rstrip('/')}/{STORAGE_CONTAINER}/{blob_name}"

    logger.info(f"Uploading {filename} to blob: {blob_name}")

    blob = BlobClient(
        account_url=STORAGE_ACCOUNT_URL,
        container_name=STORAGE_CONTAINER,
        blob_name=blob_name,
        credential=DefaultAzureCredential(),
    )

    file_size = csv_path.stat().st_size
    with open(csv_path, "rb") as f:
        blob.upload_blob(f, overwrite=True)

    size_mb = file_size / (1024 * 1024)
    logger.info(
        f"Blob upload complete: run_id={run_id} blob={blob_name} ({size_mb:.1f} MB)"
    )

    return {
        "run_id": run_id,
        "blob_name": blob_name,
        "blob_url": blob_url,
        "csv_sha256": csv_sha256,
        "size_mb": round(size_mb, 1),
    }