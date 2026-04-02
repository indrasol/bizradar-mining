import argparse
import hashlib
import os
import csv
from datetime import datetime, timezone
from typing import Optional, Tuple

from sam_gov.utils.logger import get_logger
from sam_gov.workers.queue_pipeline.config import (
    SERVICEBUS_FQNS,
    STORAGE_ACCOUNT_URL,
    STORAGE_CONTAINER,
    QUEUE_NAMES,
)
from sam_gov.workers.queue_pipeline.contracts import make_envelope, idempotency_key
from sam_gov.config.settings import env_int, env_str


logger = get_logger(__name__)


def _count_rows_with_encoding(csv_path: str, import_limit: Optional[int]) -> Tuple[int, int, str]:
    encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]
    for enc in encodings:
        try:
            with open(csv_path, "r", encoding=enc, newline="") as f:
                reader = csv.DictReader(f)
                count = 0
                notice_count = 0
                for row in reader:
                    count += 1
                    if str((row or {}).get("NoticeId") or "").strip():
                        notice_count += 1
                    if import_limit and count >= import_limit:
                        return count, notice_count, enc
                return count, notice_count, enc
        except UnicodeDecodeError:
            continue
    raise ValueError("Could not read CSV with supported encodings")


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _upload_blob(csv_path: str, run_id: str) -> Tuple[str, str]:
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobClient

    if not STORAGE_ACCOUNT_URL:
        raise ValueError("AZURE_STORAGE_ACCOUNT_URL_BIZ is required")
    if not STORAGE_CONTAINER:
        raise ValueError("AZURE_STORAGE_CONTAINER_BIZ is required")

    filename = os.path.basename(csv_path)
    blob_name = f"runs/{run_id}/{filename}"
    blob_url = f"{STORAGE_ACCOUNT_URL.rstrip('/')}/{STORAGE_CONTAINER}/{blob_name}"
    blob = BlobClient(
        account_url=STORAGE_ACCOUNT_URL,
        container_name=STORAGE_CONTAINER,
        blob_name=blob_name,
        credential=DefaultAzureCredential(),
    )
    with open(csv_path, "rb") as f:
        blob.upload_blob(f, overwrite=True)
    return blob_name, blob_url


def main() -> None:
    from sam_gov.workers.queue_pipeline.queue_io import send_envelope

    parser = argparse.ArgumentParser(description="Upload CSV and enqueue chunk pointers to Service Bus.")
    parser.add_argument("--csv-path", required=True, type=str, help="Path to CSV file")
    parser.add_argument(
        "--chunk-size",
        required=False,
        type=int,
        default=env_int("CSV_CHUNK_SIZE_BIZ", 500, legacy_names=("CSV_CHUNK_SIZE",)),
    )
    parser.add_argument("--limit", required=False, type=int, default=None)
    args = parser.parse_args()

    csv_path = args.csv_path
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    if args.chunk_size <= 0:
        raise ValueError("chunk-size must be > 0")
    if not SERVICEBUS_FQNS:
        raise ValueError("AZURE_SERVICEBUS_FQNS_BIZ is required")

    queue_name = QUEUE_NAMES["chunks"]
    run_id = env_str("PIPELINE_RUN_ID_BIZ", "", legacy_names=("PIPELINE_RUN_ID",)) or datetime.now(
        timezone.utc
    ).strftime("%Y%m%dT%H%M%SZ")
    trace_id = idempotency_key("run-trace", run_id)[:32]

    row_count, notice_count, encoding = _count_rows_with_encoding(csv_path, args.limit)
    sha = _sha256_file(csv_path)
    blob_name, blob_url = _upload_blob(csv_path, run_id)
    logger.info(
        f"CSV uploaded: run_id={run_id} blob={blob_name} rows={row_count} "
        f"notice_rows={notice_count} encoding={encoding}"
    )

    messages_sent = 0
    for chunk_start in range(1, row_count + 1, args.chunk_size):
        chunk_end = min(row_count, chunk_start + args.chunk_size - 1)
        envelope = make_envelope(
            run_id=run_id,
            trace_id=trace_id,
            stage="chunks",
            notice_id="",
            source_file=blob_name,
            row_index=chunk_start,
            message_id=idempotency_key(run_id, chunk_start, chunk_end, sha),
            data={
                "blob_name": blob_name,
                "blob_url": blob_url,
                "csv_sha256": sha,
                "csv_encoding": encoding,
                "chunk_start": chunk_start,
                "chunk_end": chunk_end,
                "row_count": row_count,
                "notice_count": notice_count,
                "import_limit": args.limit,
            },
        )
        send_envelope(SERVICEBUS_FQNS, queue_name, envelope)
        messages_sent += 1

    logger.info(f"Enqueue complete: run_id={run_id} chunks={messages_sent} rows={row_count}")


if __name__ == "__main__":
    main()
