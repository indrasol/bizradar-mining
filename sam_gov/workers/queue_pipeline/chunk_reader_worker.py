import csv
import io
from typing import Optional

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient

from sam_gov.utils.logger import get_logger
from .config import SERVICEBUS_FQNS, STORAGE_ACCOUNT_URL, STORAGE_CONTAINER, QUEUE_NAMES
from .contracts import QueueEnvelope, make_envelope, idempotency_key
from .queue_io import run_worker_loop, send_envelope


logger = get_logger(__name__)


def _download_blob_text(blob_name: str, encoding: str) -> str:
    blob = BlobClient(
        account_url=STORAGE_ACCOUNT_URL,
        container_name=STORAGE_CONTAINER,
        blob_name=blob_name,
        credential=DefaultAzureCredential(),
    )
    raw = blob.download_blob().readall()
    return raw.decode(encoding, errors="replace")


def _handle_chunk(envelope: QueueEnvelope) -> Optional[QueueEnvelope]:
    data = envelope.data or {}
    blob_name = data.get("blob_name")
    encoding = str(data.get("csv_encoding") or "utf-8")
    start = int(data.get("chunk_start") or 1)
    end = int(data.get("chunk_end") or start)
    run_meta = {
        "row_count": int(data.get("row_count") or 0),
        "notice_count": int(data.get("notice_count") or 0),
        "import_limit": data.get("import_limit"),
    }
    if not blob_name:
        raise ValueError("chunk message missing blob_name")

    csv_text = _download_blob_text(blob_name, encoding)
    reader = csv.DictReader(io.StringIO(csv_text))
    emitted = 0
    for row_index, row in enumerate(reader, start=1):
        if row_index < start:
            continue
        if row_index > end:
            break
        notice_id = str(row.get("NoticeId") or "").strip()
        raw_env = make_envelope(
            run_id=envelope.run_id,
            trace_id=envelope.trace_id,
            stage="raw_rows",
            notice_id=notice_id,
            source_file=envelope.source_file,
            row_index=row_index,
            message_id=idempotency_key(envelope.run_id, "raw", row_index, notice_id),
            data={"row": row, "run_meta": run_meta},
            attempt=envelope.attempt,
        )
        send_envelope(SERVICEBUS_FQNS, QUEUE_NAMES["raw"], raw_env)
        emitted += 1

    logger.info(f"chunk_reader emitted={emitted} range={start}-{end} blob={blob_name}")
    return None


def run() -> None:
    run_worker_loop(
        servicebus_fqns=SERVICEBUS_FQNS,
        input_queue=QUEUE_NAMES["chunks"],
        output_queue=None,
        worker_name="chunk_reader",
        handler=_handle_chunk,
    )
