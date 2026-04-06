import csv
import io
from typing import Optional

from azure.identity import DefaultAzureCredential
from azure.servicebus import ServiceBusClient
from azure.storage.blob import BlobClient

from sam_gov.utils.logger import get_logger
from .config import SERVICEBUS_FQNS, STORAGE_ACCOUNT_URL, STORAGE_CONTAINER, QUEUE_NAMES
from .contracts import QueueEnvelope, make_envelope, idempotency_key
from .queue_io import run_worker_loop, send_envelope


logger = get_logger(__name__)


def _stream_blob_lines(blob_name: str, encoding: str):
    """
    Streams a blob from Azure Storage line-by-line to avoid loading the whole file into memory.
    """
    blob = BlobClient(
        account_url=STORAGE_ACCOUNT_URL,
        container_name=STORAGE_CONTAINER,
        blob_name=blob_name,
        credential=DefaultAzureCredential(),
    )

    downloader = blob.download_blob()
    pending = ""

    for chunk in downloader.chunks():
        decoded = chunk.decode(encoding, errors="replace")
        data = pending + decoded
        lines = data.splitlines(keepends=True)

        if lines and not data.endswith(('\r', '\n')):
            pending = lines.pop()
        else:
            pending = ""

        yield from lines

    if pending:
        yield pending


def _handle_chunk(envelope: QueueEnvelope) -> Optional[QueueEnvelope]:
    data = envelope.data if envelope.data is not None else {}
    blob_name = data.get("blob_name")
    encoding = str(data.get("csv_encoding") if data.get("csv_encoding") is not None else "utf-8")
    start = int(data.get("chunk_start") if data.get("chunk_start") is not None else 1)
    end = int(data.get("chunk_end") if data.get("chunk_end") is not None else start)
    run_meta = {
        "row_count": int(data.get("row_count") if data.get("row_count") is not None else 0),
        "notice_count": int(data.get("notice_count") if data.get("notice_count") is not None else 0),
        "import_limit": data.get("import_limit"),
    }
    if blob_name is None or not str(blob_name).strip():
        raise ValueError("chunk message missing blob_name")

    # Use the new streaming line generator instead of downloading everything at once
    line_iter = _stream_blob_lines(blob_name, encoding)
    reader = csv.DictReader(line_iter)
    
    emitted = 0
    credential = DefaultAzureCredential()
    with ServiceBusClient(fully_qualified_namespace=SERVICEBUS_FQNS, credential=credential) as client:
        for row_index, row in enumerate(reader, start=1):
            if row_index < start:
                continue
            if row_index > end:
                break
            nid_val = row.get("NoticeId")
            notice_id = str(nid_val).strip() if nid_val is not None else ""
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
            send_envelope(SERVICEBUS_FQNS, QUEUE_NAMES["raw"], raw_env, client=client)
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
