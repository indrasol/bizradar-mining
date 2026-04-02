from typing import Optional

from sam_gov.services.cron.csv_import_sam_gov import process_csv_row
from sam_gov.utils.logger import get_logger
from .config import SERVICEBUS_FQNS, QUEUE_NAMES
from .contracts import QueueEnvelope, make_envelope, idempotency_key
from .queue_io import run_worker_loop


logger = get_logger(__name__)


def _handle_raw(envelope: QueueEnvelope) -> Optional[QueueEnvelope]:
    payload = envelope.data or {}
    row = payload.get("row")
    run_meta = payload.get("run_meta") if isinstance(payload.get("run_meta"), dict) else {}
    if not isinstance(row, dict):
        raise ValueError("raw row payload missing 'row' dict")

    processed = process_csv_row(row)
    if not processed:
        logger.warning(f"normalize_dedupe skipped row_index={envelope.row_index}: missing notice_id")
        return None

    notice_id = str(processed.get("notice_id") or "")
    return make_envelope(
        run_id=envelope.run_id,
        trace_id=envelope.trace_id,
        stage="normalized_rows",
        notice_id=notice_id,
        source_file=envelope.source_file,
        row_index=envelope.row_index,
        message_id=idempotency_key(envelope.run_id, "normalized", envelope.row_index, notice_id),
        data={"row": processed, "run_meta": run_meta},
        attempt=envelope.attempt,
    )


def run() -> None:
    run_worker_loop(
        servicebus_fqns=SERVICEBUS_FQNS,
        input_queue=QUEUE_NAMES["raw"],
        output_queue=QUEUE_NAMES["normalized"],
        worker_name="normalize_dedupe",
        handler=_handle_raw,
    )
