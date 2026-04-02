from typing import Optional

from sam_gov.services.cron.csv_import_sam_gov import insert_data
from .config import SERVICEBUS_FQNS, QUEUE_NAMES
from .contracts import QueueEnvelope, make_envelope, idempotency_key
from .queue_io import run_worker_loop


def _handle_embedded(envelope: QueueEnvelope) -> Optional[QueueEnvelope]:
    payload = envelope.data or {}
    row = payload.get("row")
    run_meta = payload.get("run_meta") if isinstance(payload.get("run_meta"), dict) else {}
    if not isinstance(row, dict):
        raise ValueError("embedded payload missing 'row' dict")

    result = insert_data([row])
    notice_id = str(row.get("notice_id") or "")
    return make_envelope(
        run_id=envelope.run_id,
        trace_id=envelope.trace_id,
        stage="persist_results",
        notice_id=notice_id,
        source_file=envelope.source_file,
        row_index=envelope.row_index,
        message_id=idempotency_key(envelope.run_id, "persisted", envelope.row_index, notice_id),
        data={"persist_result": result, "row": {"notice_id": notice_id}, "run_meta": run_meta},
        attempt=envelope.attempt,
    )


def run() -> None:
    run_worker_loop(
        servicebus_fqns=SERVICEBUS_FQNS,
        input_queue=QUEUE_NAMES["embedded"],
        output_queue=QUEUE_NAMES["results"],
        worker_name="persist",
        handler=_handle_embedded,
    )
