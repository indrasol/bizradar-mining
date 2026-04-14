from typing import List, Optional

from sam_gov.services.cron.csv_import_sam_gov import insert_data
from sam_gov.utils.logger import get_logger
from .config import SERVICEBUS_FQNS, QUEUE_NAMES, PERSIST_UPSERT_BATCH_SIZE
from .contracts import QueueEnvelope, make_envelope, idempotency_key
from .queue_io import run_worker_loop


logger = get_logger(__name__)


def _handle_embedded_batch(envelopes: List[QueueEnvelope]) -> List[Optional[QueueEnvelope]]:
    """
    Persist all N rows in a single insert_data() call.

    Batching reduces the number of Supabase upsert round-trips from N to 1,
    and collapses the is_latest_in_thread refresh pass across all touched threads
    into a single post-upsert sweep instead of N separate sweeps.

    insert_data() accumulates seen_rows internally to handle in-batch same-thread
    grouping — rows sharing the same thread_key are assigned the same thread_id
    via the seen_rows accumulator even before any of them are persisted to the DB.
    """
    rows = []
    run_metas = []
    valid_indices = []

    for idx, envelope in enumerate(envelopes):
        payload = envelope.data if envelope.data is not None else {}
        row = payload.get("row")
        run_meta = payload.get("run_meta") if isinstance(payload.get("run_meta"), dict) else {}
        run_metas.append(run_meta)
        if not isinstance(row, dict):
            logger.error(f"persist: missing 'row' dict for row_index={envelope.row_index}")
            rows.append(None)
        else:
            rows.append(dict(row))
            valid_indices.append(idx)

    valid_rows = [rows[i] for i in valid_indices]

    if valid_rows:
        try:
            insert_data(valid_rows)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"persist: insert_data failed for batch of {len(valid_rows)}: {exc}")
            # Return None for all — messages will be abandoned for retry
            return [None] * len(envelopes)

    results: List[Optional[QueueEnvelope]] = []
    for idx, (envelope, row, run_meta) in enumerate(zip(envelopes, rows, run_metas)):
        if row is None:
            results.append(None)
            continue
        notice_id_val = row.get("notice_id")
        notice_id = str(notice_id_val).strip() if notice_id_val is not None else ""
        results.append(make_envelope(
            run_id=envelope.run_id,
            trace_id=envelope.trace_id,
            stage="persist_results",
            notice_id=notice_id,
            source_file=envelope.source_file,
            row_index=envelope.row_index,
            message_id=idempotency_key(envelope.run_id, "persisted", envelope.row_index, notice_id),
            data={"persist_result": "ok", "row": {"notice_id": notice_id}, "run_meta": run_meta},
            attempt=envelope.attempt,
        ))

    return results


def run() -> None:
    run_worker_loop(
        servicebus_fqns=SERVICEBUS_FQNS,
        input_queue=QUEUE_NAMES["embedded"],
        output_queue=QUEUE_NAMES["results"],
        worker_name="persist",
        batch_handler=_handle_embedded_batch,
        max_message_count=PERSIST_UPSERT_BATCH_SIZE,
    )
