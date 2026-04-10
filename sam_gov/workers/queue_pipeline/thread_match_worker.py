from typing import Dict, List, Optional

from sam_gov.services.cron.csv_import_sam_gov import _resolve_thread_assignment, _ensure_thread_id, _coerce_row_for_supabase
from sam_gov.utils.db_utils import get_supabase_connection
from .config import SERVICEBUS_FQNS, QUEUE_NAMES
from .contracts import QueueEnvelope, make_envelope, idempotency_key
from .queue_io import run_worker_loop


_THREAD_CACHE: Dict[str, str] = {}
_SUPABASE = get_supabase_connection(use_service_key=True)


def _process_one(envelope: QueueEnvelope) -> Optional[QueueEnvelope]:
    payload = envelope.data if envelope.data is not None else {}
    row = payload.get("row")
    run_meta = payload.get("run_meta") if isinstance(payload.get("run_meta"), dict) else {}
    if not isinstance(row, dict):
        raise ValueError("normalized payload missing 'row' dict")

    assignment = _resolve_thread_assignment(_SUPABASE, row)
    thread_id = _ensure_thread_id(_SUPABASE, row, assignment, thread_id_cache=_THREAD_CACHE)
    row["thread_id"] = thread_id
    row["match_method"] = assignment.get("match_method", "new_thread")
    row["match_score"] = assignment.get("match_score")
    row["matched_to_version_id"] = assignment.get("matched_to_version_id")
    row["decision_reason"] = assignment.get("decision_reason")
    row["is_latest_in_thread"] = False
    row = _coerce_row_for_supabase(row)

    notice_id_val = row.get("notice_id")
    notice_id = str(notice_id_val).strip() if notice_id_val is not None else ""
    return make_envelope(
        run_id=envelope.run_id,
        trace_id=envelope.trace_id,
        stage="threaded_rows",
        notice_id=notice_id,
        source_file=envelope.source_file,
        row_index=envelope.row_index,
        message_id=idempotency_key(envelope.run_id, "threaded", envelope.row_index, notice_id),
        data={"row": row, "run_meta": run_meta},
        attempt=envelope.attempt,
    )


def _handle_normalized_batch(envelopes: List[QueueEnvelope]) -> List[Optional[QueueEnvelope]]:
    """
    Process envelopes sequentially to preserve _THREAD_CACHE ordering.

    Sequential processing is critical: when two versions of the same opportunity
    arrive in the same batch, the first row writes its thread_id to _THREAD_CACHE
    so the second row gets a cache hit and is assigned the same thread — preventing
    duplicate threads. Concurrent processing would cause a race on the cache write.
    """
    results: List[Optional[QueueEnvelope]] = []
    for envelope in envelopes:
        try:
            results.append(_process_one(envelope))
        except Exception as exc:  # noqa: BLE001
            from sam_gov.utils.logger import get_logger
            get_logger(__name__).error(
                f"thread_match error for row_index={envelope.row_index}: {exc}"
            )
            results.append(None)
    return results


def run() -> None:
    run_worker_loop(
        servicebus_fqns=SERVICEBUS_FQNS,
        input_queue=QUEUE_NAMES["normalized"],
        output_queue=QUEUE_NAMES["threaded"],
        worker_name="thread_match",
        batch_handler=_handle_normalized_batch,
    )
