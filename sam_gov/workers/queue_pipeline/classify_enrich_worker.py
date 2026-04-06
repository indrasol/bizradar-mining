import asyncio
from typing import Optional

from sam_gov.services.cron.csv_import_sam_gov import (
    VERSIONS_PER_THREAD_FOR_SUMMARY,
    _classify_opportunity_type,
    _fetch_versions_by_thread_ids_sb,
    _fetch_versions_by_thread_keys_sb,
    _looks_like_uuid,
    _coerce_row_for_supabase,
    parse_date,
)
from sam_gov.services.summary_service import build_prioritized_thread_description, generate_description_summary
from sam_gov.utils.db_utils import get_supabase_connection
from .config import SERVICEBUS_FQNS, QUEUE_NAMES
from .contracts import QueueEnvelope, make_envelope, idempotency_key
from .queue_io import run_worker_loop

_SUPABASE = get_supabase_connection(use_service_key=True)


def _load_persisted_versions(row: dict) -> list[dict]:
    # Use safe extraction to avoid truth-value ambiguity on arrays
    val_thread_id = row.get("thread_id")
    thread_id = str(val_thread_id).strip() if val_thread_id is not None else ""
    if (thread_id is not None and str(thread_id).strip()) and _looks_like_uuid(thread_id):
        by_id = _fetch_versions_by_thread_ids_sb(
            _SUPABASE,
            [thread_id],
            per_thread_limit=VERSIONS_PER_THREAD_FOR_SUMMARY,
        )
        return by_id.get(thread_id, [])

    val_thread_key = row.get("thread_key")
    thread_key = str(val_thread_key).strip() if val_thread_key is not None else ""
    if thread_key is not None and str(thread_key).strip():
        by_key = _fetch_versions_by_thread_keys_sb(
            _SUPABASE,
            [thread_key],
            per_thread_limit=VERSIONS_PER_THREAD_FOR_SUMMARY,
        )
        return by_key.get(thread_key, [])
    return []


async def _enrich_row(row: dict) -> dict:
    persisted_versions = _load_persisted_versions(row)
    description_text = build_prioritized_thread_description(
        current_opportunity=row,
        persisted_versions=persisted_versions,
        in_memory_versions=[row],
        max_versions=25,
    )
    summary_resp = await generate_description_summary("description: " + description_text)
    summary = summary_resp.get("summary", {}) if isinstance(summary_resp, dict) else {}
    row["objective"] = summary.get("objective", "")
    row["expected_outcome"] = summary.get("goal", "")
    row["eligibility"] = summary.get("eligibility", "")
    row["key_facts"] = summary.get("key_facts", "")
    row["funding"] = summary.get("budget", "")
    due_date_str = summary.get("due_date", "")
    row["due_date"] = parse_date(due_date_str) if due_date_str else None

    classification = await _classify_opportunity_type(
        row=row,
        thread_context_text=description_text[:6000],
        persisted_versions=persisted_versions,
    )
    row["opportunity_type"] = classification.get("label", "UNKNOWN")
    row["opportunity_type_confidence"] = classification.get("confidence", 0.0)
    row["opportunity_type_method"] = classification.get("method", "unknown")
    row["opportunity_type_evidence"] = classification.get("evidence", [])
    row["opportunity_type_abstained"] = bool(classification.get("abstained", False))
    row["opportunity_type_needs_review"] = bool(classification.get("needs_review", False))
    return row


def _handle_threaded(envelope: QueueEnvelope) -> Optional[QueueEnvelope]:
    payload = envelope.data if envelope.data is not None else {}
    row = payload.get("row")
    run_meta = payload.get("run_meta") if isinstance(payload.get("run_meta"), dict) else {}
    if not isinstance(row, dict):
        raise ValueError("threaded payload missing 'row' dict")

    row = asyncio.run(_enrich_row(row))
    row = _coerce_row_for_supabase(row)
    notice_id_val = row.get("notice_id")
    notice_id = str(notice_id_val).strip() if notice_id_val is not None else ""
    return make_envelope(
        run_id=envelope.run_id,
        trace_id=envelope.trace_id,
        stage="enriched_rows",
        notice_id=notice_id,
        source_file=envelope.source_file,
        row_index=envelope.row_index,
        message_id=idempotency_key(envelope.run_id, "enriched", envelope.row_index, notice_id),
        data={"row": row, "run_meta": run_meta},
        attempt=envelope.attempt,
    )


def run() -> None:
    run_worker_loop(
        servicebus_fqns=SERVICEBUS_FQNS,
        input_queue=QUEUE_NAMES["threaded"],
        output_queue=QUEUE_NAMES["enriched"],
        worker_name="classify_enrich",
        handler=_handle_threaded,
    )
