import asyncio
from typing import Optional

from sam_gov.services.cron.csv_import_sam_gov import build_embedding_text_full_row, generate_embedding, EMBED_MODEL, _coerce_row_for_supabase
from .config import SERVICEBUS_FQNS, QUEUE_NAMES
from .contracts import QueueEnvelope, make_envelope, idempotency_key
from .queue_io import run_worker_loop


async def _embed_row(row: dict) -> dict:
    text_for_embedding = build_embedding_text_full_row(row)
    row["embedding_text"] = text_for_embedding
    if text_for_embedding is not None and str(text_for_embedding).strip():
        row["embedding"] = await generate_embedding(text_for_embedding)
        row["embedding_model"] = EMBED_MODEL
        row["embedding_version"] = 1
    return row


def _handle_enriched(envelope: QueueEnvelope) -> Optional[QueueEnvelope]:
    payload = envelope.data if envelope.data is not None else {}
    row = payload.get("row")
    run_meta = payload.get("run_meta") if isinstance(payload.get("run_meta"), dict) else {}
    if not isinstance(row, dict):
        raise ValueError("enriched payload missing 'row' dict")
    row = asyncio.run(_embed_row(row))
    row = _coerce_row_for_supabase(row)

    notice_id_val = row.get("notice_id")
    notice_id = str(notice_id_val).strip() if notice_id_val is not None else ""
    return make_envelope(
        run_id=envelope.run_id,
        trace_id=envelope.trace_id,
        stage="embedded_rows",
        notice_id=notice_id,
        source_file=envelope.source_file,
        row_index=envelope.row_index,
        message_id=idempotency_key(envelope.run_id, "embedded", envelope.row_index, notice_id),
        data={"row": row, "run_meta": run_meta},
        attempt=envelope.attempt,
    )


def run() -> None:
    run_worker_loop(
        servicebus_fqns=SERVICEBUS_FQNS,
        input_queue=QUEUE_NAMES["enriched"],
        output_queue=QUEUE_NAMES["embedded"],
        worker_name="embedding",
        handler=_handle_enriched,
    )
