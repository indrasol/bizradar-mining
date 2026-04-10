from typing import List, Optional

from sam_gov.services.cron.csv_import_sam_gov import build_embedding_text_full_row, EMBED_MODEL, _coerce_row_for_supabase
from sam_gov.utils.logger import get_logger
from sam_gov.utils.openai_client import get_openai_client
from .config import SERVICEBUS_FQNS, QUEUE_NAMES
from .contracts import QueueEnvelope, make_envelope, idempotency_key
from .queue_io import run_worker_loop


logger = get_logger(__name__)


def _handle_enriched_batch(envelopes: List[QueueEnvelope]) -> List[Optional[QueueEnvelope]]:
    """
    Embed all N rows in a single embeddings.create() call.

    text-embedding-3-small supports up to 2,048 inputs per request, so a batch
    of N=10–20 is well within limits. One API call replaces N individual calls,
    giving an N× throughput improvement at this stage.
    """
    rows = []
    run_metas = []
    for envelope in envelopes:
        payload = envelope.data if envelope.data is not None else {}
        row = payload.get("row")
        run_meta = payload.get("run_meta") if isinstance(payload.get("run_meta"), dict) else {}
        run_metas.append(run_meta)
        if not isinstance(row, dict):
            logger.error(f"embedding: missing 'row' dict for row_index={envelope.row_index}")
            rows.append(None)
        else:
            rows.append(dict(row))

    texts = []
    text_indices = []
    for idx, row in enumerate(rows):
        if row is None:
            continue
        text = build_embedding_text_full_row(row)
        row["embedding_text"] = text
        if text and str(text).strip():
            texts.append(text)
            text_indices.append(idx)

    embeddings_by_idx = {}
    if texts:
        try:
            client = get_openai_client()
            response = client.embeddings.create(model=EMBED_MODEL, input=texts)
            for list_pos, idx in enumerate(text_indices):
                embeddings_by_idx[idx] = response.data[list_pos].embedding
        except Exception as exc:  # noqa: BLE001
            logger.error(f"embedding batch API call failed: {exc}")

    results: List[Optional[QueueEnvelope]] = []
    for idx, (envelope, row, run_meta) in enumerate(zip(envelopes, rows, run_metas)):
        if row is None:
            results.append(None)
            continue
        try:
            if idx in embeddings_by_idx:
                row["embedding"] = embeddings_by_idx[idx]
                row["embedding_model"] = EMBED_MODEL
                row["embedding_version"] = 1
            row = _coerce_row_for_supabase(row)
            notice_id_val = row.get("notice_id")
            notice_id = str(notice_id_val).strip() if notice_id_val is not None else ""
            results.append(make_envelope(
                run_id=envelope.run_id,
                trace_id=envelope.trace_id,
                stage="embedded_rows",
                notice_id=notice_id,
                source_file=envelope.source_file,
                row_index=envelope.row_index,
                message_id=idempotency_key(envelope.run_id, "embedded", envelope.row_index, notice_id),
                data={"row": row, "run_meta": run_meta},
                attempt=envelope.attempt,
            ))
        except Exception as exc:  # noqa: BLE001
            logger.error(f"embedding: envelope build error for row_index={envelope.row_index}: {exc}")
            results.append(None)

    return results


def run() -> None:
    run_worker_loop(
        servicebus_fqns=SERVICEBUS_FQNS,
        input_queue=QUEUE_NAMES["enriched"],
        output_queue=QUEUE_NAMES["embedded"],
        worker_name="embedding",
        batch_handler=_handle_enriched_batch,
    )
