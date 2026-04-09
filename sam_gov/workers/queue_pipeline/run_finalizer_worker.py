import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Set

from sam_gov.utils.logger import get_logger
from sam_gov.utils.db_utils import get_supabase_connection
from sam_gov.config.settings import env_int
from .config import SERVICEBUS_FQNS, QUEUE_NAMES
from .contracts import QueueEnvelope
from .queue_io import run_worker_loop


logger = get_logger(__name__)
_SUPABASE = None
_RUN_COUNTS: Dict[str, int] = {}
_FINALIZED_RUNS: Set[str] = set()
FINALIZER_IDLE_SECONDS = max(
    30,
    env_int("PIPELINE_FINALIZER_IDLE_SECONDS_BIZ", 180, legacy_names=("PIPELINE_FINALIZER_IDLE_SECONDS",)),
)


@dataclass
class RunState:
    seen_notice_ids: set[str] = field(default_factory=set)
    processed_notice_rows: int = 0
    expected_notice_rows: int = 0
    import_limit: Optional[int] = None
    last_seen_ts: float = field(default_factory=time.time)


_RUN_STATE: Dict[str, RunState] = {}


def _mark_inactive_notices(latest_notice_ids: set[str]) -> int:
    if not latest_notice_ids:
        logger.warning("run_finalizer skip inactive-marking: empty latest_notice_ids set")
        return 0

    supabase = _get_supabase()
    active_resp = (
        supabase
        .table("ai_enhanced_opportunity_versions")
        .select("notice_id")
        .eq("is_latest_in_thread", True)
        .eq("active", True)
        .execute()
    )
    active_rows = getattr(active_resp, "data", None)
    active_rows = active_rows if active_rows is not None else []
    active_ids = {r.get("notice_id") for r in active_rows if r.get("notice_id")}
    to_deactivate = list(active_ids - latest_notice_ids)
    if not to_deactivate:
        return 0

    marked_inactive = 0
    chunk_size = 500
    for idx in range(0, len(to_deactivate), chunk_size):
        chunk = to_deactivate[idx: idx + chunk_size]
        (
            supabase
            .table("ai_enhanced_opportunity_versions")
            .update({"active": False})
            .in_("notice_id", chunk)
            .eq("is_latest_in_thread", True)
            .execute()
        )
        marked_inactive += len(chunk)
    return marked_inactive


def _parse_import_limit(value: object) -> Optional[int]:
    if value in (None, "", "None", "none", "null"):
        return None
    try:
        parsed = int(value)  # type: ignore[arg-type]
        return parsed if parsed > 0 else None
    except Exception:
        return None


def _get_supabase():
    global _SUPABASE
    if _SUPABASE is None:
        _SUPABASE = get_supabase_connection(use_service_key=True)
    return _SUPABASE


def _maybe_finalize_run(run_id: str, now_ts: float) -> None:
    if run_id in _FINALIZED_RUNS:
        return
    state = _RUN_STATE.get(run_id)
    if state is None:
        return

    reached_expected = (
        state.expected_notice_rows > 0 and state.processed_notice_rows >= state.expected_notice_rows
    )
    stale_run = (now_ts - state.last_seen_ts) >= FINALIZER_IDLE_SECONDS
    if not reached_expected and not stale_run:
        return

    try:
        # Align with monolith behavior: do not mark inactive on partial imports.
        if state.import_limit is None:
            marked_inactive = _mark_inactive_notices(state.seen_notice_ids)
            logger.info(
                f"run_finalizer finalized run_id={run_id} reason="
                f"{'expected_notice_rows' if reached_expected else 'idle_timeout'} "
                f"processed={state.processed_notice_rows} expected={state.expected_notice_rows} "
                f"marked_inactive={marked_inactive}"
            )
        else:
            logger.info(
                f"run_finalizer finalized partial run_id={run_id} "
                f"processed={state.processed_notice_rows} expected={state.expected_notice_rows} "
                f"import_limit={state.import_limit}; inactive-marking skipped"
            )
    except Exception as exc:  # noqa: BLE001
        logger.error(f"run_finalizer failed finalize run_id={run_id}: {exc}")
        return

    _FINALIZED_RUNS.add(run_id)
    _RUN_STATE.pop(run_id, None)


def _handle_result(envelope: QueueEnvelope) -> Optional[QueueEnvelope]:
    run_id = envelope.run_id
    _RUN_COUNTS[run_id] = _RUN_COUNTS.get(run_id, 0) + 1
    payload = envelope.data if envelope.data is not None else {}
    row = payload.get("row") if isinstance(payload.get("row"), dict) else {}
    run_meta = payload.get("run_meta") if isinstance(payload.get("run_meta"), dict) else {}
    row_obj = row if row is not None else {}
    notice_id = str(row_obj.get("notice_id") if row_obj.get("notice_id") is not None else "").strip()

    state = _RUN_STATE.setdefault(run_id, RunState())
    state.last_seen_ts = time.time()
    if notice_id is not None and str(notice_id).strip():
        state.seen_notice_ids.add(notice_id)
        state.processed_notice_rows += 1

    expected_notice_rows = int(run_meta.get("notice_count") or 0)
    if expected_notice_rows > state.expected_notice_rows:
        state.expected_notice_rows = expected_notice_rows

    parsed_limit = _parse_import_limit(run_meta.get("import_limit"))
    if parsed_limit is not None:
        state.import_limit = parsed_limit

    if _RUN_COUNTS[run_id] % 100 == 0:
        logger.info(
            f"run_finalizer progress run_id={run_id} processed_messages={_RUN_COUNTS[run_id]} "
            f"processed_notice_rows={state.processed_notice_rows} expected_notice_rows={state.expected_notice_rows}"
        )

    now_ts = time.time()
    _maybe_finalize_run(run_id, now_ts)
    # Also sweep other stale runs to avoid leaving old state.
    for stale_run_id in list(_RUN_STATE.keys()):
        if stale_run_id != run_id:
            _maybe_finalize_run(stale_run_id, now_ts)
    return None


def run() -> None:
    run_worker_loop(
        servicebus_fqns=SERVICEBUS_FQNS,
        input_queue=QUEUE_NAMES["results"],
        output_queue=None,
        worker_name="run_finalizer",
        handler=_handle_result,
    )
