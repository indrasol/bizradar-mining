import os
import json
import re
import uuid
import hashlib
import time
import pandas as pd
import asyncio

from typing import Dict, Any, List, Optional
from datetime import datetime, date

from sam_gov.utils.logger import get_logger
from sam_gov.utils.db_utils import get_supabase_connection
from sam_gov.config.settings import env_bool, env_float, env_int, env_str
from sam_gov.services.summary_service import (
    generate_description_summary,
    build_prioritized_thread_description,
    normalize_thread_key,
    version_sort_key,
)

# Configure logging
logger = get_logger(__name__)

# Debug-mode instrumentation config (session-scoped).
DEBUG_SESSION_ID = "d4c923"
DEBUG_LOG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../debug-d4c923.log")
)


def _debug_log(run_id: str, hypothesis_id: str, location: str, message: str, data: Optional[Dict[str, Any]] = None) -> None:
    def json_serial(obj: Any) -> Any:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    try:
        payload = {
            "sessionId": DEBUG_SESSION_ID,
            "runId": run_id,
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data or {},
            "timestamp": int(time.time() * 1000),
        }
        with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, default=json_serial, ensure_ascii=True) + "\n")
    except Exception:
        pass

# === Database Functions (from database.py) ===

EMBED_MODEL = "text-embedding-3-small"
CPU_COUNT = os.cpu_count() or 2


def _get_env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    """Read bounded int from env for safe runner tuning."""
    parsed = env_int(f"{name}_BIZ", default, legacy_names=(name,))
    return max(minimum, min(parsed, maximum))


# Safe defaults for free GitHub Actions runners (usually 2 vCPU).
CSV_BATCH_SIZE = _get_env_int("CSV_PROCESS_BATCH_SIZE", 2000, 200, 10000)
SUMMARY_CONCURRENCY = _get_env_int("CSV_SUMMARY_CONCURRENCY", max(1, min(4, CPU_COUNT)), 1, 6)
DB_UPSERT_BATCH_SIZE = _get_env_int("CSV_DB_UPSERT_BATCH_SIZE", 200, 25, 1000)
INDEXING_EMBED_CONCURRENCY = _get_env_int("CSV_INDEX_EMBED_CONCURRENCY", max(1, min(3, CPU_COUNT)), 1, 4)
VERSIONS_PER_THREAD_FOR_SUMMARY = _get_env_int("CSV_SUMMARY_VERSIONS_PER_THREAD", 50, 5, 100)
THREAD_CANDIDATE_LIMIT = _get_env_int("CSV_THREAD_CANDIDATE_LIMIT", 20, 5, 100)
THREAD_HIGH_CONFIDENCE = env_float("CSV_THREAD_HIGH_CONFIDENCE_BIZ", 0.78, legacy_names=("CSV_THREAD_HIGH_CONFIDENCE",))
THREAD_MEDIUM_CONFIDENCE = env_float("CSV_THREAD_MEDIUM_CONFIDENCE_BIZ", 0.60, legacy_names=("CSV_THREAD_MEDIUM_CONFIDENCE",))
CLASSIFIER_ENABLED = env_bool(
    "CSV_ENABLE_OPPORTUNITY_CLASSIFIER_BIZ",
    True,
    legacy_names=("CSV_ENABLE_OPPORTUNITY_CLASSIFIER",),
)
CLASSIFIER_CONCURRENCY = _get_env_int("CSV_CLASSIFIER_CONCURRENCY", max(1, min(3, CPU_COUNT)), 1, 6)
CLASSIFIER_MIN_CONFIDENCE = env_float(
    "CSV_CLASSIFIER_MIN_CONFIDENCE_BIZ",
    0.62,
    legacy_names=("CSV_CLASSIFIER_MIN_CONFIDENCE",),
)
CLASSIFIER_FLIP_MIN_CONFIDENCE = env_float(
    "CSV_CLASSIFIER_FLIP_MIN_CONFIDENCE_BIZ",
    0.78,
    legacy_names=("CSV_CLASSIFIER_FLIP_MIN_CONFIDENCE",),
)
CLASSIFIER_PROMPT_VERSION = env_str(
    "CSV_CLASSIFIER_PROMPT_VERSION_BIZ",
    "opp-type-v1",
    legacy_names=("CSV_CLASSIFIER_PROMPT_VERSION",),
)
CLASSIFIER_MODEL = env_str("CSV_CLASSIFIER_MODEL_BIZ", "gpt-4.1-mini", legacy_names=("CSV_CLASSIFIER_MODEL",))
SUPABASE_RETRY_ATTEMPTS = _get_env_int("CSV_SUPABASE_RETRY_ATTEMPTS", 3, 1, 8)
SUPABASE_RETRY_BASE_MS = _get_env_int("CSV_SUPABASE_RETRY_BASE_MS", 250, 50, 5000)
SUPABASE_CLIENT_ROTATE_EVERY = _get_env_int("CSV_SUPABASE_CLIENT_ROTATE_EVERY", 2500, 500, 20000)

CSV_REQUIRED_COLUMNS = [
    "NoticeId", "Title", "Sol#", "Department/Ind.Agency", "Sub-Tier", "PostedDate",
    "ArchiveType", "ArchiveDate", "ResponseDeadLine", "NaicsCode", "Description", "Active",
    "Type", "BaseType", "ClassificationCode",
    "PrimaryContactTitle", "PrimaryContactFullname", "PrimaryContactEmail", "PrimaryContactPhone", "PrimaryContactFax",
    "SecondaryContactTitle", "SecondaryContactFullname", "SecondaryContactEmail", "SecondaryContactPhone", "SecondaryContactFax",
]

CLASSIFICATION_LABELS = (
    "RFI",
    "RFP",
    "RFQ",
    "RFK",
    "SOURCES_SOUGHT",
    "PRESOLICITATION",
    "OTHER",
    "UNKNOWN",
)

_TYPE_KEYWORD_PATTERNS = {
    "RFI": [
        r"\brfi\b",
        r"request\s+for\s+information",
    ],
    "RFP": [
        r"\brfp\b",
        r"request\s+for\s+proposal(?:s)?",
        r"letter\s+request\s+for\s+proposal",
        r"draft\s+rfp",
    ],
    "RFQ": [
        r"\brfq\b",
        r"request\s+for\s+quotation(?:s)?",
        r"request\s+for\s+quote(?:s)?",
    ],
    "RFK": [
        r"\brfk\b",
        r"request\s+for\s+knowledge",
    ],
    "SOURCES_SOUGHT": [
        r"sources?\s+sought",
        r"sought\s+sources?",
    ],
    "PRESOLICITATION": [
        r"presolicitation",
        r"pre[\s-]?solicitation",
    ],
}
_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$")


def _looks_like_uuid(value: Any) -> bool:
    text = safe_string(value, "")
    return bool(text and _UUID_RE.fullmatch(text))

def _canonicalize_value(v):
    if v is None:
        return ""
    if isinstance(v, (list, tuple)):
        return ", ".join(str(x) for x in v if x is not None)
    if isinstance(v, dict):
        return "; ".join(f"{k}={_canonicalize_value(v[k])}" for k in sorted(v.keys()))
    return str(v)

def build_embedding_text_full_row(opp: Dict[str, Any]) -> str:
    """Create canonical text from the entire row to embed the whole record."""
    fields_in_order = [
        "title","description","objective","expected_outcome","eligibility","key_facts",
        "department","sub_departments","naics_code","classification_code",
        "source_notice_type","source_base_type","opportunity_type",
        "published_date","response_date","due_date","funding","solicitation_number",
        "notice_id","url","point_of_contact","active"
    ]
    lines, seen = [], set()
    for f in fields_in_order:
        if f in opp:
            seen.add(f)
            lines.append(f"{f}: {_canonicalize_value(opp.get(f))}")
    for k, v in opp.items():
        if k in seen:
            continue
        if k in {"provisional_assignment", "provisional_match_method", "provisional_match_score"}:
            continue
        lines.append(f"{k}: {_canonicalize_value(v)}")
    return "\n".join(lines)[:20000]

async def generate_embedding(text: str) -> List[float]:
    from sam_gov.utils.openai_client import get_openai_client
    client = get_openai_client()
    r = client.embeddings.create(model=EMBED_MODEL, input=text)
    return r.data[0].embedding

def _make_json_serializable(value):
    """Coerce value into JSON-serializable form for Supabase client."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value

def _coerce_row_for_supabase(row: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure row payload only contains JSON-serializable values.

    - Dates to ISO strings
    - point_of_contact to dict
    """
    safe_row = dict(row)
    for key in ("published_date", "response_date", "due_date", "source_archive_date", "source_posted_at", "classified_at", "type_changed_at"):
        value = safe_row.get(key)
        if isinstance(value, (datetime, date)):
            # For date columns, a 'YYYY-MM-DD' string is acceptable
            safe_row[key] = value.isoformat()
    # Ensure JSON not stringified
    poc = safe_row.get("point_of_contact")
    if isinstance(poc, str):
        try:
            safe_row["point_of_contact"] = json.loads(poc)
        except Exception:
            safe_row["point_of_contact"] = None
    # Ensure embedding is a JSON-serializable list
    emb = safe_row.get("embedding")
    try:
        if hasattr(emb, "tolist"):
            safe_row["embedding"] = emb.tolist()
    except Exception:
        pass
    return safe_row



def _progress_line(prefix: str, current: int, total: int):
    """CI-friendly progress output with percentage and counts."""
    if total <= 0:
        print(f"{prefix}: 100% (0/0)", flush=True)
        return
    pct = int((current / total) * 100)
    bar_len = 30
    filled = int((pct / 100) * bar_len)
    bar = "#" * filled + "-" * (bar_len - filled)
    print(f"{prefix} [{bar}] {pct}% ({current}/{total})", flush=True)


def _normalize_naive_dt(value):
    """Normalize timestamp for safe comparison/storage (naive UTC-like)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    return value


def _chunked(items: List[Any], size: int):
    """Yield list chunks of size N."""
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _fetch_versions_by_thread_keys_sb(
    supabase,
    thread_keys: List[str],
    per_thread_limit: int = 50,
    key_chunk_size: int = 150,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch version rows via Supabase client (no direct DB/psycopg dependency).
    """
    clean_keys = [str(k).strip() for k in (thread_keys or []) if str(k).strip()]
    if not clean_keys:
        return {}

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    try:
        for key_chunk in _chunked(clean_keys, key_chunk_size):
            try:
                resp = (
                    supabase
                    .table("ai_enhanced_opportunity_versions")
                    .select(
                        "thread_key, notice_id, description, source_posted_at, "
                        "source_archive_type, source_archive_date, ingested_at, published_date, opportunity_type"
                    )
                    .in_("thread_key", key_chunk)
                    .execute()
                )
            except Exception:
                # PostgREST schema cache can lag new columns briefly.
                resp = (
                    supabase
                    .table("ai_enhanced_opportunity_versions")
                    .select(
                        "thread_key, notice_id, description, source_posted_at, "
                        "source_archive_type, source_archive_date, ingested_at, published_date"
                    )
                    .in_("thread_key", key_chunk)
                    .execute()
                )
            rows = getattr(resp, "data", None) or []
            for row in rows:
                thread_key = (row.get("thread_key") or "").strip()
                if not thread_key:
                    continue
                grouped.setdefault(thread_key, []).append(row)

        for key, values in grouped.items():
            values.sort(key=version_sort_key, reverse=True)
            grouped[key] = values[:per_thread_limit]

        return grouped
    except Exception as e:
        logger.warning(f"Could not batch fetch version threads via Supabase: {e}")
        return {}


def _fetch_versions_by_thread_ids_sb(
    supabase,
    thread_ids: List[str],
    per_thread_limit: int = 50,
    id_chunk_size: int = 150,
) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch version rows grouped by thread_id via Supabase client."""
    clean_ids = [str(t).strip() for t in (thread_ids or []) if str(t).strip()]
    if not clean_ids:
        return {}
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    try:
        for id_chunk in _chunked(clean_ids, id_chunk_size):
            try:
                resp = (
                    supabase
                    .table("ai_enhanced_opportunity_versions")
                    .select(
                        "thread_id, notice_id, description, source_posted_at, "
                        "source_archive_type, source_archive_date, ingested_at, published_date, opportunity_type"
                    )
                    .in_("thread_id", id_chunk)
                    .execute()
                )
            except Exception:
                resp = (
                    supabase
                    .table("ai_enhanced_opportunity_versions")
                    .select(
                        "thread_id, notice_id, description, source_posted_at, "
                        "source_archive_type, source_archive_date, ingested_at, published_date"
                    )
                    .in_("thread_id", id_chunk)
                    .execute()
                )
            rows = getattr(resp, "data", None) or []
            for row in rows:
                thread_id = (row.get("thread_id") or "").strip()
                if not thread_id:
                    continue
                grouped.setdefault(thread_id, []).append(row)
        for key, values in grouped.items():
            values.sort(key=version_sort_key, reverse=True)
            grouped[key] = values[:per_thread_limit]
        return grouped
    except Exception as e:
        logger.warning(f"Could not batch fetch version threads by id via Supabase: {e}")
        return {}



def _build_version_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    """Build version payload for ai_enhanced_opportunity_versions."""
    source_posted_at = row.get("source_posted_at")
    source_archive_date = row.get("source_archive_date")
    # Ensure conflict key fields are deterministic even when upstream data is sparse.
    if not source_posted_at and isinstance(row.get("published_date"), date):
        source_posted_at = datetime.combine(row.get("published_date"), datetime.min.time())
    if not source_archive_date and isinstance(row.get("published_date"), date):
        source_archive_date = row.get("published_date")

    return _coerce_row_for_supabase({
        "thread_key": row.get("thread_key"),
        "thread_id": row.get("thread_id"),
        "thread_key_version": row.get("thread_key_version", 1),
        "match_method": row.get("match_method", "new_thread"),
        "match_score": row.get("match_score"),
        "matched_to_version_id": row.get("matched_to_version_id"),
        "decision_reason": row.get("decision_reason"),
        "content_simhash": row.get("content_simhash"),
        "metadata_simhash": row.get("metadata_simhash"),
        "title_norm": row.get("title_norm"),
        "description_norm": row.get("description_norm"),
        "department_norm": row.get("department_norm"),
        "solicitation_number_norm": row.get("solicitation_number_norm"),
        "is_latest_in_thread": row.get("is_latest_in_thread", False),
        "notice_id": row.get("notice_id"),
        "solicitation_number": row.get("solicitation_number"),
        "source_posted_at": _normalize_naive_dt(source_posted_at),
        "source_archive_type": row.get("source_archive_type") or "",
        "source_archive_date": source_archive_date,
        "title": row.get("title"),
        "department": row.get("department"),
        "naics_code": row.get("naics_code"),
        "published_date": row.get("published_date"),
        "response_date": row.get("response_date"),
        "description": row.get("description"),
        "url": row.get("url"),
        "active": row.get("active"),
        "additional_description": row.get("additional_description"),
        "sub_departments": row.get("sub_departments"),
        "point_of_contact": row.get("point_of_contact"),
        "expected_outcome": row.get("expected_outcome"),
        "funding": row.get("funding"),
        "key_facts": row.get("key_facts"),
        "eligibility": row.get("eligibility"),
        "objective": row.get("objective"),
        "due_date": row.get("due_date"),
        "embedding_text": row.get("embedding_text"),
        "embedding": row.get("embedding"),
        "embedding_model": row.get("embedding_model"),
        "embedding_version": row.get("embedding_version"),
        "opportunity_type": row.get("opportunity_type"),
        "opportunity_type_confidence": row.get("opportunity_type_confidence"),
        "opportunity_type_method": row.get("opportunity_type_method"),
        "opportunity_type_evidence": row.get("opportunity_type_evidence"),
        "opportunity_type_abstained": row.get("opportunity_type_abstained", False),
        "opportunity_type_needs_review": row.get("opportunity_type_needs_review", False),
        "classification_model": row.get("classification_model"),
        "classification_prompt_version": row.get("classification_prompt_version"),
        "classified_at": row.get("classified_at"),
    })


def insert_data(rows):
    """Insert or update rows while keeping versioned amendments."""
    supabase = get_supabase_connection(use_service_key=True)
    inserted = 0
    skipped = 0
    updated = 0
    total_rows = len(rows)
    last_pct = -1

    version_payloads: List[Dict[str, Any]] = []
    thread_ids_touched: set = set()
    seen_rows: List[Dict[str, Any]] = []
    thread_id_cache: Dict[str, str] = {}
    fallback_rpc_resolutions = 0
    # region agent log
    _debug_log(
        run_id="import-run",
        hypothesis_id="H3",
        location="csv_import_sam_gov.py:insert_data",
        message="Insert phase started",
        data={
            "rowCount": len(rows),
            "batchSize": DB_UPSERT_BATCH_SIZE,
        },
    )
    # endregion
    for idx, row in enumerate(rows, start=1):
        supabase = _maybe_rotate_supabase_client(supabase, idx)
        notice_id = row.get("notice_id")
        if not notice_id:
            skipped += 1
            continue

        assignment = _resolve_thread_assignment_from_seen(seen_rows, row)
        if assignment is None:
            precomputed = row.get("provisional_assignment")
            if isinstance(precomputed, dict):
                assignment = precomputed
            else:
                fallback_rpc_resolutions += 1
                # region agent log
                if fallback_rpc_resolutions <= 3:
                    _debug_log(
                        run_id="import-run",
                        hypothesis_id="H7",
                        location="csv_import_sam_gov.py:insert_data",
                        message="Fallback RPC resolution due to missing provisional assignment",
                        data={
                            "idx": idx,
                            "noticeId": notice_id,
                            "precomputedType": type(precomputed).__name__,
                        },
                    )
                # endregion
                assignment = _resolve_thread_assignment(supabase, row)
        if idx % 2000 == 0:
            # region agent log
            _debug_log(
                run_id="import-run",
                hypothesis_id="H7",
                location="csv_import_sam_gov.py:insert_data",
                message="Insert assignment progress",
                data={
                    "processed": idx,
                    "total": len(rows),
                    "fallbackRpcResolutions": fallback_rpc_resolutions,
                },
            )
            # endregion
        thread_id = _ensure_thread_id(supabase, row, assignment, thread_id_cache=thread_id_cache)
        row["thread_id"] = thread_id
        row["match_method"] = assignment.get("match_method", "new_thread")
        row["match_score"] = assignment.get("match_score")
        row["matched_to_version_id"] = assignment.get("matched_to_version_id")
        row["decision_reason"] = assignment.get("decision_reason")
        row["is_latest_in_thread"] = False
        if thread_id:
            thread_ids_touched.add(thread_id)
        seen_rows.append({
            "thread_id": thread_id,
            "solicitation_number_norm": row.get("solicitation_number_norm"),
            "department_norm": row.get("department_norm"),
            "title_norm": row.get("title_norm"),
            "contact_email_norm": row.get("contact_email_norm"),
            "contact_phone_norm": row.get("contact_phone_norm"),
            "contact_name_norm": row.get("contact_name_norm"),
            "metadata_simhash": row.get("metadata_simhash"),
        })

        if thread_id:
            version_payloads.append(_build_version_payload(row))

    try:
        dedup_versions: Dict[tuple, Dict[str, Any]] = {}
        for payload in version_payloads:
            key = (
                payload.get("notice_id"),
                payload.get("source_posted_at"),
                payload.get("source_archive_type") or "",
                payload.get("source_archive_date"),
            )
            dedup_versions[key] = payload
        version_payloads = list(dedup_versions.values())

        processed = 0
        for batch in _chunked(version_payloads, DB_UPSERT_BATCH_SIZE):
            try:
                supabase.table("ai_enhanced_opportunity_versions").upsert(
                    batch,
                    on_conflict="notice_id,source_posted_at,source_archive_type,source_archive_date",
                ).execute()
                updated += len(batch)
            except Exception as e:
                logger.error(f"Error upserting version batch size={len(batch)}: {e}")
                # region agent log
                _debug_log(
                    run_id="import-run",
                    hypothesis_id="H4",
                    location="csv_import_sam_gov.py:insert_data",
                    message="Version upsert batch failed",
                    data={
                        "batchSize": len(batch),
                        "errorType": type(e).__name__,
                        "errorText": str(e)[:240],
                    },
                )
                # endregion
                skipped += len(batch)
            processed += len(batch)
            pct = int((processed / total_rows) * 100) if total_rows else 100
            if pct != last_pct:
                _progress_line("Database import progress", processed, total_rows)
                last_pct = pct

        # Set is_latest_in_thread flag using Python-side ordering.
        for thread_id_chunk in _chunked(list(thread_ids_touched), 100):
            try:
                try:
                    q = (
                        supabase
                        .table("ai_enhanced_opportunity_versions")
                        .select(
                            "version_id, thread_id, source_archive_date, source_posted_at, ingested_at, "
                            "opportunity_type, opportunity_type_confidence"
                        )
                        .in_("thread_id", thread_id_chunk)
                        .order("source_archive_date", desc=True, nullsfirst=False)
                        .order("source_posted_at", desc=True, nullsfirst=False)
                        .order("ingested_at", desc=True)
                        .execute()
                    )
                except Exception:
                    q = (
                        supabase
                        .table("ai_enhanced_opportunity_versions")
                        .select("version_id, thread_id, source_archive_date, source_posted_at, ingested_at")
                        .in_("thread_id", thread_id_chunk)
                        .order("source_archive_date", desc=True, nullsfirst=False)
                        .order("source_posted_at", desc=True, nullsfirst=False)
                        .order("ingested_at", desc=True)
                        .execute()
                    )
                vrows = getattr(q, "data", None) or []
                best_by_thread: Dict[str, Dict[str, Any]] = {}
                for vr in vrows:
                    t_id = vr.get("thread_id")
                    if t_id and t_id not in best_by_thread:
                        best_by_thread[t_id] = vr
                for t_id, latest_row in best_by_thread.items():
                    best_version_id = latest_row.get("version_id")
                    try:
                        supabase.table("ai_enhanced_opportunity_versions") \
                            .update({"is_latest_in_thread": False}) \
                            .eq("thread_id", t_id).neq("version_id", best_version_id).execute()
                        supabase.table("ai_enhanced_opportunity_versions") \
                            .update({"is_latest_in_thread": True}) \
                            .eq("version_id", best_version_id).execute()
                    except Exception as ee:
                        logger.error(f"Failed to set is_latest_in_thread for thread={t_id}: {ee}")
            except Exception as e:
                logger.error(f"Failed to refresh latest flags for thread chunk: {e}")

        inserted = max(0, len(version_payloads) - skipped)
        _progress_line("Database import progress", total_rows, total_rows)
        # region agent log
        _debug_log(
            run_id="import-run",
            hypothesis_id="H3",
            location="csv_import_sam_gov.py:insert_data",
            message="Insert phase finished",
            data={
                "inserted": inserted,
                "updated": updated,
                "skipped": skipped,
                "versionPayloads": len(version_payloads),
                "threadTouched": len(thread_ids_touched),
            },
        )
        # endregion
        return {"inserted": inserted, "updated": updated, "skipped": skipped}
    except Exception as e:
        # region agent log
        _debug_log(
            run_id="import-run",
            hypothesis_id="H5",
            location="csv_import_sam_gov.py:insert_data",
            message="Insert phase raised top-level exception",
            data={"errorType": type(e).__name__, "errorText": str(e)[:240]},
        )
        # endregion
        return {"error": str(e), "inserted": inserted, "updated": updated, "skipped": skipped}

# === CSV Processing Functions ===

def parse_date(date_str):
    """Parse a date string into a date object."""
    if not date_str:
        return None
    
    # Handle NaN/float values from pandas
    if pd.isna(date_str) or (isinstance(date_str, float) and pd.isna(date_str)):
        return None
    
    if not isinstance(date_str, str):
        return None

    raw = date_str.strip()
    if not raw:
        return None

    # SAM "PostedDate" can be time-only (e.g., "16:19.0"); treat as no date.
    if re.fullmatch(r"\d{1,2}:\d{2}(?:\.\d+)?", raw):
        return None

    # Normalize datetime-like strings to their date component.
    if "T" in raw:
        raw = raw.split("T", 1)[0]
    elif " " in raw:
        raw = raw.split(" ", 1)[0]

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue

    return None


def parse_datetime(date_str):
    """Parse datetime-like CSV values into datetime."""
    if not date_str:
        return None
    if pd.isna(date_str):
        return None
    if isinstance(date_str, datetime):
        return _normalize_naive_dt(date_str)
    try:
        raw = str(date_str).strip()
        if not raw:
            return None
        # Accept formats like 2024-03-06 15:16:32.846-05 and ISO strings.
        normalized = raw.replace(" ", "T", 1) if " " in raw and "T" not in raw else raw
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        parsed = datetime.fromisoformat(normalized)
        return _normalize_naive_dt(parsed)
    except Exception:
        return None

def truncate_string(text, max_length=255):
    """Truncate a string to specified maximum length."""
    if not text:
        return text
    return text[:max_length]

def safe_string(value, default=""):
    """Safely convert a value to string, handling NaN and None values."""
    if pd.isna(value) or value is None:
        return default
    return str(value).strip()


def _normalize_text(value: Any) -> str:
    """Normalize free text for robust matching."""
    text = safe_string(value, "")
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"\bamendment\s*0*\d+\b", "amendment", text)
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _normalize_phone(value: Any) -> str:
    """Normalize phone-like strings into digits-only form for matching."""
    text = safe_string(value, "")
    if not text:
        return ""
    digits = re.sub(r"\D+", "", text)
    return digits


def _safe_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _extract_contact_norms(point_of_contact: Any) -> Dict[str, str]:
    """Extract normalized contact signals from point_of_contact payload."""
    payload = _safe_json_dict(point_of_contact)
    primary = payload.get("primary") if isinstance(payload.get("primary"), dict) else {}
    secondary = payload.get("secondary") if isinstance(payload.get("secondary"), dict) else {}

    def _best_norm(*vals: Any) -> str:
        for raw in vals:
            normalized = _normalize_text(raw)
            if normalized:
                return normalized
        return ""

    def _best_phone(*vals: Any) -> str:
        for raw in vals:
            normalized = _normalize_phone(raw)
            if normalized:
                return normalized
        return ""

    email = _best_norm(
        primary.get("email"),
        secondary.get("email"),
    )
    phone = _best_phone(
        primary.get("phone"),
        secondary.get("phone"),
    )
    name = _best_norm(
        primary.get("name"),
        secondary.get("name"),
    )
    return {
        "contact_email_norm": email,
        "contact_phone_norm": phone,
        "contact_name_norm": name,
    }


def _contact_match_strength(row: Dict[str, Any], cand: Dict[str, Any]) -> float:
    """
    Return [0..1] contact match strength.
    1.00: email match
    0.85: phone match
    0.65: name match
    0.00: no match
    """
    row_email = safe_string(row.get("contact_email_norm"), "")
    row_phone = safe_string(row.get("contact_phone_norm"), "")
    row_name = safe_string(row.get("contact_name_norm"), "")
    cand_email = safe_string(cand.get("candidate_contact_email_norm"), "")
    cand_phone = safe_string(cand.get("candidate_contact_phone_norm"), "")
    cand_name = safe_string(cand.get("candidate_contact_name_norm"), "")

    if row_email and cand_email and row_email == cand_email:
        return 1.0
    if row_phone and cand_phone and row_phone == cand_phone:
        return 0.85
    if row_name and cand_name and row_name == cand_name:
        return 0.65
    return 0.0


def _compute_simhash64(text: str) -> Optional[int]:
    """Compute 64-bit simhash from normalized token text."""
    if not text:
        return None
    weights = [0] * 64
    for token in text.split():
        h = int.from_bytes(hashlib.sha1(token.encode("utf-8")).digest()[:8], "big", signed=False)
        for i in range(64):
            bit = (h >> i) & 1
            weights[i] += 1 if bit else -1
    out = 0
    for i, w in enumerate(weights):
        if w >= 0:
            out |= (1 << i)
    # Postgres bigint is signed int64; convert unsigned 64-bit to signed range.
    if out >= (1 << 63):
        out -= (1 << 64)
    return int(out)


def _prepare_matching_signals(row: Dict[str, Any]) -> Dict[str, Any]:
    """Prepare normalized and hash-based fields for thread matching."""
    title_norm = _normalize_text(row.get("title"))
    dept_norm = _normalize_text(row.get("department"))
    sol_norm = _normalize_text(row.get("solicitation_number"))
    desc_norm = _normalize_text(row.get("description"))
    contact_norms = _extract_contact_norms(row.get("point_of_contact"))
    meta_text = " ".join([sol_norm, dept_norm, title_norm]).strip()
    content_text = " ".join([title_norm, desc_norm]).strip()
    row["title_norm"] = title_norm
    row["department_norm"] = dept_norm
    row["solicitation_number_norm"] = sol_norm
    row["description_norm"] = desc_norm
    row["contact_email_norm"] = contact_norms.get("contact_email_norm", "")
    row["contact_phone_norm"] = contact_norms.get("contact_phone_norm", "")
    row["contact_name_norm"] = contact_norms.get("contact_name_norm", "")
    row["metadata_simhash"] = _compute_simhash64(meta_text)
    row["content_simhash"] = _compute_simhash64(content_text)
    return row


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _is_transient_supabase_error(exc: Exception) -> bool:
    text = str(exc or "").lower()
    transient_markers = (
        "connectionterminated",
        "connection terminated",
        "stream",
        "timeout",
        "temporarily unavailable",
        "connection reset",
        "http2",
    )
    return any(marker in text for marker in transient_markers)


def _retry_sleep_seconds(attempt_idx: int) -> float:
    base = max(0.05, SUPABASE_RETRY_BASE_MS / 1000.0)
    return base * (2 ** max(0, attempt_idx))


def _run_with_supabase_retries(fn, *, label: str):
    last_exc = None
    for attempt in range(SUPABASE_RETRY_ATTEMPTS):
        try:
            return fn()
        except Exception as e:
            last_exc = e
            # region agent log
            _debug_log(
                run_id="import-run",
                hypothesis_id="H1",
                location="csv_import_sam_gov.py:_run_with_supabase_retries",
                message="Supabase operation failed on attempt",
                data={
                    "label": label,
                    "attempt": attempt + 1,
                    "maxAttempts": SUPABASE_RETRY_ATTEMPTS,
                    "isTransient": _is_transient_supabase_error(e),
                    "errorType": type(e).__name__,
                    "errorText": str(e)[:240],
                },
            )
            # endregion
            if not _is_transient_supabase_error(e) or attempt >= SUPABASE_RETRY_ATTEMPTS - 1:
                raise
            wait_s = _retry_sleep_seconds(attempt)
            logger.warning(f"{label} transient Supabase error (attempt {attempt + 1}/{SUPABASE_RETRY_ATTEMPTS}): {e}; retrying in {wait_s:.2f}s")
            time.sleep(wait_s)
    if last_exc:
        raise last_exc
    return None


def _maybe_rotate_supabase_client(current_client, op_index: int):
    if op_index > 0 and op_index % SUPABASE_CLIENT_ROTATE_EVERY == 0:
        # region agent log
        _debug_log(
            run_id="import-run",
            hypothesis_id="H1",
            location="csv_import_sam_gov.py:_maybe_rotate_supabase_client",
            message="Rotating Supabase client",
            data={"opIndex": op_index, "rotateEvery": SUPABASE_CLIENT_ROTATE_EVERY},
        )
        # endregion
        return get_supabase_connection(use_service_key=True)
    return current_client


def _hamming_distance_64(a: Optional[int], b: Optional[int]) -> Optional[int]:
    if a is None or b is None:
        return None
    mask = (1 << 64) - 1
    ax = int(a) & mask
    bx = int(b) & mask
    return (ax ^ bx).bit_count()


def _normalize_classifier_text(value: Any) -> str:
    text = safe_string(value, "")
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"[\u2018\u2019]", "'", text)
    text = re.sub(r"[\u201c\u201d]", '"', text)
    text = re.sub(r"[^a-z0-9\s\-/]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _is_low_information_text(text: str, min_chars: int = 80) -> bool:
    if not text:
        return True
    t = _normalize_classifier_text(text)
    if len(t) < min_chars:
        return True
    token_count = len(t.split())
    if token_count <= 12 and len(set(t.split())) <= 4:
        return True
    return False


def _sanitize_label(value: Any, default: str = "UNKNOWN") -> str:
    label = safe_string(value, default).upper().replace(" ", "_")
    return label if label in CLASSIFICATION_LABELS else default


def _extract_keyword_evidence(text: str, label: str) -> List[str]:
    if not text:
        return []
    snippets: List[str] = []
    for pattern in _TYPE_KEYWORD_PATTERNS.get(label, []):
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            snippets.append(text[max(0, m.start() - 40):min(len(text), m.end() + 80)].strip())
    return snippets[:2]


def _detect_rule_based_type(row: Dict[str, Any]) -> Dict[str, Any]:
    title = safe_string(row.get("title"), "")
    description = safe_string(row.get("description"), "")
    source_type = _normalize_classifier_text(row.get("source_notice_type"))
    source_base_type = _normalize_classifier_text(row.get("source_base_type"))
    combined_text = f"{title}\n{description}"
    normalized = _normalize_classifier_text(combined_text)

    # Strong source metadata matches first.
    source_label = None
    if "sources sought" in source_type or "sources sought" in source_base_type:
        source_label = "SOURCES_SOUGHT"
    elif "presolicitation" in source_type or "presolicitation" in source_base_type:
        source_label = "PRESOLICITATION"
    if source_label:
        return {
            "label": source_label,
            "confidence": 0.98,
            "method": "rule_source_type",
            "evidence": [source_type or source_base_type],
            "conflict": False,
        }

    matches: Dict[str, List[str]] = {}
    negated: set = set()
    for label, patterns in _TYPE_KEYWORD_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, normalized, flags=re.IGNORECASE):
                matches.setdefault(label, []).append(pattern)
                negation_pattern = rf"(not|no)\s+(an?\s+)?({pattern})"
                if re.search(negation_pattern, normalized, flags=re.IGNORECASE):
                    negated.add(label)

    for label in list(matches.keys()):
        if label in negated:
            matches.pop(label, None)

    if not matches:
        return {
            "label": "UNKNOWN",
            "confidence": 0.0,
            "method": "rule_none",
            "evidence": [],
            "conflict": False,
        }

    # Rank labels by specificity and context confidence.
    rank = {
        "RFI": 5,
        "RFP": 5,
        "RFQ": 5,
        "RFK": 5,
        "SOURCES_SOUGHT": 4,
        "PRESOLICITATION": 4,
        "OTHER": 1,
        "UNKNOWN": 0,
    }
    sorted_labels = sorted(matches.keys(), key=lambda x: rank.get(x, 0), reverse=True)
    winner = sorted_labels[0]
    conflict = len(sorted_labels) > 1 and sorted_labels[1] != winner
    evidence = _extract_keyword_evidence(combined_text, winner)
    base_conf = 0.92 if not conflict else 0.68
    if "amendment" in normalized:
        base_conf = max(0.65, base_conf - 0.08)
    return {
        "label": winner,
        "confidence": base_conf,
        "method": "rule_keyword",
        "evidence": evidence,
        "conflict": conflict,
    }


async def _classify_type_with_llm(
    row: Dict[str, Any],
    thread_context_text: str,
    prior_type: Optional[str],
) -> Dict[str, Any]:
    from sam_gov.utils.openai_client import get_openai_client

    title = safe_string(row.get("title"), "")
    description = safe_string(row.get("description"), "")
    if not title and not description:
        return {
            "label": "UNKNOWN",
            "confidence": 0.0,
            "method": "llm_empty",
            "evidence": [],
            "abstained": True,
        }

    try:
        client = get_openai_client()
        thread_excerpt = safe_string(thread_context_text, "")[:6000]
        response = client.chat.completions.create(
            model=CLASSIFIER_MODEL,
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You classify US federal opportunities into strict labels.\n"
                        "Allowed labels: RFI, RFP, RFQ, RFK, SOURCES_SOUGHT, PRESOLICITATION, OTHER, UNKNOWN.\n"
                        "Use UNKNOWN if evidence is weak or contradictory.\n"
                        "Prefer explicit notice wording over weak implications.\n"
                        "Return strict JSON with keys: label, confidence, method, evidence, abstained.\n"
                        "method must be one of: explicit_keyword, implicit_intent, historical_transition, unknown.\n"
                        "confidence must be numeric 0..1.\n"
                        "evidence must be a short array of snippets from the provided text."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Title: {title}\n"
                        f"Description: {description[:3500]}\n"
                        f"SourceType: {safe_string(row.get('source_notice_type'), '')}\n"
                        f"SourceBaseType: {safe_string(row.get('source_base_type'), '')}\n"
                        f"PriorThreadType: {safe_string(prior_type, 'NONE')}\n"
                        f"ThreadContext: {thread_excerpt}"
                    ),
                },
            ],
            temperature=0.0,
            max_tokens=220,
            n=1,
        )
        payload = json.loads(response.choices[0].message.content.strip())
        label = _sanitize_label(payload.get("label"))
        confidence = _safe_float(payload.get("confidence"), 0.0)
        return {
            "label": label,
            "confidence": max(0.0, min(confidence, 1.0)),
            "method": safe_string(payload.get("method"), "unknown"),
            "evidence": payload.get("evidence") if isinstance(payload.get("evidence"), list) else [],
            "abstained": bool(payload.get("abstained", False)),
        }
    except Exception as e:
        logger.warning(f"LLM opportunity-type classification failed for {row.get('notice_id')}: {e}")
        return {
            "label": "UNKNOWN",
            "confidence": 0.0,
            "method": "llm_error",
            "evidence": [],
            "abstained": True,
        }


def _latest_persisted_thread_type(versions: List[Dict[str, Any]]) -> Optional[str]:
    if not versions:
        return None
    ordered = sorted([v for v in versions if isinstance(v, dict)], key=version_sort_key, reverse=True)
    for row in ordered:
        label = _sanitize_label(row.get("opportunity_type"), default="")
        if label and label in CLASSIFICATION_LABELS and label != "UNKNOWN":
            return label
    return None


def _finalize_classification(
    row: Dict[str, Any],
    rule_result: Dict[str, Any],
    llm_result: Optional[Dict[str, Any]],
    prior_type: Optional[str],
) -> Dict[str, Any]:
    rule_label = _sanitize_label(rule_result.get("label"))
    rule_conf = _safe_float(rule_result.get("confidence"), 0.0)
    rule_conflict = bool(rule_result.get("conflict", False))
    low_info = _is_low_information_text(safe_string(row.get("description"), ""))

    if rule_label != "UNKNOWN" and rule_conf >= 0.90 and not rule_conflict:
        chosen = {
            "label": rule_label,
            "confidence": rule_conf,
            "method": rule_result.get("method", "rule_keyword"),
            "evidence": rule_result.get("evidence", []),
            "abstained": False,
            "needs_review": False,
        }
    else:
        llm_label = _sanitize_label((llm_result or {}).get("label"))
        llm_conf = _safe_float((llm_result or {}).get("confidence"), 0.0)
        if llm_label != "UNKNOWN" and llm_conf >= rule_conf:
            chosen = {
                "label": llm_label,
                "confidence": llm_conf,
                "method": f"llm_{safe_string((llm_result or {}).get('method'), 'unknown')}",
                "evidence": (llm_result or {}).get("evidence", []),
                "abstained": bool((llm_result or {}).get("abstained", False)),
                "needs_review": llm_conf < 0.75,
            }
        else:
            chosen = {
                "label": rule_label,
                "confidence": rule_conf,
                "method": rule_result.get("method", "rule_keyword"),
                "evidence": rule_result.get("evidence", []),
                "abstained": rule_label == "UNKNOWN",
                "needs_review": rule_conflict or rule_conf < 0.75,
            }

    if prior_type and chosen["label"] not in {"UNKNOWN", prior_type} and chosen["confidence"] < CLASSIFIER_FLIP_MIN_CONFIDENCE:
        chosen = {
            "label": prior_type,
            "confidence": max(0.66, min(chosen["confidence"], 0.74)),
            "method": "inherited_guarded_transition",
            "evidence": chosen.get("evidence", []),
            "abstained": False,
            "needs_review": True,
        }

    if low_info and prior_type and chosen["label"] == "UNKNOWN":
        chosen = {
            "label": prior_type,
            "confidence": 0.65,
            "method": "inherited_low_information",
            "evidence": ["latest amendment text is low information; inherited prior thread type"],
            "abstained": False,
            "needs_review": True,
        }

    if chosen["confidence"] < CLASSIFIER_MIN_CONFIDENCE:
        chosen["label"] = "UNKNOWN"
        chosen["abstained"] = True
        chosen["needs_review"] = True

    chosen["label"] = _sanitize_label(chosen.get("label"))
    chosen["confidence"] = max(0.0, min(_safe_float(chosen.get("confidence"), 0.0), 1.0))
    return chosen


async def _classify_opportunity_type(
    row: Dict[str, Any],
    thread_context_text: str,
    persisted_versions: List[Dict[str, Any]],
) -> Dict[str, Any]:
    if not CLASSIFIER_ENABLED:
        return {
            "label": "UNKNOWN",
            "confidence": 0.0,
            "method": "disabled",
            "evidence": [],
            "abstained": True,
            "needs_review": False,
        }
    rule_result = _detect_rule_based_type(row)
    llm_result = None
    should_call_llm = (
        rule_result.get("label") == "UNKNOWN"
        or rule_result.get("conflict")
        or _safe_float(rule_result.get("confidence"), 0.0) < 0.90
    )
    prior_type = _latest_persisted_thread_type(persisted_versions)
    if should_call_llm:
        llm_result = await _classify_type_with_llm(row, thread_context_text, prior_type)
    return _finalize_classification(row, rule_result, llm_result, prior_type)


def _resolve_thread_assignment_from_seen(
    seen_rows: List[Dict[str, Any]],
    row: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Resolve thread assignment from rows already processed in this import batch."""
    row_sol = safe_string(row.get("solicitation_number_norm"), "")
    row_dept = safe_string(row.get("department_norm"), "")
    row_title = safe_string(row.get("title_norm"), "")
    row_email = safe_string(row.get("contact_email_norm"), "")
    row_phone = safe_string(row.get("contact_phone_norm"), "")
    row_name = safe_string(row.get("contact_name_norm"), "")
    row_simhash = row.get("metadata_simhash")

    for prev in reversed(seen_rows):
        prev_thread_id = prev.get("thread_id")
        if not prev_thread_id:
            continue
        prev_sol = safe_string(prev.get("solicitation_number_norm"), "")
        prev_dept = safe_string(prev.get("department_norm"), "")
        prev_title = safe_string(prev.get("title_norm"), "")
        prev_email = safe_string(prev.get("contact_email_norm"), "")
        prev_phone = safe_string(prev.get("contact_phone_norm"), "")
        prev_name = safe_string(prev.get("contact_name_norm"), "")
        prev_simhash = prev.get("metadata_simhash")

        if row_sol and prev_sol and row_sol == prev_sol:
            if row_dept and prev_dept and row_dept != prev_dept:
                continue
            return {
                "thread_id": prev_thread_id,
                "match_method": "in_batch_solicitation",
                "match_score": 0.99,
                "matched_to_version_id": None,
                "decision_reason": {"reason": "in_batch_same_solicitation"},
            }

        if row_dept and prev_dept and row_title and prev_title and row_dept == prev_dept and row_title == prev_title:
            contact_match = (
                (row_email and prev_email and row_email == prev_email)
                or (row_phone and prev_phone and row_phone == prev_phone)
                or (row_name and prev_name and row_name == prev_name)
            )
            any_contact_present = bool(
                row_email or row_phone or row_name or prev_email or prev_phone or prev_name
            )
            if contact_match:
                return {
                    "thread_id": prev_thread_id,
                    "match_method": "in_batch_title_department_contact",
                    "match_score": 0.97,
                    "matched_to_version_id": None,
                    "decision_reason": {"reason": "in_batch_same_title_department_contact"},
                }
            if not any_contact_present:
                return {
                    "thread_id": prev_thread_id,
                    "match_method": "in_batch_title_department",
                    "match_score": 0.92,
                    "matched_to_version_id": None,
                    "decision_reason": {"reason": "in_batch_same_title_department_no_contact"},
                }

        sim_dist = _hamming_distance_64(row_simhash, prev_simhash)
        if sim_dist is not None and sim_dist <= 6 and row_dept and prev_dept and row_dept == prev_dept:
            return {
                "thread_id": prev_thread_id,
                "match_method": "in_batch_simhash",
                "match_score": round(1.0 - (sim_dist / 64.0), 5),
                "matched_to_version_id": None,
                "decision_reason": {"reason": "in_batch_simhash_close", "distance": sim_dist},
            }
    return None


def _vector_literal(embedding: Any) -> Optional[str]:
    """Serialize embedding list to pgvector literal format."""
    if not isinstance(embedding, list) or not embedding:
        return None
    try:
        vals = ",".join(f"{float(v):.8f}" for v in embedding)
        return f"[{vals}]"
    except Exception:
        return None


def _resolve_thread_assignment(supabase, row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Resolve thread assignment using Supabase RPC + Python weighted scoring.
    Returns match metadata and thread_id (possibly None when new thread is needed).
    """
    rpc_args = {
        "p_notice_id": row.get("notice_id"),
        "p_solicitation_number_norm": row.get("solicitation_number_norm"),
        "p_title_norm": row.get("title_norm"),
        "p_department_norm": row.get("department_norm"),
        "p_contact_email_norm": row.get("contact_email_norm"),
        "p_contact_phone_norm": row.get("contact_phone_norm"),
        "p_contact_name_norm": row.get("contact_name_norm"),
        "p_metadata_simhash": row.get("metadata_simhash"),
        "p_query_embedding": _vector_literal(row.get("embedding")),
        "p_limit": THREAD_CANDIDATE_LIMIT,
    }
    try:
        resp = _run_with_supabase_retries(
            lambda: supabase.rpc("rpc_find_thread_candidates", rpc_args).execute(),
            label=f"rpc_find_thread_candidates notice={row.get('notice_id')}",
        )
        candidates = getattr(resp, "data", None) or []
    except Exception:
        candidates = []

    if not candidates:
        return {
            "thread_id": None,
            "match_method": "new_thread",
            "match_score": 0.0,
            "matched_to_version_id": None,
            "decision_reason": {"reason": "no_candidate"},
        }

    best = None
    best_score = -1.0
    for c in candidates:
        sql_score = _safe_float(c.get("score"), 0.0)
        sim_distance = c.get("simhash_distance")
        sim_score = 0.0
        if sim_distance is not None:
            sim_score = max(0.0, 1.0 - (int(sim_distance) / 64.0))
        emb_score = _safe_float(c.get("embedding_similarity"), 0.0)
        contact_score = _contact_match_strength(row, c)
        # Prioritize deterministic identifiers and contact consistency.
        py_score = (sql_score * 0.72) + (contact_score * 0.18) + (sim_score * 0.06) + (emb_score * 0.04)
        if py_score > best_score:
            best = c
            best_score = py_score

    if best is None or best_score < THREAD_MEDIUM_CONFIDENCE:
        return {
            "thread_id": None,
            "match_method": "new_thread",
            "match_score": round(best_score, 5),
            "matched_to_version_id": None,
            "decision_reason": {"reason": "score_below_threshold", "score": best_score},
        }

    basis = safe_string(best.get("match_basis"), "hybrid")
    row_sol = safe_string(row.get("solicitation_number_norm"), "")
    row_dept = safe_string(row.get("department_norm"), "")
    row_title = safe_string(row.get("title_norm"), "")
    cand_sol = safe_string(best.get("candidate_solicitation_number_norm"), "")
    cand_dept = safe_string(best.get("candidate_department_norm"), "")
    cand_title = safe_string(best.get("candidate_title_norm"), "")
    same_sol = bool(row_sol and cand_sol and row_sol == cand_sol)
    same_dept = bool(row_dept and cand_dept and row_dept == cand_dept)
    same_title = bool(row_title and cand_title and row_title == cand_title)
    contact_strength = _contact_match_strength(row, best)
    row_has_contact = bool(
        safe_string(row.get("contact_email_norm"), "")
        or safe_string(row.get("contact_phone_norm"), "")
        or safe_string(row.get("contact_name_norm"), "")
    )
    cand_has_contact = bool(
        safe_string(best.get("candidate_contact_email_norm"), "")
        or safe_string(best.get("candidate_contact_phone_norm"), "")
        or safe_string(best.get("candidate_contact_name_norm"), "")
    )

    if basis != "exact_notice_id" and row_sol and cand_sol and not same_sol:
        return {
            "thread_id": None,
            "match_method": "new_thread",
            "match_score": round(best_score, 5),
            "matched_to_version_id": None,
            "decision_reason": {
                "reason": "rejected_solicitation_mismatch",
                "score": best_score,
            },
        }

    if basis in {"title_department_contact", "title_department", "metadata", "embedding", "simhash"}:
        if row_dept and cand_dept and not same_dept:
            return {
                "thread_id": None,
                "match_method": "new_thread",
                "match_score": round(best_score, 5),
                "matched_to_version_id": None,
                "decision_reason": {
                    "reason": "rejected_department_mismatch",
                    "score": best_score,
                },
            }

    if basis == "title_department_contact" and contact_strength <= 0.0:
        return {
            "thread_id": None,
            "match_method": "new_thread",
            "match_score": round(best_score, 5),
            "matched_to_version_id": None,
            "decision_reason": {
                "reason": "rejected_missing_contact_match",
                "score": best_score,
            },
        }

    if basis == "title_department" and row_has_contact and cand_has_contact and contact_strength <= 0.0:
        return {
            "thread_id": None,
            "match_method": "new_thread",
            "match_score": round(best_score, 5),
            "matched_to_version_id": None,
            "decision_reason": {
                "reason": "rejected_title_department_contact_conflict",
                "score": best_score,
            },
        }

    # Title-only fallback should still require department consistency.
    if basis == "title_department" and not (same_title and same_dept):
        return {
            "thread_id": None,
            "match_method": "new_thread",
            "match_score": round(best_score, 5),
            "matched_to_version_id": None,
            "decision_reason": {
                "reason": "rejected_title_department_mismatch",
                "score": best_score,
            },
        }

    method = "hybrid" if basis not in {
        "exact_notice_id",
        "solicitation_number",
        "title_department_contact",
        "title_department",
        "embedding",
        "simhash",
    } else basis
    confidence = "high" if best_score >= THREAD_HIGH_CONFIDENCE else "medium"
    return {
        "thread_id": best.get("thread_id"),
        "match_method": method,
        "match_score": round(best_score, 5),
        "matched_to_version_id": best.get("candidate_version_id"),
        "decision_reason": {
            "reason": "candidate_selected",
            "confidence": confidence,
            "basis": basis,
        },
    }


def _ensure_thread_id(
    supabase,
    row: Dict[str, Any],
    assignment: Dict[str, Any],
    thread_id_cache: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """Get existing thread_id from ai_enhanced_opportunity_versions by thread_key, or generate a new UUID."""
    if assignment.get("thread_id"):
        return assignment["thread_id"]
    thread_key = safe_string(row.get("thread_key")) or f"notice:{row.get('notice_id')}"
    if thread_id_cache and thread_key in thread_id_cache:
        return thread_id_cache[thread_key]
    try:
        q = _run_with_supabase_retries(
            lambda: (
                supabase
                .table("ai_enhanced_opportunity_versions")
                .select("thread_id")
                .eq("thread_key", thread_key)
                .limit(1)
                .execute()
            ),
            label=f"ensure_thread_select notice={row.get('notice_id')}",
        )
        rows = getattr(q, "data", None) or []
        thread_id = rows[0].get("thread_id") if rows else str(uuid.uuid4())
        if thread_id_cache is not None:
            thread_id_cache[thread_key] = thread_id
        return thread_id
    except Exception as e:
        logger.error(f"Failed to ensure thread for {row.get('notice_id')}: {e}")
        # region agent log
        _debug_log(
            run_id="import-run",
            hypothesis_id="H2",
            location="csv_import_sam_gov.py:_ensure_thread_id",
            message="Thread ensure failed",
            data={
                "noticeId": row.get("notice_id"),
                "threadKey": thread_key[:120],
                "errorType": type(e).__name__,
                "errorText": str(e)[:240],
            },
        )
        # endregion
    return str(uuid.uuid4())

def process_csv_row(row: Dict[str, str]) -> Dict[str, Any]:
    """
    Process a single CSV row and convert it to the format expected by the database.
    
    Args:
        row: Dictionary containing CSV row data
        
    Returns:
        Dictionary formatted for database insertion
    """
    # Get NAICS code and convert to integer for database when possible.
    naics = row.get("NaicsCode")
    try:
        naics_code = int(naics) if naics else None
    except (ValueError, TypeError):
        naics_code = None
    
    # Get notice ID
    notice_id = str(row.get("NoticeId", "")).strip()
    if not notice_id:
        return None  # Skip rows without notice ID

    solicitation_number = safe_string(row.get("Sol#"), "")
    # New rows should start as notice-scoped threads; adaptive matching can merge later.
    thread_key = f"notice:{notice_id}"
    
    # Get description and generate AI summary
    description = row.get("Description", "")
    # Ensure description is a valid string
    if pd.isna(description) or not isinstance(description, str):
        description = ""
    
    # Format data to match table schema exactly as in original code
    processed_row = {
        "notice_id": notice_id,
        "solicitation_number": solicitation_number,
        "thread_key": thread_key,
        "thread_key_version": 1,
        "title": truncate_string(safe_string(row.get("Title"), "No title")),
        "department": safe_string(row.get("Department/Ind.Agency"), "").split(".")[0] if safe_string(row.get("Department/Ind.Agency")) else "",
        "naics_code": naics_code,
        "published_date": parse_date(row.get("PostedDate")),
        "source_posted_at": parse_datetime(row.get("PostedDate")),
        "source_archive_type": safe_string(row.get("ArchiveType"), ""),
        "source_archive_date": parse_date(row.get("ArchiveDate")),
        "source_notice_type": safe_string(row.get("Type"), ""),
        "source_base_type": safe_string(row.get("BaseType"), ""),
        "response_date": parse_date(row.get("ResponseDeadLine")),
        "description": description,
        "classification_code": safe_string(row.get("ClassificationCode"), ""),
        "url": f"https://sam.gov/opp/{notice_id}/view" if notice_id else None,
        "point_of_contact": json.dumps({
            "primary": {
                "title": safe_string(row.get("PrimaryContactTitle"), ""),
                "name": safe_string(row.get("PrimaryContactFullname"), ""),
                "email": safe_string(row.get("PrimaryContactEmail"), ""),
                "phone": safe_string(row.get("PrimaryContactPhone"), ""),
                "fax": safe_string(row.get("PrimaryContactFax"), "")
            },
            "secondary": {
                "title": safe_string(row.get("SecondaryContactTitle"), ""),
                "name": safe_string(row.get("SecondaryContactFullname"), ""),
                "email": safe_string(row.get("SecondaryContactEmail"), ""),
                "phone": safe_string(row.get("SecondaryContactPhone"), ""),
                "fax": safe_string(row.get("SecondaryContactFax"), "")
            }
        }),
        "active": True if safe_string(row.get("Active", "Yes")).strip().lower() == "yes" else False,
        "sub_departments": safe_string(row.get("Sub-Tier"), ""),
        "objective": "",
        "expected_outcome": "",
        "eligibility": "",
        "key_facts": "",
        "due_date": None,
        "funding": ""
    }

    return _prepare_matching_signals(processed_row)

async def process_csv_file(
    csv_file_path: str,
    batch_size: int = CSV_BATCH_SIZE,
    import_limit: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Process the CSV file and update the database with opportunities.
    
    Args:
        csv_file_path: Path to the CSV file
        batch_size: Number of rows to process in each batch
        
    Returns:
        Dictionary with results summary
    """
    if not csv_file_path or not isinstance(csv_file_path, str):
        return {"source": "csv_import", "count": 0, "error": "CSV file path must be a non-empty string"}
    if not os.path.exists(csv_file_path):
        return {"source": "csv_import", "count": 0, "error": f"CSV file not found: {csv_file_path}"}
    
    # logger.info(f"Starting CSV import from: {csv_file_path}")
    
    all_opportunities = []
    total_processed = 0
    
    try:
        # Try different encodings
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        df = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(
                    csv_file_path,
                    encoding=encoding,
                    low_memory=False,
                    usecols=lambda c: c in CSV_REQUIRED_COLUMNS,
                )
                logger.info(f"Successfully read CSV with {encoding} encoding; rows={len(df)}")
                break
            except UnicodeDecodeError:
                continue
            except ValueError:
                # Fallback for schema/column surprises.
                df = pd.read_csv(csv_file_path, encoding=encoding, low_memory=False)
                logger.warning(f"CSV columns differed from expected; loaded full schema with {encoding}")
                break
        
        if df is None:
            return {"source": "csv_import", "count": 0, "error": "Could not read CSV file with any encoding"}
        
        if import_limit is not None and import_limit > 0:
            df = df.head(import_limit).copy()
        elif import_limit is not None and import_limit <= 0:
            return {"source": "csv_import", "count": 0, "error": f"Invalid limit: {import_limit}. Use positive integer or omit."}
        logger.info(f"CSV loaded for processing: {len(df)} rows (limit={import_limit})")
        # region agent log
        _debug_log(
            run_id="import-run",
            hypothesis_id="H5",
            location="csv_import_sam_gov.py:process_csv_file",
            message="CSV loaded and parsing begins",
            data={"rowCount": len(df), "batchSize": batch_size, "limit": import_limit},
        )
        # endregion
        
        # Process rows in batches
        for start_idx in range(0, len(df), batch_size):
            end_idx = min(start_idx + batch_size, len(df))
            batch_df = df.iloc[start_idx:end_idx]
            
            # logger.info(f"Processing batch {start_idx//batch_size + 1}: rows {start_idx+1}-{end_idx}")
            
            batch_opportunities = []
            for row_dict in batch_df.to_dict(orient="records"):
                processed_row = process_csv_row(row_dict)
                if processed_row:
                    batch_opportunities.append(processed_row)
                    total_processed += 1
            
            all_opportunities.extend(batch_opportunities)
            # logger.info(f"Batch complete: {len(batch_opportunities)} opportunities found, {total_processed} total processed")
        
        # logger.info(f"CSV processing complete: {total_processed} opportunities found out of {len(df)} total rows")
        
        # Provisional thread assignment before summary generation (metadata/simhash stage).
        supabase_for_versions = get_supabase_connection(use_service_key=True)
        provisional_seen_rows: List[Dict[str, Any]] = []
        provisional_rpc_calls = 0
        provisional_in_batch_hits = 0
        for idx, opp in enumerate(all_opportunities, start=1):
            supabase_for_versions = _maybe_rotate_supabase_client(supabase_for_versions, idx)
            assignment = _resolve_thread_assignment_from_seen(provisional_seen_rows, opp)
            if assignment is None:
                # Avoid expensive per-row RPC in provisional phase; final assignment still happens in insert_data.
                assignment = {
                    "thread_id": None,
                    "match_method": "provisional_new_thread",
                    "match_score": 0.0,
                    "matched_to_version_id": None,
                    "decision_reason": {"reason": "provisional_rpc_skipped"},
                }
            else:
                provisional_in_batch_hits += 1
            opp["provisional_assignment"] = assignment
            provisional_thread_id = assignment.get("thread_id")
            if provisional_thread_id:
                opp["thread_id"] = provisional_thread_id
            opp["provisional_match_method"] = assignment.get("match_method")
            opp["provisional_match_score"] = assignment.get("match_score")
            provisional_seen_rows.append({
                "thread_id": provisional_thread_id,
                "solicitation_number_norm": opp.get("solicitation_number_norm"),
                "department_norm": opp.get("department_norm"),
                "title_norm": opp.get("title_norm"),
                "contact_email_norm": opp.get("contact_email_norm"),
                "contact_phone_norm": opp.get("contact_phone_norm"),
                "contact_name_norm": opp.get("contact_name_norm"),
                "metadata_simhash": opp.get("metadata_simhash"),
            })
            if idx <= 3:
                # region agent log
                _debug_log(
                    run_id="import-run",
                    hypothesis_id="H6",
                    location="csv_import_sam_gov.py:process_csv_file",
                    message="Provisional assignment sample row",
                    data={
                        "processed": idx,
                        "rpcCalls": provisional_rpc_calls,
                        "inBatchHits": provisional_in_batch_hits,
                        "matchMethod": assignment.get("match_method"),
                    },
                )
                # endregion
            if idx % 500 == 0:
                # region agent log
                _debug_log(
                    run_id="import-run",
                    hypothesis_id="H6",
                    location="csv_import_sam_gov.py:process_csv_file",
                    message="Provisional assignment progress",
                    data={
                        "processed": idx,
                        "total": len(all_opportunities),
                        "rpcCalls": provisional_rpc_calls,
                        "inBatchHits": provisional_in_batch_hits,
                    },
                )
                # endregion
        # region agent log
        _debug_log(
            run_id="import-run",
            hypothesis_id="H6",
            location="csv_import_sam_gov.py:process_csv_file",
            message="Provisional assignment completed",
            data={
                "total": len(all_opportunities),
                "rpcCalls": provisional_rpc_calls,
                "inBatchHits": provisional_in_batch_hits,
            },
        )
        # endregion

        # Prepare thread groups once for thread-aware summaries.
        thread_groups: Dict[str, List[Dict[str, Any]]] = {}
        for opp in all_opportunities:
            group_key = opp.get("thread_id") or opp.get("thread_key") or opp.get("notice_id")
            thread_groups.setdefault(group_key, []).append(opp)

        for group_key, values in thread_groups.items():
            values.sort(key=version_sort_key, reverse=True)

        group_keys = [safe_string(k, "") for k in thread_groups.keys() if safe_string(k, "")]
        thread_ids_for_fetch = [k for k in group_keys if _looks_like_uuid(k)]
        thread_keys_for_fetch = [k for k in group_keys if not _looks_like_uuid(k)]
        persisted_thread_cache_by_id = _fetch_versions_by_thread_ids_sb(
            supabase_for_versions,
            thread_ids_for_fetch,
            per_thread_limit=VERSIONS_PER_THREAD_FOR_SUMMARY,
        )
        persisted_thread_cache_by_key = _fetch_versions_by_thread_keys_sb(
            supabase_for_versions,
            thread_keys_for_fetch,
            per_thread_limit=VERSIONS_PER_THREAD_FOR_SUMMARY,
        )

        # Generate AI summaries using all thread versions (latest prioritized).
        logger.info("Generating AI summaries for opportunities...")
        sem = asyncio.Semaphore(SUMMARY_CONCURRENCY)
        classifier_sem = asyncio.Semaphore(CLASSIFIER_CONCURRENCY)
        summary_total = len(all_opportunities)
        summary_done = 0
        summary_last_pct = -1

        async def _enrich_opportunity(opp: Dict[str, Any]):
            nonlocal summary_done, summary_last_pct
            try:
                group_key = opp.get("thread_id") or opp.get("thread_key") or opp.get("notice_id")
                persisted_versions = persisted_thread_cache_by_id.get(group_key, [])
                if not persisted_versions:
                    persisted_versions = persisted_thread_cache_by_key.get(group_key, [])
                description_text = build_prioritized_thread_description(
                    current_opportunity=opp,
                    persisted_versions=persisted_versions,
                    in_memory_versions=thread_groups.get(group_key, []),
                    max_versions=25,
                )
                async with sem:
                    if description_text and not pd.isna(description_text) and isinstance(description_text, str):
                        summary_resp = await generate_description_summary("description: " + description_text)
                        summary = summary_resp.get("summary", {}) if isinstance(summary_resp, dict) else {}
                        opp["objective"] = summary.get("objective", "")
                        opp["expected_outcome"] = summary.get("goal", "")
                        opp["eligibility"] = summary.get("eligibility", "")
                        opp["key_facts"] = summary.get("key_facts", "")
                        due_date_str = summary.get("due_date", "")
                        if due_date_str and not pd.isna(due_date_str) and isinstance(due_date_str, str):
                            opp["due_date"] = parse_date(due_date_str)
                        else:
                            opp["due_date"] = None
                        opp["funding"] = summary.get("budget", "")
                    else:
                        opp["objective"] = ""
                        opp["expected_outcome"] = ""
                        opp["eligibility"] = ""
                        opp["key_facts"] = ""
                        opp["due_date"] = None
                        opp["funding"] = ""

                async with classifier_sem:
                    classification = await _classify_opportunity_type(
                        row=opp,
                        thread_context_text=description_text,
                        persisted_versions=persisted_versions,
                    )
                opp["opportunity_type"] = classification.get("label", "UNKNOWN")
                opp["opportunity_type_confidence"] = classification.get("confidence", 0.0)
                opp["opportunity_type_method"] = classification.get("method", "unknown")
                opp["opportunity_type_evidence"] = {
                    "evidence": classification.get("evidence", []),
                    "notice_type": opp.get("source_notice_type"),
                    "base_type": opp.get("source_base_type"),
                }
                opp["opportunity_type_abstained"] = bool(classification.get("abstained", False))
                opp["opportunity_type_needs_review"] = bool(classification.get("needs_review", False))
                opp["classification_model"] = CLASSIFIER_MODEL
                opp["classification_prompt_version"] = CLASSIFIER_PROMPT_VERSION
                opp["classified_at"] = datetime.utcnow()

                text_for_embedding = build_embedding_text_full_row(opp)
                if text_for_embedding.strip():
                    opp["embedding_text"] = text_for_embedding
                    opp["embedding"] = await generate_embedding(text_for_embedding)
                    opp["embedding_model"] = EMBED_MODEL
                    opp["embedding_version"] = 1
            except Exception as e:
                opp["objective"] = ""
                opp["expected_outcome"] = ""
                opp["eligibility"] = ""
                opp["key_facts"] = ""
                opp["due_date"] = None
                opp["funding"] = ""
                logger.error(f"Error enriching opportunity {opp.get('notice_id')}: {e}")
            finally:
                summary_done += 1
                pct = int((summary_done / summary_total) * 100) if summary_total else 100
                if pct != summary_last_pct:
                    _progress_line("Summary/embedding progress", summary_done, summary_total)
                    summary_last_pct = pct

        await asyncio.gather(*(_enrich_opportunity(opp) for opp in all_opportunities))
        # region agent log
        _debug_log(
            run_id="import-run",
            hypothesis_id="H5",
            location="csv_import_sam_gov.py:process_csv_file",
            message="Enrichment phase completed",
            data={"opportunityCount": len(all_opportunities), "summaryConcurrency": SUMMARY_CONCURRENCY},
        )
        # endregion
        
        # Insert into database
        if all_opportunities:
            # logger.info(f"Preparing to insert {len(all_opportunities)} opportunities into database")
            result = insert_data(all_opportunities)
            # region agent log
            _debug_log(
                run_id="import-run",
                hypothesis_id="H5",
                location="csv_import_sam_gov.py:process_csv_file",
                message="Insert result returned to process_csv_file",
                data={
                    "inserted": result.get("inserted", 0),
                    "updated": result.get("updated", 0),
                    "skipped": result.get("skipped", 0),
                    "hasError": bool(result.get("error")),
                },
            )
            # endregion
            
            # Return detailed results exactly as in original code
            db_results = {
                "source": "csv_import", 
                "total_fetched": total_processed,
                "processed": len(all_opportunities),
                "inserted": result.get("inserted", 0),
                "skipped": result.get("skipped", 0),
                "error": result.get("error")
            }
            
            # Supabase vector refresh: fill embeddings for latest versions missing them.
            try:
                supabase = get_supabase_connection(use_service_key=True)
                PAGE = 200
                total_refreshed = 0
                while True:
                    q = (
                        supabase
                        .table("ai_enhanced_opportunity_versions")
                        .select(
                            "version_id, notice_id, title, department, naics_code, description, url, "
                            "objective, expected_outcome, eligibility, key_facts, response_date, due_date, "
                            "sub_departments, funding, point_of_contact, published_date, active, embedding"
                        )
                        .eq("is_latest_in_thread", True)
                        .is_("embedding", None)
                        .limit(PAGE)
                    ).execute()
                    rows = getattr(q, "data", None) or []
                    if not rows:
                        break

                    sem_index = asyncio.Semaphore(INDEXING_EMBED_CONCURRENCY)

                    async def _build_index_update(r):
                        async with sem_index:
                            row_full = {
                                "notice_id": r.get("notice_id"),
                                "title": r.get("title"),
                                "department": r.get("department"),
                                "naics_code": r.get("naics_code"),
                                "description": r.get("description"),
                                "url": r.get("url"),
                                "objective": r.get("objective"),
                                "expected_outcome": r.get("expected_outcome"),
                                "eligibility": r.get("eligibility"),
                                "key_facts": r.get("key_facts"),
                                "response_date": r.get("response_date"),
                                "due_date": r.get("due_date"),
                                "sub_departments": r.get("sub_departments"),
                                "funding": r.get("funding"),
                                "point_of_contact": r.get("point_of_contact"),
                                "published_date": r.get("published_date"),
                                "active": r.get("active"),
                            }
                            full_text = build_embedding_text_full_row(row_full)
                            if not full_text.strip():
                                return None
                            emb = await generate_embedding(full_text)
                            return {
                                "version_id": r["version_id"],
                                "embedding_text": full_text[:20000],
                                "embedding": emb,
                                "embedding_model": EMBED_MODEL,
                                "embedding_version": 1
                            }

                    raw_updates = await asyncio.gather(*(_build_index_update(r) for r in rows), return_exceptions=True)
                    updates = []
                    for u in raw_updates:
                        if isinstance(u, Exception):
                            continue
                        if u:
                            updates.append(u)

                    if updates:
                        supabase.table("ai_enhanced_opportunity_versions").upsert(updates, on_conflict="version_id").execute()
                        total_refreshed += len(updates)

                db_results["indexed_count"] = total_refreshed
                # logger.info(f"Supabase vector refresh complete. Embeddings updated: {total_refreshed}")
            except Exception as e:
                # logger.error(f"Error during Supabase vector refresh: {e}")
                db_results["indexing_error"] = str(e)

            # Only perform inactive marking on full imports.
            if import_limit is None:
                try:
                    latest_notice_ids = set(row["notice_id"] for row in all_opportunities if row.get("notice_id"))
                    supabase = get_supabase_connection(use_service_key=True)
                    if latest_notice_ids:
                        active_resp = (
                            supabase
                            .table("ai_enhanced_opportunity_versions")
                            .select("notice_id")
                            .eq("is_latest_in_thread", True)
                            .eq("active", True)
                            .execute()
                        )
                        active_rows = getattr(active_resp, "data", None) or []
                        active_ids = {r["notice_id"] for r in active_rows if r.get("notice_id")}
                        to_deactivate = list(active_ids - latest_notice_ids)
                        marked_inactive = 0
                        if to_deactivate:
                            CHUNK = 500
                            for i in range(0, len(to_deactivate), CHUNK):
                                chunk = to_deactivate[i:i+CHUNK]
                                supabase.table("ai_enhanced_opportunity_versions") \
                                    .update({"active": False}) \
                                    .in_("notice_id", chunk) \
                                    .eq("is_latest_in_thread", True).execute()
                                marked_inactive += len(chunk)
                        db_results["marked_inactive"] = marked_inactive
                    else:
                        logger.warning("No notice_ids found in latest CSV fetch for inactive marking step.")
                except Exception as e:
                    db_results["inactive_marking_error"] = str(e)
            else:
                db_results["marked_inactive"] = "skipped_partial_import"
                
            return db_results
        
        return {"source": "csv_import", "count": 0, "status": "No opportunities found"}
        
    except Exception as e:
        logger.error(f"Error processing CSV file: {e}")
        return {"source": "csv_import", "count": 0, "error": str(e)}

async def import_from_csv(csv_file_path: str, import_limit: Optional[int] = None) -> Dict[str, Any]:
    """
    Main function to import opportunities from CSV file.
    
    Args:
        csv_file_path: Path to the CSV file.
        
    Returns:
        Dictionary with results summary
    """
    if not csv_file_path:
        return {"source": "csv_import", "count": 0, "error": "csv_file_path is required"}

    # If a directory path was provided, append the default filename
    if os.path.isdir(csv_file_path):
        csv_file_path = os.path.join(csv_file_path, "ContractOpportunitiesFullCSV.csv")
    
    # logger.info(f"Starting CSV import process from: {csv_file_path}")
    
    # Process the CSV file
    if import_limit is None:
        env_limit = env_str("CSV_IMPORT_LIMIT_BIZ", "", legacy_names=("CSV_IMPORT_LIMIT",))
        if env_limit not in (None, "", "None", "none", "null"):
            try:
                import_limit = int(env_limit)
            except ValueError:
                import_limit = None
    elif import_limit <= 0:
        return {"source": "csv_import", "count": 0, "error": f"Invalid limit: {import_limit}. Use positive integer or omit."}

    result = await process_csv_file(csv_file_path, import_limit=import_limit)
    
    # logger.info(f"CSV import process complete: {result}")
    return result

# Function to handle command line arguments (exactly as in original)
def parse_args():
    """Parse command line arguments"""
    import argparse
    parser = argparse.ArgumentParser(description='CSV import script for SAM.gov opportunities')
    parser.add_argument('--record-id', type=int, help='ETL record ID')
    parser.add_argument('--trigger-type', type=str, help='Trigger type (scheduled or manual)')
    parser.add_argument('--csv-path', type=str, required=True, help='Path to CSV file')
    parser.add_argument('--limit', type=int, help='Import only first N CSV rows; omit for full import')
    return parser.parse_args()

# For running as a script (exactly as in original)
if __name__ == "__main__":
    # Parse command line arguments
    args = parse_args()
    
    if args.record_id:
        logger.info(f"Running with ETL record ID: {args.record_id}, trigger type: {args.trigger_type}")
    
    # Run the async function
    result = asyncio.run(import_from_csv(args.csv_path, args.limit))
    
    # Calculate counts for output (exactly as in original)
    count = result.get("total_fetched", 0)
    new_count = result.get("inserted", 0) 
    status = "error" if result.get("error") else "success"
    
    # Output in JSON format for the GitHub workflow (exactly as in original)
    output = {
        "count": count,
        "new_count": new_count, 
        "status": status
    }
    
    print(output)
