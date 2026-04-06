import argparse
import concurrent.futures
import csv
import json
import math
import os
import re
import threading
import time
from collections import deque
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, TypedDict

from sam_gov.utils.logger import get_logger
from sam_gov.utils.db_utils import get_db_connection
from sam_gov.utils.openai_client import get_openai_client
from sam_gov.config.settings import AZURE_OPENAI_EMBEDDING_MODEL, env_int, env_str


try:
    from langgraph.graph import END, StateGraph  # type: ignore

    HAS_LANGGRAPH = True
except Exception:
    END = None
    StateGraph = None
    HAS_LANGGRAPH = False


logger = get_logger(__name__)

_env_int_local = env_int
_env_str_local = env_str


DEFAULT_TABLE = _env_str_local("CSV_EMBED_TARGET_TABLE_BIZ", "all_opportunities", legacy_names=("CSV_EMBED_TARGET_TABLE",))
DEFAULT_FALLBACK_TABLE = _env_str_local(
    "CSV_EMBED_FALLBACK_TABLE_BIZ",
    "sam_opportunities",
    legacy_names=("CSV_EMBED_FALLBACK_TABLE",),
)
DEFAULT_EMBED_MODEL = (
    _env_str_local("CSV_EMBED_MODEL_BIZ", "", legacy_names=("CSV_EMBED_MODEL",))
    or AZURE_OPENAI_EMBEDDING_MODEL
    or "text-embedding-3-small"
)

# Large-file defaults tuned for 250MB+ CSV workloads.
DEFAULT_ROW_BATCH_SIZE = _env_int_local("CSV_EMBED_ROW_BATCH_SIZE_BIZ", 200, legacy_names=("CSV_EMBED_ROW_BATCH_SIZE",))
DEFAULT_EMBED_BATCH_SIZE = _env_int_local("CSV_EMBED_OPENAI_BATCH_SIZE_BIZ", 64, legacy_names=("CSV_EMBED_OPENAI_BATCH_SIZE",))
DEFAULT_UPSERT_BATCH_SIZE = _env_int_local("CSV_EMBED_UPSERT_BATCH_SIZE_BIZ", 200, legacy_names=("CSV_EMBED_UPSERT_BATCH_SIZE",))
DEFAULT_EMBED_TEXT_LIMIT = _env_int_local("CSV_EMBED_TEXT_LIMIT_BIZ", 20000, legacy_names=("CSV_EMBED_TEXT_LIMIT",))
DEFAULT_SEARCH_EXPANSIONS = _env_int_local("CSV_SEARCH_EXPANSIONS_BIZ", 4, legacy_names=("CSV_SEARCH_EXPANSIONS",))
DEFAULT_SEARCH_WORKERS = _env_int_local("CSV_SEARCH_WORKERS_BIZ", 4, legacy_names=("CSV_SEARCH_WORKERS",))
DEFAULT_LATENCY_BUDGET_MS = _env_int_local(
    "CSV_SEARCH_LATENCY_BUDGET_MS_BIZ",
    5000,
    legacy_names=("CSV_SEARCH_LATENCY_BUDGET_MS",),
)
DEFAULT_SEARCH_MODEL = _env_str_local("CSV_SEARCH_MODEL_BIZ", "gpt-4.1-mini", legacy_names=("CSV_SEARCH_MODEL",))
OPENAI_TPM_LIMIT = _env_int_local("OPENAI_TPM_LIMIT_BIZ", 120000, legacy_names=("OPENAI_TPM_LIMIT",))
OPENAI_RPM_LIMIT = _env_int_local("OPENAI_RPM_LIMIT_BIZ", 720, legacy_names=("OPENAI_RPM_LIMIT",))
DEFAULT_RPCS = tuple(
    p.strip()
    for p in _env_str_local(
        "CSV_EMBED_MATCH_RPCS_BIZ",
        "match_all_opportunities,match_ai_enhanced_opps_all,match_ai_enhanced_opps",
        legacy_names=("CSV_EMBED_MATCH_RPCS",),
    ).split(",")
    if p.strip()
)

EMBEDDING_FIELD_NAMES = {"embedding", "embedding_text", "embedding_model", "embedding_version"}

CSV_HEADER_TO_DB_COLUMN = {
    "NoticeId": "notice_id",
    "Title": "title",
    "Sol#": "solicitation_number",
    "Department/Ind.Agency": "department_ind_agency",
    "CGAC": "cgac",
    "Sub-Tier": "sub_tier",
    "FPDS Code": "fpds_code",
    "Office": "office",
    "AAC Code": "aac_code",
    "PostedDate": "posted_date",
    "Type": "notice_type",
    "BaseType": "base_type",
    "ArchiveType": "archive_type",
    "ArchiveDate": "archive_date",
    "SetASideCode": "set_aside_code",
    "SetASide": "set_aside",
    "ResponseDeadLine": "response_deadline",
    "NaicsCode": "naics_code",
    "ClassificationCode": "classification_code",
    "PopStreetAddress": "pop_street_address",
    "PopCity": "pop_city",
    "PopState": "pop_state",
    "PopZip": "pop_zip",
    "PopCountry": "pop_country",
    "Active": "active",
    "AwardNumber": "award_number",
    "AwardDate": "award_date",
    "Award$": "award_amount",
    "Awardee": "awardee",
    "PrimaryContactTitle": "primary_contact_title",
    "PrimaryContactFullname": "primary_contact_fullname",
    "PrimaryContactEmail": "primary_contact_email",
    "PrimaryContactPhone": "primary_contact_phone",
    "PrimaryContactFax": "primary_contact_fax",
    "SecondaryContactTitle": "secondary_contact_title",
    "SecondaryContactFullname": "secondary_contact_fullname",
    "SecondaryContactEmail": "secondary_contact_email",
    "SecondaryContactPhone": "secondary_contact_phone",
    "SecondaryContactFax": "secondary_contact_fax",
    "OrganizationType": "organization_type",
    "State": "state",
    "City": "city",
    "ZipCode": "zip_code",
    "CountryCode": "country_code",
    "AdditionalInfoLink": "additional_info_link",
    "Link": "link",
    "Description": "description",
}

REQUIRED_INPUT_HEADERS = tuple(CSV_HEADER_TO_DB_COLUMN.keys())


class OpenAIRateLimiter:
    """
    Thread-safe sliding-window limiter for OpenAI RPM and TPM limits.
    """

    def __init__(self, requests_per_minute: int, tokens_per_minute: int):
        self.requests_per_minute = max(1, int(requests_per_minute))
        self.tokens_per_minute = max(1, int(tokens_per_minute))
        self._lock = threading.Lock()
        self._request_times: deque = deque()
        self._token_events: deque = deque()
        self._token_sum = 0

    def _prune(self, now: float) -> None:
        cutoff = now - 60.0
        while self._request_times and self._request_times[0] <= cutoff:
            self._request_times.popleft()
        while self._token_events and self._token_events[0][0] <= cutoff:
            _, old_tokens = self._token_events.popleft()
            self._token_sum -= old_tokens
            if self._token_sum < 0:
                self._token_sum = 0

    def acquire(self, token_cost: int, request_cost: int = 1) -> None:
        token_cost = max(0, int(token_cost))
        request_cost = max(1, int(request_cost))
        while True:
            wait_seconds = 0.0
            with self._lock:
                now = time.time()
                self._prune(now)

                # Request headroom.
                req_over = (len(self._request_times) + request_cost) - self.requests_per_minute
                if req_over > 0 and len(self._request_times) >= req_over:
                    req_wait_until = self._request_times[req_over - 1] + 60.0
                    wait_seconds = max(wait_seconds, req_wait_until - now)

                # Token headroom.
                if token_cost > self.tokens_per_minute:
                    # Single-call token estimate should not exceed full minute budget.
                    token_cost = self.tokens_per_minute
                token_over = (self._token_sum + token_cost) - self.tokens_per_minute
                if token_over > 0 and self._token_events:
                    running = 0
                    token_wait_until = None
                    for ts, tok in self._token_events:
                        running += tok
                        if running >= token_over:
                            token_wait_until = ts + 60.0
                            break
                    if token_wait_until is not None:
                        wait_seconds = max(wait_seconds, token_wait_until - now)

                if wait_seconds <= 0:
                    for _ in range(request_cost):
                        self._request_times.append(now)
                    if token_cost > 0:
                        self._token_events.append((now, token_cost))
                        self._token_sum += token_cost
                    return

            time.sleep(min(wait_seconds, 1.0))


_OPENAI_RATE_LIMITER = OpenAIRateLimiter(
    requests_per_minute=OPENAI_RPM_LIMIT,
    tokens_per_minute=OPENAI_TPM_LIMIT,
)


def _estimate_tokens_for_text(text: Any) -> int:
    # Conservative approximate token estimate for throttling.
    return max(1, math.ceil(len(_safe_text(text)) / 4))


def _estimate_tokens_for_messages(messages: Sequence[Dict[str, Any]], max_tokens: int = 0) -> int:
    total = 0
    for message in messages:
        total += _estimate_tokens_for_text(message.get("role"))
        total += _estimate_tokens_for_text(message.get("content"))
        total += 6
    return total + max(0, int(max_tokens)) + 32


def _openai_embeddings_create(
    client: Any,
    *,
    model: str,
    input_texts: Sequence[str],
    rate_limiter: OpenAIRateLimiter,
) -> Any:
    estimated = sum(_estimate_tokens_for_text(t) for t in input_texts) + 8
    rate_limiter.acquire(token_cost=estimated, request_cost=1)
    return client.embeddings.create(model=model, input=list(input_texts))


def _openai_chat_create(
    client: Any,
    *,
    model: str,
    messages: Sequence[Dict[str, Any]],
    temperature: float,
    max_tokens: int,
    rate_limiter: OpenAIRateLimiter,
) -> Any:
    estimated = _estimate_tokens_for_messages(messages, max_tokens=max_tokens)
    rate_limiter.acquire(token_cost=estimated, request_cost=1)
    return client.chat.completions.create(
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        messages=list(messages),
    )


class HybridSearchState(TypedDict, total=False):
    start_time: float
    latency_budget_ms: int
    query: str
    query_intent: str
    k: int
    search_expansions: int
    search_workers: int
    only_active: bool
    table_name: str
    openai_client: Any
    supabase: Any
    embed_model: str
    search_model: str
    rate_limiter: Any
    rpc_candidates: List[str]
    query_embedding: List[float]
    search_texts: List[str]
    search_embeddings: List[List[float]]
    keyword_rows: List[Dict[str, Any]]
    vector_rows: List[Dict[str, Any]]
    all_rows: List[Dict[str, Any]]
    unique_rows: List[Dict[str, Any]]
    output_fields: List[str]
    requested_schema: Dict[str, Any]
    results: List[Dict[str, Any]]


def _normalize_table_name(table_name: str) -> str:
    name = (table_name or "").strip()
    if name.lower().startswith("public."):
        return name.split(".", 1)[1]
    return name


def _to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _extract_json_dict(text: str) -> Dict[str, Any]:
    candidate = _safe_text(text)
    if not candidate:
        return {}
    if "```" in candidate:
        # Handle fenced JSON responses.
        chunks = candidate.split("```")
        for chunk in chunks:
            chunk = chunk.strip()
            if not chunk:
                continue
            if chunk.lower().startswith("json"):
                chunk = chunk[4:].strip()
            try:
                parsed = json.loads(chunk)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                continue
    try:
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    return {}


def _extract_first_json_object(text: str) -> Optional[Dict[str, Any]]:
    s = _safe_text(text)
    if not s:
        return None
    start = s.find("{")
    if start < 0:
        return None
    depth = 0
    for i in range(start, len(s)):
        ch = s[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                candidate = s[start : i + 1]
                try:
                    parsed = json.loads(candidate)
                    if isinstance(parsed, dict):
                        return parsed
                except Exception:
                    return None
    return None


def _extract_requested_schema_from_query(query: str) -> Tuple[str, Optional[Dict[str, Any]]]:
    schema = _extract_first_json_object(query)
    if not isinstance(schema, dict):
        return query, None
    # Only treat this as schema if it looks like field template object.
    schema_hint_keys = {"title", "notice_id", "description", "point_of_contact", "naics_code", "solicitation"}
    if not any(k in schema for k in schema_hint_keys):
        return query, None
    schema_text = json.dumps(schema)
    clean_query = query.replace(schema_text, " ").strip()
    clean_query = re.sub(r"\s+", " ", clean_query)
    if not clean_query:
        clean_query = query
    return clean_query, schema


def _parse_iso_date(value: Any) -> Optional[datetime]:
    text = _safe_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        # date-only fallback
        try:
            return datetime.fromisoformat(text.split("T")[0])
        except Exception:
            return None


def _build_default_schema_template() -> Dict[str, Any]:
    return {
        "title": "",
        "notice_id": "",
        "description": "",
        "sponsor": "",
        "objective": "",
        "key_facts": "",
        "eligibility": "",
        "expected_outcome": "",
        "point_of_contact": {
            "name": "",
            "contact": "",
        },
        "published_date": "",
        "due_date": "",
        "response_in_days": "",
        "naics_code": "",
        "solicitation": "",
        "funding": "",
    }


def _value_for_schema_path(path: List[str], row: Dict[str, Any], rank: int, score: float) -> Any:
    key = path[-1]
    parent = path[-2] if len(path) > 1 else ""
    if key == "rank":
        return rank
    if key == "relevance_score":
        return score

    # Nested point of contact block.
    if parent == "point_of_contact":
        primary_name = _safe_text(row.get("primary_contact_fullname"))
        secondary_name = _safe_text(row.get("secondary_contact_fullname"))
        primary_email = _safe_text(row.get("primary_contact_email"))
        secondary_email = _safe_text(row.get("secondary_contact_email"))
        primary_phone = _safe_text(row.get("primary_contact_phone"))
        secondary_phone = _safe_text(row.get("secondary_contact_phone"))
        contact_name = primary_name or secondary_name
        contact_value = primary_email or primary_phone or secondary_email or secondary_phone
        if key == "name":
            return contact_name or None
        if key == "contact":
            return contact_value or None
        return None

    if key == "title":
        return row.get("title")
    if key == "notice_id":
        return row.get("notice_id")
    if key == "description":
        return row.get("description")
    if key == "sponsor":
        return row.get("department_ind_agency")
    if key == "objective":
        return row.get("objective")
    if key == "key_facts":
        return row.get("key_facts")
    if key == "eligibility":
        return row.get("eligibility")
    if key == "expected_outcome":
        return row.get("expected_outcome")
    if key == "published_date":
        return row.get("posted_date")
    if key in {"due_date", "response_deadline"}:
        return row.get("response_deadline")
    if key == "response_in_days":
        due = _parse_iso_date(row.get("response_deadline"))
        if due is None:
            return None
        delta = (due.date() - datetime.utcnow().date()).days
        return max(delta, 0)
    if key == "naics_code":
        return row.get("naics_code")
    if key in {"solicitation", "solicitation_number"}:
        return row.get("solicitation_number")
    if key == "funding":
        return row.get("award_amount")

    return row.get(key)


def _shape_with_schema(template: Any, row: Dict[str, Any], rank: int, score: float, path: Optional[List[str]] = None) -> Any:
    current_path = path or []
    if isinstance(template, dict):
        shaped: Dict[str, Any] = {}
        for k, v in template.items():
            shaped[k] = _shape_with_schema(v, row, rank, score, current_path + [k])
        return shaped
    if isinstance(template, list):
        return []
    return _value_for_schema_path(current_path, row, rank, score)


def _coerce_csv_value(value: Any) -> Any:
    text = _safe_text(value)
    return text if text != "" else None


def _parse_csv_row(row: Dict[str, Any], text_limit: int, include_embeddings: bool = True) -> Dict[str, Any]:
    missing = [header for header in REQUIRED_INPUT_HEADERS if header not in row]
    if missing:
        raise ValueError(
            f"CSV is missing required headers: {missing}. "
            "Expected the full column set for all_opportunities ingest."
        )

    mapped_fields: Dict[str, Any] = {}
    embedding_text_fields: List[str] = []
    for csv_header, db_column in CSV_HEADER_TO_DB_COLUMN.items():
        raw_value = row.get(csv_header)
        if db_column == "active":
            mapped_fields[db_column] = _to_bool(raw_value, default=True)
        else:
            mapped_fields[db_column] = _coerce_csv_value(raw_value)
        embedding_text_fields.append(f"{csv_header}: {_safe_text(raw_value)}")

    embedding_text = "\n".join(embedding_text_fields)[:text_limit]

    # Strict explicit payload for all requested CSV columns + embedding columns.
    payload: Dict[str, Any] = {
        "source_row": row,
        "source_file": "csv_data.csv",
        "updated_at": datetime.utcnow().isoformat(),
    }
    if include_embeddings:
        payload["embedding_text"] = embedding_text
        payload["embedding_model"] = DEFAULT_EMBED_MODEL
        payload["embedding_version"] = 1
    payload.update(mapped_fields)
    return payload


def _iter_csv_rows(csv_file_path: str) -> Iterable[Dict[str, str]]:
    encodings = ("utf-8-sig", "utf-8", "latin-1", "cp1252", "iso-8859-1")
    for enc in encodings:
        try:
            with open(csv_file_path, "r", encoding=enc, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    yield row
            return
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("csv", b"", 0, 1, "Failed to decode CSV with known encodings")


def _chunked(items: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    step = max(1, int(size))
    for i in range(0, len(items), step):
        yield items[i : i + step]


def _extract_missing_column(error_text: str) -> Optional[str]:
    patterns = [
        r"Could not find the '([^']+)' column",
        r'column "([^"]+)" of relation',
        r"column ([a-zA-Z0-9_]+) does not exist",
    ]
    for pattern in patterns:
        m = re.search(pattern, error_text, flags=re.IGNORECASE)
        if m:
            return m.group(1)
    return None


def _extract_missing_table(error_text: str) -> Optional[str]:
    patterns = [
        r"Could not find the table 'public\.([^']+)'",
        r"relation \"public\.([^\"]+)\" does not exist",
    ]
    for pattern in patterns:
        m = re.search(pattern, error_text, flags=re.IGNORECASE)
        if m:
            return m.group(1)
    return None


def _cosine_similarity(a: Sequence[float], b: Sequence[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b):
        fx = float(x)
        fy = float(y)
        dot += fx * fy
        na += fx * fx
        nb += fy * fy
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    return dot / (math.sqrt(na) * math.sqrt(nb))


def create_embeddings(
    client: Any,
    texts: Sequence[str],
    model: str,
    embed_batch_size: int,
    rate_limiter: OpenAIRateLimiter,
) -> List[List[float]]:
    vectors: List[List[float]] = []
    if not texts:
        return vectors

    for batch in _chunked(list(texts), embed_batch_size):
        response = _openai_embeddings_create(
            client,
            model=model,
            input_texts=list(batch),
            rate_limiter=rate_limiter,
        )
        vectors.extend(item.embedding for item in response.data)
    return vectors


def _filter_payload_columns(payloads: List[Dict[str, Any]], allowed_columns: Optional[set]) -> List[Dict[str, Any]]:
    if not allowed_columns:
        return payloads
    return [{k: v for k, v in p.items() if k in allowed_columns} for p in payloads]


def upsert_batch_with_adaptive_schema(
    supabase: Any,
    payloads: List[Dict[str, Any]],
    table_name: str,
    on_conflict: str = "notice_id",
    fallback_table: Optional[str] = None,
) -> Tuple[str, List[Dict[str, Any]]]:
    current_table = _normalize_table_name(table_name)
    fallback_table = _normalize_table_name(fallback_table) if fallback_table else None
    allowed_columns = set(payloads[0].keys()) if payloads else set()
    attempts = 0

    while True:
        attempts += 1
        if attempts > 40:
            raise RuntimeError("Exceeded adaptive schema retries while upserting batch.")

        filtered = _filter_payload_columns(payloads, allowed_columns)
        try:
            supabase.table(current_table).upsert(filtered, on_conflict=on_conflict).execute()
            return current_table, filtered
        except Exception as exc:
            message = str(exc)
            missing_col = _extract_missing_column(message)
            if missing_col and missing_col in allowed_columns:
                allowed_columns.remove(missing_col)
                logger.warning(
                    "Removed unknown column '%s' for table '%s' and retrying.",
                    missing_col,
                    current_table,
                )
                continue

            missing_table = _extract_missing_table(message)
            if (
                missing_table
                and fallback_table
                and current_table != fallback_table
            ):
                logger.warning(
                    "Table '%s' missing in schema cache, switching to fallback '%s'.",
                    current_table,
                    fallback_table,
                )
                current_table = fallback_table
                continue
            raise


def ingest_csv_embeddings(
    csv_file_path: str,
    table_name: str = DEFAULT_TABLE,
    row_batch_size: int = DEFAULT_ROW_BATCH_SIZE,
    embed_batch_size: int = DEFAULT_EMBED_BATCH_SIZE,
    upsert_batch_size: int = DEFAULT_UPSERT_BATCH_SIZE,
    max_rows: Optional[int] = None,
    fallback_table: Optional[str] = DEFAULT_FALLBACK_TABLE,
    embedding_enabled: bool = True,
) -> Dict[str, Any]:
    if not csv_file_path or not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"CSV file not found: {csv_file_path}")

    client = None
    if embedding_enabled:
        client = get_openai_client()
        if client is None:
            raise RuntimeError("OpenAI client is not configured. Check OPENAI/Azure env vars.")

    supabase = get_supabase_connection(use_service_key=True)

    target_table = _normalize_table_name(table_name)
    processed = 0
    inserted = 0
    skipped = 0
    started = time.time()

    row_buffer: List[Dict[str, str]] = []

    def flush_rows(rows: List[Dict[str, str]], current_table: str) -> str:
        nonlocal processed, inserted, skipped
        if not rows:
            return current_table

        parsed_rows = [
            _parse_csv_row(
                r,
                text_limit=DEFAULT_EMBED_TEXT_LIMIT,
                include_embeddings=embedding_enabled,
            )
            for r in rows
        ]

        if embedding_enabled:
            emb_texts = [p.get("embedding_text", "") for p in parsed_rows]
            vectors = create_embeddings(
                client,
                emb_texts,
                DEFAULT_EMBED_MODEL,
                embed_batch_size,
                rate_limiter=_OPENAI_RATE_LIMITER,
            )
            if len(vectors) != len(parsed_rows):
                raise RuntimeError(
                    f"Embedding count mismatch. vectors={len(vectors)}, rows={len(parsed_rows)}"
                )

            for p, vec in zip(parsed_rows, vectors):
                p["embedding"] = vec

        for upsert_chunk in _chunked(parsed_rows, upsert_batch_size):
            used_table, actual_payload = upsert_batch_with_adaptive_schema(
                supabase=supabase,
                payloads=list(upsert_chunk),
                table_name=current_table,
                on_conflict="notice_id",
                fallback_table=fallback_table,
            )
            current_table = used_table
            inserted += len(actual_payload)

        processed += len(rows)
        elapsed = max(1e-9, time.time() - started)
        speed = processed / elapsed
        print(
            f"[csv_embed] processed={processed} inserted={inserted} skipped={skipped} "
            f"table={current_table} rate={speed:.2f} rows/s",
            flush=True,
        )
        return current_table

    for csv_row in _iter_csv_rows(csv_file_path):
        if max_rows is not None and max_rows > 0 and processed + len(row_buffer) >= max_rows:
            break
        notice_id = _safe_text(csv_row.get("NoticeId"))
        if not notice_id:
            skipped += 1
            continue
        row_buffer.append(csv_row)
        if len(row_buffer) >= row_batch_size:
            target_table = flush_rows(row_buffer, target_table)
            row_buffer = []

    if row_buffer:
        target_table = flush_rows(row_buffer, target_table)

    elapsed_total = time.time() - started
    return {
        "status": "ok",
        "csv_file": csv_file_path,
        "target_table": target_table,
        "processed": processed,
        "inserted": inserted,
        "skipped": skipped,
        "embedding_enabled": bool(embedding_enabled),
        "embedding_model": DEFAULT_EMBED_MODEL,
        "elapsed_seconds": round(elapsed_total, 2),
    }


def _expand_query_texts_node(state: HybridSearchState) -> HybridSearchState:
    query = _safe_text(state.get("query"))
    expansions = max(1, int(state.get("search_expansions") or DEFAULT_SEARCH_EXPANSIONS))
    client = state["openai_client"]
    model = _safe_text(state.get("search_model")) or DEFAULT_SEARCH_MODEL

    if not query:
        state["search_texts"] = []
        state["query_intent"] = ""
        return state

    prompt = (
        "Expand this user query into concise retrieval queries for procurement opportunities.\n"
        "Return strict JSON with keys: query_intent (string), search_texts (array of strings).\n"
        f"Provide exactly {expansions} search_texts, each <= 12 words, distinct and non-overlapping."
    )
    try:
        resp = _openai_chat_create(
            client,
            model=model,
            temperature=0.2,
            max_tokens=240,
            messages=[
                {"role": "system", "content": "You produce compact JSON only."},
                {"role": "user", "content": f"{prompt}\n\nUser query: {query}"},
            ],
            rate_limiter=state.get("rate_limiter") or _OPENAI_RATE_LIMITER,
        )
        content = resp.choices[0].message.content if resp.choices else ""
        parsed = _extract_json_dict(content or "")
        texts = parsed.get("search_texts") if isinstance(parsed.get("search_texts"), list) else []
        cleaned = []
        seen = set()
        for t in texts:
            s = _safe_text(t)
            if s and s.lower() not in seen:
                seen.add(s.lower())
                cleaned.append(s)
        if query.lower() not in seen:
            cleaned.insert(0, query)
        state["search_texts"] = cleaned[:expansions]
        state["query_intent"] = _safe_text(parsed.get("query_intent")) or query
    except Exception:
        state["search_texts"] = [query]
        state["query_intent"] = query
    return state


def _keyword_search_single(
    supabase: Any,
    table_name: str,
    search_text: str,
    k: int,
    only_active: bool,
) -> List[Dict[str, Any]]:
    escaped = _safe_text(search_text).replace(",", " ").replace("%", "").strip()
    if not escaped:
        return []
    query = supabase.table(table_name).select("*")
    if only_active:
        query = query.eq("active", True)
    query = query.or_(
        "title.ilike.%{q}%,description.ilike.%{q}%,naics_code.ilike.%{q}%,department_ind_agency.ilike.%{q}%".format(
            q=escaped
        )
    )
    res = query.limit(max(5, k * 2)).execute()
    return (res.data or []) if hasattr(res, "data") else []


def _vector_search_single(
    supabase: Any,
    embedding: List[float],
    rpc_candidates: List[str],
    k: int,
    only_active: bool,
) -> List[Dict[str, Any]]:
    if not embedding:
        return []
    for rpc_name in rpc_candidates:
        try:
            res = supabase.rpc(
                rpc_name,
                {
                    "query_embedding": embedding,
                    "match_count": max(10, k * 3),
                    "only_active": bool(only_active),
                },
            ).execute()
            rows = (res.data or []) if hasattr(res, "data") else []
            if rows:
                return rows
        except Exception:
            continue
    return []


def _parallel_hybrid_search_node(state: HybridSearchState) -> HybridSearchState:
    search_texts = state.get("search_texts") or [_safe_text(state.get("query"))]
    k = max(1, int(state.get("k") or 20))
    only_active = bool(state.get("only_active", True))
    table_name = _normalize_table_name(state.get("table_name") or DEFAULT_TABLE)
    rpc_candidates = list(state.get("rpc_candidates") or list(DEFAULT_RPCS))
    workers = max(1, int(state.get("search_workers") or DEFAULT_SEARCH_WORKERS))
    client = state["openai_client"]
    embed_model = _safe_text(state.get("embed_model")) or DEFAULT_EMBED_MODEL

    if not search_texts:
        state["keyword_rows"] = []
        state["vector_rows"] = []
        state["all_rows"] = []
        state["search_embeddings"] = []
        return state

    # Batch embedding all expanded queries once for lower latency.
    embeddings = create_embeddings(
        client,
        search_texts,
        embed_model,
        embed_batch_size=len(search_texts),
        rate_limiter=state.get("rate_limiter") or _OPENAI_RATE_LIMITER,
    )
    state["search_embeddings"] = embeddings

    keyword_rows_all: List[Dict[str, Any]] = []
    vector_rows_all: List[Dict[str, Any]] = []

    def _job(search_text: str, emb: List[float], rank: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        sb = get_supabase_connection(use_service_key=True)
        kw = _keyword_search_single(sb, table_name, search_text, k, only_active)
        vr = _vector_search_single(sb, emb, rpc_candidates, k, only_active)
        for row in kw:
            row["_search_rank"] = rank
            row["_search_text"] = search_text
        for row in vr:
            base = row.get("doc") if isinstance(row, dict) and isinstance(row.get("doc"), dict) else row
            if isinstance(base, dict):
                base["_search_rank"] = rank
                base["_search_text"] = search_text
        return kw, vr

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(workers, len(search_texts))) as pool:
        futures = []
        for idx, (text, emb) in enumerate(zip(search_texts, embeddings)):
            futures.append(pool.submit(_job, text, emb, idx))

        for fut in concurrent.futures.as_completed(futures):
            try:
                kw, vr = fut.result()
                keyword_rows_all.extend(kw)
                vector_rows_all.extend(vr)
            except Exception:
                continue

    state["keyword_rows"] = keyword_rows_all
    state["vector_rows"] = vector_rows_all
    state["all_rows"] = keyword_rows_all + vector_rows_all
    return state


def _dedupe_unique_node(state: HybridSearchState) -> HybridSearchState:
    k = max(1, int(state.get("k") or 20))
    merged: Dict[str, Dict[str, Any]] = {}

    for row in state.get("keyword_rows") or []:
        notice_id = _safe_text(row.get("notice_id")) or _safe_text(row.get("NoticeId"))
        if not notice_id:
            continue
        rank_boost = 1.0 / (1.0 + _safe_float(row.get("_search_rank"), 0.0))
        item = merged.get(notice_id, dict(row))
        item["_hybrid_keyword_score"] = max(_safe_float(item.get("_hybrid_keyword_score")), rank_boost)
        merged[notice_id] = item

    for row in state.get("vector_rows") or []:
        base = row.get("doc") if isinstance(row, dict) and isinstance(row.get("doc"), dict) else row
        if not isinstance(base, dict):
            continue
        notice_id = _safe_text(base.get("notice_id")) or _safe_text(base.get("NoticeId"))
        if not notice_id:
            continue
        rank_boost = 1.0 / (1.0 + _safe_float(base.get("_search_rank"), 0.0))
        vector_score = _safe_float(row.get("similarity"), _safe_float(base.get("similarity"), 0.0))
        item = merged.get(notice_id, dict(base))
        item["_hybrid_vector_score"] = max(_safe_float(item.get("_hybrid_vector_score")), vector_score + (0.05 * rank_boost))
        merged[notice_id] = item

    ranked = sorted(
        merged.values(),
        key=lambda r: (
            0.70 * _safe_float(r.get("_hybrid_vector_score"))
            + 0.30 * _safe_float(r.get("_hybrid_keyword_score"))
        ),
        reverse=True,
    )

    # Get unique opportunities only.
    state["unique_rows"] = ranked[: max(k * 3, 30)]
    return state


def _schema_transform_node(state: HybridSearchState) -> HybridSearchState:
    query = _safe_text(state.get("query"))
    k = max(1, int(state.get("k") or 20))
    rows = state.get("unique_rows") or []
    client = state["openai_client"]
    model = _safe_text(state.get("search_model")) or DEFAULT_SEARCH_MODEL
    requested_schema = state.get("requested_schema")

    available_fields = sorted(
        {
            "notice_id",
            "title",
            "solicitation_number",
            "department_ind_agency",
            "notice_type",
            "base_type",
            "response_deadline",
            "naics_code",
            "classification_code",
            "award_amount",
            "awardee",
            "city",
            "state",
            "country_code",
            "link",
            "description",
            "set_aside",
            "archive_type",
            "posted_date",
            "active",
        }
    )

    fields = ["notice_id", "title", "department_ind_agency", "naics_code", "response_deadline", "link"]
    if not requested_schema:
        try:
            prompt = (
                "Given a user query and available opportunity fields, select the best result schema.\n"
                "Return JSON only with key output_fields as array of 5-9 fields."
            )
            resp = _openai_chat_create(
                client,
                model=model,
                temperature=0.1,
                max_tokens=150,
                messages=[
                    {"role": "system", "content": "Return compact JSON only."},
                    {
                        "role": "user",
                        "content": f"{prompt}\n\nQuery: {query}\nAvailable fields: {available_fields}",
                    },
                ],
                rate_limiter=state.get("rate_limiter") or _OPENAI_RATE_LIMITER,
            )
            content = resp.choices[0].message.content if resp.choices else ""
            parsed = _extract_json_dict(content or "")
            maybe_fields = parsed.get("output_fields") if isinstance(parsed.get("output_fields"), list) else []
            filtered = [f for f in maybe_fields if _safe_text(f) in available_fields]
            if filtered:
                fields = filtered[:9]
        except Exception:
            pass

    shaped: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows[:k], start=1):
        score = round(
            0.70 * _safe_float(row.get("_hybrid_vector_score")) + 0.30 * _safe_float(row.get("_hybrid_keyword_score")),
            4,
        )
        if requested_schema:
            item = _shape_with_schema(requested_schema, row, rank=idx, score=score)
            if isinstance(item, dict):
                item["rank"] = idx
                item["relevance_score"] = score
        else:
            item = {
                "rank": idx,
                "relevance_score": score,
            }
            for f in fields:
                item[f] = row.get(f)
        shaped.append(item)

    state["output_fields"] = list(requested_schema.keys()) if isinstance(requested_schema, dict) else fields
    state["results"] = shaped
    return state


def build_hybrid_search_graph() -> Any:
    if not HAS_LANGGRAPH:
        raise RuntimeError(
            "LangGraph is not installed. Install it with `pip install langgraph` to run hybrid graph search."
        )

    graph = StateGraph(HybridSearchState)
    graph.add_node("expand_queries", _expand_query_texts_node)
    graph.add_node("parallel_hybrid_search", _parallel_hybrid_search_node)
    graph.add_node("dedupe_unique", _dedupe_unique_node)
    graph.add_node("schema_transform", _schema_transform_node)

    graph.set_entry_point("expand_queries")
    graph.add_edge("expand_queries", "parallel_hybrid_search")
    graph.add_edge("parallel_hybrid_search", "dedupe_unique")
    graph.add_edge("dedupe_unique", "schema_transform")
    graph.add_edge("schema_transform", END)
    return graph.compile()


def run_hybrid_search(
    query: str,
    k: int = 20,
    only_active: bool = True,
    table_name: str = DEFAULT_TABLE,
    search_expansions: int = DEFAULT_SEARCH_EXPANSIONS,
    search_workers: int = DEFAULT_SEARCH_WORKERS,
    latency_budget_ms: int = DEFAULT_LATENCY_BUDGET_MS,
    requested_schema: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    openai_client = get_openai_client()
    if openai_client is None:
        raise RuntimeError("OpenAI client is not configured.")

    supabase = get_supabase_connection(use_service_key=True)
    start_time = time.time()
    clean_query, inline_schema = _extract_requested_schema_from_query(query)
    final_schema = requested_schema or inline_schema or _build_default_schema_template()
    state: HybridSearchState = HybridSearchState(
        {
            "start_time": start_time,
            "latency_budget_ms": max(1000, int(latency_budget_ms)),
            "query": clean_query,
            "query_intent": "",
            "k": k,
            "search_expansions": max(1, int(search_expansions)),
            "search_workers": max(1, int(search_workers)),
            "only_active": only_active,
            "table_name": _normalize_table_name(table_name),
            "openai_client": openai_client,
            "rate_limiter": _OPENAI_RATE_LIMITER,
            "supabase": supabase,
            "embed_model": DEFAULT_EMBED_MODEL,
            "search_model": DEFAULT_SEARCH_MODEL,
            "rpc_candidates": list(DEFAULT_RPCS),
            "query_embedding": [],
            "search_texts": [],
            "search_embeddings": [],
            "keyword_rows": [],
            "vector_rows": [],
            "all_rows": [],
            "unique_rows": [],
            "output_fields": [],
            "requested_schema": final_schema,
            "results": [],
        }
    )

    if HAS_LANGGRAPH:
        app = build_hybrid_search_graph()
        final_state = app.invoke(state)
    else:
        # Fallback path keeps the same pipeline when LangGraph isn't installed.
        final_state = _expand_query_texts_node(state)
        final_state = _parallel_hybrid_search_node(final_state)
        final_state = _dedupe_unique_node(final_state)
        final_state = _schema_transform_node(final_state)

    elapsed_ms = int((time.time() - start_time) * 1000)
    return {
        "query": clean_query,
        "query_intent": final_state.get("query_intent", ""),
        "search_texts": final_state.get("search_texts", []),
        "count": len(final_state.get("results") or []),
        "results": final_state.get("results") or [],
        "output_fields": final_state.get("output_fields", []),
        "requested_schema": final_schema,
        "latency_ms": elapsed_ms,
        "latency_target_ms": max(1000, int(latency_budget_ms)),
        "within_latency_target": elapsed_ms <= max(1000, int(latency_budget_ms)),
        "langgraph_used": HAS_LANGGRAPH,
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest large CSV into Supabase with embeddings and run hybrid search."
    )
    parser.add_argument("--mode", choices=["ingest", "search"], default="ingest")
    parser.add_argument("--csv", default="csv_data.csv", help="Path to source CSV file.")
    parser.add_argument("--table", default=DEFAULT_TABLE, help="Target Supabase table.")
    parser.add_argument("--fallback-table", default=DEFAULT_FALLBACK_TABLE, help="Fallback table if primary table is missing.")
    parser.add_argument("--row-batch-size", type=int, default=DEFAULT_ROW_BATCH_SIZE)
    parser.add_argument("--embed-batch-size", type=int, default=DEFAULT_EMBED_BATCH_SIZE)
    parser.add_argument("--upsert-batch-size", type=int, default=DEFAULT_UPSERT_BATCH_SIZE)
    parser.add_argument(
        "--embedding",
        type=str,
        default="true",
        help="Set true/false. If false, skips embedding generation and stores only row data.",
    )
    parser.add_argument("--max-rows", type=int, default=0, help="Optional cap for testing.")
    parser.add_argument("--query", default="", help="Hybrid search query text (search mode).")
    parser.add_argument(
        "--schema-json",
        default="",
        help="Optional output schema JSON template. If omitted, schema can also be embedded in query.",
    )
    parser.add_argument("--k", type=int, default=20, help="Hybrid search result limit.")
    parser.add_argument("--only-active", type=str, default="true", help="Filter active opportunities only.")
    parser.add_argument("--search-expansions", type=int, default=DEFAULT_SEARCH_EXPANSIONS, help="How many expanded search texts to generate.")
    parser.add_argument("--search-workers", type=int, default=DEFAULT_SEARCH_WORKERS, help="Parallel workers for per-text hybrid searches.")
    parser.add_argument("--latency-budget-ms", type=int, default=DEFAULT_LATENCY_BUDGET_MS, help="Target latency budget in milliseconds.")
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    if args.mode == "ingest":
        result = ingest_csv_embeddings(
            csv_file_path=args.csv,
            table_name=args.table,
            row_batch_size=max(1, int(args.row_batch_size)),
            embed_batch_size=max(1, int(args.embed_batch_size)),
            upsert_batch_size=max(1, int(args.upsert_batch_size)),
            max_rows=(int(args.max_rows) if int(args.max_rows) > 0 else None),
            fallback_table=args.fallback_table,
            embedding_enabled=_to_bool(args.embedding, default=True),
        )
        print(json.dumps(result, indent=2))
        return

    if not _safe_text(args.query):
        raise ValueError("--query is required in search mode.")

    result = run_hybrid_search(
        query=args.query,
        k=max(1, int(args.k)),
        only_active=_to_bool(args.only_active, default=True),
        table_name=args.table,
        search_expansions=max(1, int(args.search_expansions)),
        search_workers=max(1, int(args.search_workers)),
        latency_budget_ms=max(1000, int(args.latency_budget_ms)),
        requested_schema=(_extract_json_dict(args.schema_json) if _safe_text(args.schema_json) else None),
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
