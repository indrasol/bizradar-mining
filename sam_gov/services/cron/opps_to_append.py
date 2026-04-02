import argparse
import os
from typing import Dict, List, Set

import pandas as pd
from supabase import Client, create_client

try:
    from app.config.settings import SUPABASE_URL, SUPABASE_SERVICE_KEY, SUPABASE_ANON_KEY
except Exception:
    from config.settings import SUPABASE_URL, SUPABASE_SERVICE_KEY, SUPABASE_ANON_KEY


DEFAULT_CSV_PATH = r"C:\Users\rdine\Downloads\ContractOpportunitiesFullCSV (8).csv"
def _build_supabase_client() -> Client:
    """
    Build Supabase client using *_BIZ env variables.
    Prefers service key for stable reads on large `in_` queries.
    """
    url = (SUPABASE_URL or "").strip()
    service_key = (SUPABASE_SERVICE_KEY or "").strip()
    anon_key = (SUPABASE_ANON_KEY or "").strip()
    key = service_key or anon_key

    if not url:
        raise ValueError("Missing required env var: SUPABASE_URL_BIZ")
    if not key:
        raise ValueError("Missing required env var: SUPABASE_SERVICE_KEY_BIZ (or fallback SUPABASE_ANON_KEY_BIZ)")
    return create_client(url, key)


def _read_csv_notice_ids(csv_path: str) -> Dict[str, object]:
    """
    Read CSV and extract valid unique NoticeId values.
    Returns counters useful for reporting.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = None
    for enc in ("utf-8", "latin-1", "cp1252", "iso-8859-1"):
        try:
            df = pd.read_csv(csv_path, encoding=enc, low_memory=False)
            break
        except UnicodeDecodeError:
            continue

    if df is None:
        raise ValueError("Could not decode CSV with supported encodings")
    if "NoticeId" not in df.columns:
        raise ValueError("CSV does not contain required column: NoticeId")

    raw_count = len(df)
    notice_series = df["NoticeId"]
    cleaned_ids: List[str] = []
    blank_or_null = 0

    for value in notice_series:
        if pd.isna(value):
            blank_or_null += 1
            continue
        notice_id = str(value).strip()
        if not notice_id:
            blank_or_null += 1
            continue
        cleaned_ids.append(notice_id)

    unique_notice_ids: Set[str] = set(cleaned_ids)
    duplicate_notice_rows = max(0, len(cleaned_ids) - len(unique_notice_ids))

    return {
        "raw_rows": raw_count,
        "blank_notice_rows": blank_or_null,
        "rows_with_notice": len(cleaned_ids),
        "unique_notice_ids": len(unique_notice_ids),
        "duplicate_notice_rows": duplicate_notice_rows,
        "_ids": unique_notice_ids,  # internal payload for next step
    }


def _existing_notice_ids(supabase: Client, notice_ids: Set[str], chunk_size: int = 500) -> Set[str]:
    """
    Query existing notice IDs from ai_enhanced_opportunities in chunks.
    """
    existing: Set[str] = set()
    id_list = list(notice_ids)

    for idx in range(0, len(id_list), chunk_size):
        chunk = id_list[idx : idx + chunk_size]
        if not chunk:
            continue
        response = (
            supabase.table("ai_enhanced_opportunities")
            .select("notice_id")
            .in_("notice_id", chunk)
            .execute()
        )
        rows = getattr(response, "data", None) or []
        for row in rows:
            notice_id = row.get("notice_id")
            if notice_id:
                existing.add(str(notice_id).strip())

    return existing


def estimate_rows_to_append(csv_path: str) -> Dict[str, int]:
    """
    Estimate how many rows would be newly added (new notice_id values)
    to public.ai_enhanced_opportunities.
    """
    csv_stats = _read_csv_notice_ids(csv_path)
    notice_ids = csv_stats.pop("_ids")
    if not isinstance(notice_ids, set):
        raise TypeError("Internal error: expected set of notice ids")

    if not notice_ids:
        return {
            **csv_stats,
            "existing_notice_ids": 0,
            "estimated_new_rows": 0,
        }

    supabase = _build_supabase_client()
    existing_ids = _existing_notice_ids(supabase, notice_ids)

    return {
        **csv_stats,
        "existing_notice_ids": len(existing_ids),
        "estimated_new_rows": len(notice_ids - existing_ids),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Estimate new rows for public.ai_enhanced_opportunities from CSV NoticeId values."
    )
    parser.add_argument(
        "--csv-path",
        default=DEFAULT_CSV_PATH,
        help=f"Path to CSV file (default: {DEFAULT_CSV_PATH})",
    )
    args = parser.parse_args()

    result = estimate_rows_to_append(args.csv_path)
    print("=== ai_enhanced_opportunities append estimator ===")
    print(f"CSV: {args.csv_path}")
    print(f"Raw CSV rows: {result['raw_rows']}")
    print(f"Rows with blank/null NoticeId: {result['blank_notice_rows']}")
    print(f"Rows with NoticeId: {result['rows_with_notice']}")
    print(f"Duplicate NoticeId rows in CSV: {result['duplicate_notice_rows']}")
    print(f"Unique NoticeId values in CSV: {result['unique_notice_ids']}")
    print(f"Existing NoticeId values in table: {result['existing_notice_ids']}")
    print(f"Estimated new rows to append: {result['estimated_new_rows']}")


if __name__ == "__main__":
    main()
