import argparse
import asyncio
import csv
import json
import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


from sam_gov.services.cron.csv_import_sam_gov import (
    process_csv_row,
    _resolve_thread_assignment_from_seen,
    _classify_opportunity_type,
    build_embedding_text_full_row,
    generate_embedding,
)


@dataclass
class SimulationStats:
    rows_read: int = 0
    rows_processed: int = 0
    rows_skipped: int = 0
    enriched: int = 0
    embedded: int = 0
    grouped_into_existing: int = 0
    new_threads: int = 0


def _read_csv_rows(csv_path: str, limit: Optional[int]) -> List[Dict[str, Any]]:
    encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]
    for enc in encodings:
        try:
            with open(csv_path, "r", encoding=enc, newline="") as f:
                reader = csv.DictReader(f)
                out: List[Dict[str, Any]] = []
                for i, row in enumerate(reader, start=1):
                    out.append(row)
                    if limit and i >= limit:
                        break
                return out
        except UnicodeDecodeError:
            continue
    raise ValueError("Could not decode CSV with supported encodings")


async def _maybe_enrich_and_embed(row: Dict[str, Any], do_ai: bool, do_embed: bool) -> Dict[str, Any]:
    if do_ai:
        description_text = str(row.get("description") or "")
        classification = await _classify_opportunity_type(row=row, thread_context_text=description_text[:6000], persisted_versions=[])
        row["opportunity_type"] = classification.get("label", "UNKNOWN")
        row["opportunity_type_confidence"] = classification.get("confidence", 0.0)
        row["opportunity_type_method"] = classification.get("method", "unknown")
        row["opportunity_type_needs_review"] = bool(classification.get("needs_review", False))
    else:
        row["opportunity_type"] = row.get("opportunity_type") or "UNKNOWN"
        row["opportunity_type_confidence"] = row.get("opportunity_type_confidence") or 0.0
        row["opportunity_type_method"] = row.get("opportunity_type_method") or "simulation_skipped"
        row["opportunity_type_needs_review"] = row.get("opportunity_type_needs_review") or True

    if do_embed:
        text = build_embedding_text_full_row(row)
        if text.strip():
            row["embedding_text"] = text
            row["embedding"] = await generate_embedding(text)
            row["embedding_version"] = 1
    else:
        row["embedding_text"] = build_embedding_text_full_row(row)[:2000]
        row["embedding"] = []
        row["embedding_version"] = 0

    return row


def run_simulation(csv_path: str, limit: Optional[int], do_ai: bool, do_embed: bool) -> Dict[str, Any]:
    raw_rows = _read_csv_rows(csv_path, limit)
    stats = SimulationStats(rows_read=len(raw_rows))
    seen_rows: List[Dict[str, Any]] = []
    thread_counter = 0
    thread_id_cache: Dict[str, str] = {}
    processed_rows: List[Dict[str, Any]] = []

    for raw in raw_rows:
        row = process_csv_row(raw)
        if not row:
            stats.rows_skipped += 1
            continue
        stats.rows_processed += 1

        assignment = _resolve_thread_assignment_from_seen(seen_rows, row)
        if assignment and assignment.get("thread_id"):
            thread_id = assignment["thread_id"]
            stats.grouped_into_existing += 1
        else:
            thread_key = str(row.get("thread_key") or f"notice:{row.get('notice_id')}")
            if thread_key not in thread_id_cache:
                thread_counter += 1
                thread_id_cache[thread_key] = f"sim-thread-{thread_counter}"
                stats.new_threads += 1
            thread_id = thread_id_cache[thread_key]
            assignment = {
                "thread_id": thread_id,
                "match_method": "simulation_new_thread",
                "match_score": 0.0,
                "matched_to_version_id": None,
                "decision_reason": {"reason": "simulation_new_thread"},
            }

        row["thread_id"] = thread_id
        row["match_method"] = assignment.get("match_method")
        row["match_score"] = assignment.get("match_score")
        row["matched_to_version_id"] = assignment.get("matched_to_version_id")
        row["decision_reason"] = assignment.get("decision_reason")
        row["is_latest_in_thread"] = False

        seen_rows.append(
            {
                "thread_id": thread_id,
                "solicitation_number_norm": row.get("solicitation_number_norm"),
                "department_norm": row.get("department_norm"),
                "title_norm": row.get("title_norm"),
                "contact_email_norm": row.get("contact_email_norm"),
                "contact_phone_norm": row.get("contact_phone_norm"),
                "contact_name_norm": row.get("contact_name_norm"),
                "metadata_simhash": row.get("metadata_simhash"),
            }
        )
        processed_rows.append(row)

    async def _apply_all() -> None:
        for r in processed_rows:
            await _maybe_enrich_and_embed(r, do_ai=do_ai, do_embed=do_embed)

    asyncio.run(_apply_all())
    if processed_rows:
        stats.enriched = len(processed_rows)
        stats.embedded = len(processed_rows)

    versions_per_thread: Dict[str, int] = defaultdict(int)
    for r in processed_rows:
        versions_per_thread[str(r.get("thread_id"))] += 1

    return {
        "stats": stats.__dict__,
        "threads_total": len(versions_per_thread),
        "versions_per_thread": dict(sorted(versions_per_thread.items(), key=lambda kv: kv[1], reverse=True)),
        "sample_rows": [
            {
                "notice_id": r.get("notice_id"),
                "thread_id": r.get("thread_id"),
                "match_method": r.get("match_method"),
                "opportunity_type": r.get("opportunity_type"),
                "opportunity_type_confidence": r.get("opportunity_type_confidence"),
            }
            for r in processed_rows[:10]
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Local in-memory queue pipeline simulation.")
    parser.add_argument("--csv-path", required=True, type=str, help="Path to CSV input")
    parser.add_argument("--limit", required=False, type=int, default=50, help="Max rows to simulate")
    parser.add_argument("--enable-ai", action="store_true", help="Enable live classification calls")
    parser.add_argument("--enable-embedding", action="store_true", help="Enable live embedding calls")
    parser.add_argument("--report-path", required=False, type=str, default="", help="Optional path to write JSON report")
    args = parser.parse_args()

    report = run_simulation(
        csv_path=args.csv_path,
        limit=args.limit,
        do_ai=bool(args.enable_ai),
        do_embed=bool(args.enable_embedding),
    )
    out = json.dumps(report, ensure_ascii=True, indent=2)
    print(out)
    if args.report_path:
        with open(args.report_path, "w", encoding="utf-8") as f:
            f.write(out + "\n")


if __name__ == "__main__":
    main()
