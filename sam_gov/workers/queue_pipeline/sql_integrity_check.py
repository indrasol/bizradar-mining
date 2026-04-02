from collections import Counter, defaultdict

from sam_gov.utils.db_utils import get_supabase_connection


PAGE_SIZE = 1000
THREAD_ID_CHUNK_SIZE = 300


def _fetch_all_rows(supabase, table: str, columns: str) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        response = (
            supabase
            .table(table)
            .select(columns)
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        page = getattr(response, "data", None) or []
        if not page:
            break
        rows.extend(page)
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return rows


def _fetch_existing_thread_ids(supabase, thread_ids: set[str]) -> set[str]:
    if not thread_ids:
        return set()
    out: set[str] = set()
    ordered_ids = sorted(thread_ids)
    for idx in range(0, len(ordered_ids), THREAD_ID_CHUNK_SIZE):
        chunk = ordered_ids[idx: idx + THREAD_ID_CHUNK_SIZE]
        response = (
            supabase
            .table("ai_opportunity_threads")
            .select("thread_id")
            .in_("thread_id", chunk)
            .execute()
        )
        rows = getattr(response, "data", None) or []
        out.update(str(r.get("thread_id")) for r in rows if r.get("thread_id"))
    return out


def main() -> None:
    supabase = get_supabase_connection(use_service_key=True)
    versions = _fetch_all_rows(
        supabase,
        "ai_enhanced_opportunity_versions",
        "thread_id,is_latest_in_thread,notice_id,source_posted_at,source_archive_type,source_archive_date",
    )
    if not versions:
        print("SQL integrity check skipped: no version rows found.")
        return

    latest_counts: dict[str, int] = defaultdict(int)
    all_thread_ids: set[str] = set()
    duplicate_key_counter: Counter[tuple] = Counter()
    orphan_candidates = 0

    for row in versions:
        thread_id = str(row.get("thread_id") or "").strip()
        if not thread_id:
            orphan_candidates += 1
        else:
            all_thread_ids.add(thread_id)
            if bool(row.get("is_latest_in_thread", False)):
                latest_counts[thread_id] += 1

        duplicate_key = (
            str(row.get("notice_id") or "").strip(),
            str(row.get("source_posted_at") or ""),
            str(row.get("source_archive_type") or ""),
            str(row.get("source_archive_date") or ""),
        )
        duplicate_key_counter[duplicate_key] += 1

    existing_thread_ids = _fetch_existing_thread_ids(supabase, all_thread_ids)

    orphan_versions = orphan_candidates + sum(
        1
        for row in versions
        if str(row.get("thread_id") or "").strip()
        and str(row.get("thread_id") or "").strip() not in existing_thread_ids
    )

    latest_violations = [thread_id for thread_id, count in latest_counts.items() if count != 1]
    missing_latest_threads = sorted(all_thread_ids - set(latest_counts.keys()))
    duplicate_conflict_keys = [key for key, count in duplicate_key_counter.items() if count > 1]

    print(f"versions_total={len(versions)}")
    print(f"threads_total={len(all_thread_ids)}")
    print(f"orphan_versions={orphan_versions}")
    print(f"latest_violations={len(latest_violations)}")
    print(f"missing_latest_threads={len(missing_latest_threads)}")
    print(f"duplicate_conflict_keys={len(duplicate_conflict_keys)}")

    failures: list[str] = []
    if orphan_versions:
        failures.append(f"orphan_versions={orphan_versions}")
    if latest_violations:
        failures.append(f"latest_violations={len(latest_violations)}")
    if missing_latest_threads:
        failures.append(f"missing_latest_threads={len(missing_latest_threads)}")
    if duplicate_conflict_keys:
        failures.append(f"duplicate_conflict_keys={len(duplicate_conflict_keys)}")

    if failures:
        raise SystemExit("SQL integrity checks failed: " + ", ".join(failures))

    print("SQL integrity checks passed.")


if __name__ == "__main__":
    main()
