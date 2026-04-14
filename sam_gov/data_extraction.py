import os
import tempfile
import time
from pathlib import Path
from typing import Set

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from sam_gov.config.settings import SUPABASE_URL, SUPABASE_SERVICE_KEY, SUPABASE_ANON_KEY

SAM_URL = "https://sam.gov/data-services/Contract%20Opportunities/datagov?privacy=Public"
DEST_FILENAME = "ContractOpportunitiesFullCSV.csv"
NAV_TIMEOUT_MS = 60000
DOWNLOAD_TIMEOUT_MS = 120000

def scroll_terms_and_accept(page):
    # Wait for terms dialog if present
    try:
        page.wait_for_selector("dialog, [role='dialog']", timeout=5000)
    except PlaywrightTimeoutError:
        return  # No dialog; nothing to accept

    # Scroll all scrollable elements inside the dialog to bottom to enable Accept
    page.evaluate(
        """() => {
            const dialog = document.querySelector('dialog, [role="dialog"]');
            if (!dialog) return;
            const nodes = dialog.querySelectorAll('*');
            for (const el of nodes) {
              const style = getComputedStyle(el);
              const canScroll = /(auto|scroll)/.test(style.overflowY) && el.scrollHeight > el.clientHeight;
              if (canScroll) el.scrollTop = el.scrollHeight;
            }
        }"""
    )

    # Wait until Accept is enabled, then click
    page.wait_for_function(
        """() => {
            const btns = Array.from(document.querySelectorAll('button'));
            const btn = btns.find(b => b.textContent && b.textContent.trim() === 'Accept');
            return btn && !btn.disabled;
        }""",
        timeout=15000,
    )
    page.get_by_role("button", name="Accept").click()

def _fetch_existing_notice_ids(notice_ids: Set[str], chunk_size: int = 500) -> Set[str]:
    """
    Query ai_enhanced_opportunity_versions in Supabase and return the subset of
    notice_ids that already exist in the table.
    """
    from supabase import create_client

    url = (SUPABASE_URL or "").strip()
    key = (SUPABASE_SERVICE_KEY or "").strip() or (SUPABASE_ANON_KEY or "").strip()
    if not url or not key:
        raise ValueError("Missing SUPABASE_URL_BIZ and/or SUPABASE_SERVICE_KEY_BIZ")
    client = create_client(url, key)

    existing: Set[str] = set()
    id_list = list(notice_ids)
    for idx in range(0, len(id_list), chunk_size):
        chunk = id_list[idx : idx + chunk_size]
        if not chunk:
            continue
        response = (
            client.table("ai_enhanced_opportunity_versions")
            .select("notice_id")
            .in_("notice_id", chunk)
            .execute()
        )
        for row in getattr(response, "data", None) or []:
            nid = row.get("notice_id")
            if nid:
                existing.add(str(nid).strip())
    return existing


def _filter_existing_notice_ids(dest_path: Path) -> None:
    """
    Read the downloaded CSV, remove rows whose NoticeId already exists in
    ai_enhanced_opportunity_versions, and overwrite the file with the filtered result.
    """
    encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]
    df = None
    detected_encoding = "utf-8"
    for enc in encodings:
        try:
            df = pd.read_csv(dest_path, encoding=enc, low_memory=False)
            detected_encoding = enc
            break
        except UnicodeDecodeError:
            continue
    if df is None:
        raise ValueError("Could not decode CSV with any supported encoding")

    if "Active" in df.columns:
        before_active = len(df)
        df = df[df["Active"].astype(str).str.strip().str.lower() == "yes"]
        removed_inactive = before_active - len(df)
        if removed_inactive:
            print(f"Removed {removed_inactive} inactive rows — {len(df)} active rows remain")
    else:
        print("Column 'Active' not found in CSV — skipping active filter")

    if "NoticeId" not in df.columns:
        print("Column 'NoticeId' not found in CSV — skipping duplicate filter")
        return

    csv_ids = set(
        str(v).strip()
        for v in df["NoticeId"].dropna()
        if str(v).strip()
    )
    if not csv_ids:
        return

    existing_ids = _fetch_existing_notice_ids(csv_ids)
    if not existing_ids:
        print(f"No existing notice_ids found — keeping all {len(df)} rows")
        return

    before = len(df)
    mask = df["NoticeId"].apply(
        lambda v: pd.isna(v) or str(v).strip() not in existing_ids
    )
    df = df[mask]
    after = len(df)
    print(f"Filtered {before - after} existing rows — {after} rows remain")

    df.to_csv(dest_path, index=False, encoding=detected_encoding)


def _reverse_csv_rows(dest_path: Path) -> None:
    """
    Reverse the data rows of the downloaded CSV in-place, keeping the header first.

    Uses readlines() instead of csv.reader to avoid per-cell Python object overhead.
    Peak RAM is ~1.05× file size — safe for the GitHub Actions 7 GB standard runner
    even for multi-GB SAM.gov exports.

    Writes to a temp file on the same filesystem then swaps atomically via os.replace()
    so the original is never left in a partially-written state on failure.
    """
    encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]
    detected_encoding = "utf-8"
    for enc in encodings:
        try:
            with open(dest_path, "r", encoding=enc) as _f:
                _f.read(4096)
            detected_encoding = enc
            break
        except UnicodeDecodeError:
            continue

    tmp_fd, tmp_path = tempfile.mkstemp(dir=dest_path.parent, suffix=".tmp")
    try:
        with open(dest_path, "r", encoding=detected_encoding, errors="replace") as fh:
            header = fh.readline()
            lines = fh.readlines()

        with os.fdopen(tmp_fd, "w", encoding=detected_encoding) as fh:
            fh.write(header)
            for line in reversed(lines):
                fh.write(line)

        os.replace(tmp_path, dest_path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def main():
    csv_env = os.getenv("CSV_PATH")
    if csv_env:
        dest_path = Path(csv_env)
        if dest_path.is_dir():
            dest_path = dest_path / DEST_FILENAME
    else:
        dest_path = Path.cwd() / DEST_FILENAME

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # Navigate
        page.goto(SAM_URL, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
        page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT_MS)

        # Handle terms modal (scroll to enable and accept)
        scroll_terms_and_accept(page)

        # Ensure list is visible
        page.wait_for_selector("text=File Extracts", timeout=15000)
        page.wait_for_selector("text=ContractOpportunitiesFullCSV.csv", timeout=15000)

        # Remove existing file if present to avoid save_as conflicts
        if dest_path.exists():
            dest_path.unlink()

        # Ensure the file link is present
        file_link = page.locator("a.data-service-file-link:has-text('ContractOpportunitiesFullCSV.csv')").first
        page.wait_for_selector("a.data-service-file-link:has-text('ContractOpportunitiesFullCSV.csv')", timeout=15000)

        # Retry clicking and accepting terms if the modal reappears
        download = None
        last_error = None
        for _ in range(3):
            try:
                with page.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as dl_info:
                    file_link.click()
                download = dl_info.value
                break
            except Exception as e:  # capture timeout or other click issues
                last_error = e
                # If a terms dialog popped up again, accept and retry
                try:
                    page.wait_for_selector("dialog, [role='dialog']", timeout=2000)
                    scroll_terms_and_accept(page)
                    continue
                except PlaywrightTimeoutError:
                    # No dialog; small delay and retry
                    time.sleep(1)
                    continue

        if download is None:
            raise RuntimeError(f"Failed to start download after retries: {last_error}")

        download.save_as(str(dest_path))

        _reverse_csv_rows(dest_path)
        _filter_existing_notice_ids(dest_path)

        # print(f"Saved: {dest_path}")

        context.close()
        browser.close()

if __name__ == "__main__":
    main()