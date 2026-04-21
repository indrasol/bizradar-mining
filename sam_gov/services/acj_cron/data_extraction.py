"""Step 1: Download Contract Opportunities CSV from SAM.gov.

Responsibilities:
  - Launch headless Chromium (pre-installed in the Docker image)
  - Navigate to SAM.gov data services, accept terms dialog
  - Download ContractOpportunitiesFullCSV.csv
  - Filter to active rows only (Active == 'yes')
  - Reverse row order so newest opportunities come first
  - Return the local file path for downstream scripts

This script does NOT chunk, upload, deduplicate, or ingest.
"""

import logging
import os
import tempfile
import time
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

logger = logging.getLogger(__name__)

SAM_URL = "https://sam.gov/data-services/Contract%20Opportunities/datagov?privacy=Public"
DEST_FILENAME = "ContractOpportunitiesFullCSV.csv"
NAV_TIMEOUT_MS = 60_000
DOWNLOAD_TIMEOUT_MS = 120_000
MAX_DOWNLOAD_RETRIES = 3

# Encoding fallback chain for SAM.gov CSV files
ENCODINGS = ("utf-8", "latin-1", "cp1252", "iso-8859-1")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _detect_encoding(path: Path) -> str:
    """Try each encoding on the first 4 KB and return the first that works."""
    for enc in ENCODINGS:
        try:
            with open(path, "r", encoding=enc) as f:
                f.read(4096)
            return enc
        except UnicodeDecodeError:
            continue
    return "utf-8"


def _read_csv(path: Path) -> tuple[pd.DataFrame, str]:
    """Read a CSV with automatic encoding detection. Returns (df, encoding)."""
    for enc in ENCODINGS:
        try:
            df = pd.read_csv(path, encoding=enc, low_memory=False, on_bad_lines="skip")
            return df, enc
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not decode {path} with any supported encoding")


# ---------------------------------------------------------------------------
# SAM.gov terms dialog handling
# ---------------------------------------------------------------------------

def _scroll_terms_and_accept(page) -> None:
    """Scroll the terms dialog to the bottom (enabling Accept) and click it."""
    try:
        page.wait_for_selector("dialog, [role='dialog']", timeout=5000)
    except PlaywrightTimeoutError:
        return  # no dialog present

    page.evaluate(
        """() => {
            const dialog = document.querySelector('dialog, [role="dialog"]');
            if (!dialog) return;
            for (const el of dialog.querySelectorAll('*')) {
              const style = getComputedStyle(el);
              if (/(auto|scroll)/.test(style.overflowY) && el.scrollHeight > el.clientHeight)
                el.scrollTop = el.scrollHeight;
            }
        }"""
    )

    page.wait_for_function(
        """() => {
            const btn = Array.from(document.querySelectorAll('button'))
                         .find(b => (b.textContent || '').trim() === 'Accept');
            return btn && !btn.disabled;
        }""",
        timeout=15000,
    )
    page.get_by_role("button", name="Accept").click()


# ---------------------------------------------------------------------------
# Post-download processing
# ---------------------------------------------------------------------------

def _filter_active_rows(csv_path: Path) -> None:
    """Keep only rows where Active == 'yes', overwrite the CSV in place."""
    df, enc = _read_csv(csv_path)

    if "Active" not in df.columns:
        logger.warning("Column 'Active' not found in CSV, skipping active filter")
        return

    before = len(df)
    df = df[df["Active"].astype(str).str.strip().str.lower() == "yes"]
    removed = before - len(df)

    if removed:
        logger.info(f"Removed {removed} inactive rows, {len(df)} active rows remain")

    df.to_csv(csv_path, index=False, encoding=enc)


def _reverse_csv_rows(csv_path: Path) -> None:
    """Reverse data rows in place (header stays first) so newest come first.

    Uses atomic write via temp file + os.replace to avoid partial writes.
    """
    enc = _detect_encoding(csv_path)

    tmp_fd, tmp_path = tempfile.mkstemp(dir=csv_path.parent, suffix=".tmp")
    try:
        with open(csv_path, "r", encoding=enc, errors="replace") as fh:
            header = fh.readline()
            lines = fh.readlines()

        with os.fdopen(tmp_fd, "w", encoding=enc) as fh:
            fh.write(header)
            for line in reversed(lines):
                fh.write(line)

        os.replace(tmp_path, csv_path)
        logger.info(f"Reversed {len(lines)} data rows (newest first)")
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# Main download function
# ---------------------------------------------------------------------------

def download_csv(dest_path: Path | None = None) -> Path:
    """Download the SAM.gov CSV, filter active rows, and reverse order.

    Args:
        dest_path: Where to save the file. Falls back to CSV_PATH env var
                   or current working directory.

    Returns:
        Path to the processed CSV on disk, ready for dedup and chunking.
    """
    if dest_path is None:
        csv_env = os.getenv("CSV_PATH")
        if csv_env:
            dest_path = Path(csv_env)
            if dest_path.is_dir():
                dest_path = dest_path / DEST_FILENAME
        else:
            dest_path = Path.cwd() / DEST_FILENAME

    logger.info(f"Starting SAM.gov download, target: {dest_path}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # Navigate
        page.goto(SAM_URL, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
        page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT_MS)

        # Accept terms
        _scroll_terms_and_accept(page)

        # Wait for file list
        page.wait_for_selector("text=File Extracts", timeout=15000)
        page.wait_for_selector(
            "text=ContractOpportunitiesFullCSV.csv", timeout=15000
        )

        # Clean up any previous file
        if dest_path.exists():
            dest_path.unlink()

        # Locate the download link
        file_link = page.locator(
            "a.data-service-file-link:has-text('ContractOpportunitiesFullCSV.csv')"
        ).first
        page.wait_for_selector(
            "a.data-service-file-link:has-text('ContractOpportunitiesFullCSV.csv')",
            timeout=15000,
        )

        # Retry loop: click download, re-accept terms if dialog reappears
        download = None
        last_error = None
        for attempt in range(1, MAX_DOWNLOAD_RETRIES + 1):
            try:
                with page.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as dl_info:
                    file_link.click()
                download = dl_info.value
                break
            except Exception as exc:
                last_error = exc
                logger.warning(f"Download attempt {attempt} failed: {exc}")
                try:
                    page.wait_for_selector("dialog, [role='dialog']", timeout=2000)
                    _scroll_terms_and_accept(page)
                except PlaywrightTimeoutError:
                    time.sleep(1)

        if download is None:
            raise RuntimeError(
                f"Failed to download after {MAX_DOWNLOAD_RETRIES} attempts: {last_error}"
            )

        download.save_as(str(dest_path))
        size_mb = dest_path.stat().st_size / (1024 * 1024)
        logger.info(f"Downloaded {dest_path.name} ({size_mb:.1f} MB)")

        context.close()
        browser.close()

    # Post-processing
    _filter_active_rows(dest_path)
    _reverse_csv_rows(dest_path)

    logger.info(f"Data extraction complete: {dest_path}")
    return dest_path