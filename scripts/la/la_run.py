import os
import io
import json
import time
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Optional

import gspread
from google.oauth2.service_account import Credentials

import openpyxl  # .xlsx
import xlrd      # .xls (BIFF)

from playwright.sync_api import sync_playwright


# =============================
# CONFIG
# =============================

CEQANET_URL = "https://ceqanet.lci.ca.gov/"

# Search window = all of 2026
DATE_START = "01/01/2026"
DATE_END   = "12/31/2026"

# Reuse your existing LA env vars (so you don't need new secrets)
SHEET_ID = os.environ.get("LA_SHEET_ID")
TAB_NAME = os.environ.get("LA_TAB_NAME")
CREDS_ENV = "LA_GOOGLE_CREDENTIALS_JSON"

# Controls
MAX_NEW = int(os.environ.get("LA_MAX_NEW", "2000"))
SLEEP_SECONDS = float(os.environ.get("LA_SLEEP_SECONDS", "0.05"))


# =============================
# Helpers
# =============================

def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def normalize_str(x) -> str:
    return str(x).strip() if x is not None else ""


def get_gspread_client():
    creds_json = os.environ.get(CREDS_ENV)
    if not creds_json:
        raise RuntimeError(f"Missing {CREDS_ENV} secret/env var")
    creds_dict = json.loads(creds_json)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)


def header_map(ws) -> dict:
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("Row 1 headers are empty. Paste your column headers in row 1.")
    return {h: i + 1 for i, h in enumerate(headers)}  # 1-based


def load_existing_ids(ws, id_col: int = 1) -> set:
    col_values = ws.col_values(id_col)
    return {v.strip() for v in col_values[1:] if v and v.strip()}


def _looks_like_xlsx(b: bytes) -> bool:
    return len(b) >= 2 and b[0:2] == b"PK"


def _looks_like_xls(b: bytes) -> bool:
    return len(b) >= 8 and b[0:8] == bytes([0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1])


def parse_excel_xlsx(x: bytes) -> List[Dict[str, str]]:
    wb = openpyxl.load_workbook(io.BytesIO(x), read_only=True, data_only=True)
    ws = wb.worksheets[0]
    rows_iter = ws.iter_rows(values_only=True)

    headers: List[str] = []
    out: List[Dict[str, str]] = []
    for idx, row in enumerate(rows_iter):
        values = [normalize_str(v) for v in row]
        if idx == 0:
            headers = values
            continue
        if not any(values):
            continue
        if len(values) < len(headers):
            values += [""] * (len(headers) - len(values))
        out.append({headers[i]: values[i] for i in range(min(len(headers), len(values)))})
    return out


def parse_excel_xls(x: bytes) -> List[Dict[str, str]]:
    book = xlrd.open_workbook(file_contents=x)
    sheet = book.sheet_by_index(0)

    if sheet.nrows < 1:
        return []

    headers = [normalize_str(sheet.cell_value(0, c)) for c in range(sheet.ncols)]
    out: List[Dict[str, str]] = []
    for r in range(1, sheet.nrows):
        rowvals = [normalize_str(sheet.cell_value(r, c)) for c in range(sheet.ncols)]
        if not any(rowvals):
            continue
        if len(rowvals) < len(headers):
            rowvals += [""] * (len(headers) - len(rowvals))
        out.append({headers[i]: rowvals[i] for i in range(min(len(headers), len(rowvals)))})
    return out


def parse_download_bytes(download_bytes: bytes) -> List[Dict[str, str]]:
    # Excel formats
    if _looks_like_xlsx(download_bytes):
        return parse_excel_xlsx(download_bytes)
    if _looks_like_xls(download_bytes):
        return parse_excel_xls(download_bytes)

    # CSV fallback
    text = download_bytes.decode("utf-8", errors="ignore").strip()
    if "," in text and "\n" in text:
        import csv
        rows = list(csv.DictReader(io.StringIO(text)))
        return [{k: normalize_str(v) for k, v in r.items()} for r in rows]

    return []


def pick(row: Dict[str, str], candidates: List[str]) -> str:
    lower_map = {k.lower(): k for k in row.keys()}
    for c in candidates:
        k = lower_map.get(c.lower())
        if k:
            v = normalize_str(row.get(k))
            if v:
                return v
    return ""


# =============================
# CEQAnet automation (Playwright)
# =============================

def ceqanet_download_statewide_2026(timeout_ms: int = 120_000) -> bytes:
    """
    Drives CEQAnet Advanced Search and downloads results for statewide California for 2026 date range.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        page.goto(CEQANET_URL, wait_until="domcontentloaded", timeout=timeout_ms)

        # Go to Advanced Search
        advanced_selectors = [
            "text=Advanced Search",
            "a:has-text('Advanced Search')",
            "button:has-text('Advanced Search')",
            "text=Advanced",
        ]
        for sel in advanced_selectors:
            try:
                page.locator(sel).first.click(timeout=10_000)
                break
            except Exception:
                continue

        page.wait_for_timeout(800)

        # Fill date range. Labels differ across versions; use multiple fallbacks.
        def try_fill_by_label(label_frag: str, value: str) -> bool:
            try:
                page.get_by_label(label_frag, exact=False).fill(value, timeout=5_000)
                return True
            except Exception:
                return False

        # Best-effort: fill 2 date fields
        ok1 = (
            try_fill_by_label("Start", DATE_START) or
            try_fill_by_label("From", DATE_START) or
            try_fill_by_label("Begin", DATE_START)
        )
        ok2 = (
            try_fill_by_label("End", DATE_END) or
            try_fill_by_label("To", DATE_END)
        )

        # If label matching fails, fill the first two visible text inputs that look empty.
        if not (ok1 and ok2):
            inputs = page.locator("input[type='text']:visible")
            count = inputs.count()
            if count >= 2:
                inputs.nth(0).fill(DATE_START)
                inputs.nth(1).fill(DATE_END)

        # Ensure statewide by clearing County/City if present
        for label in ["County", "City"]:
            try:
                dd = page.get_by_label(label, exact=False)
                dd.select_option("")  # clear
            except Exception:
                pass

        # Click Search
        search_selectors = [
            "button:has-text('Search')",
            "input[value='Search']",
            "text=Search",
        ]
        clicked = False
        for sel in search_selectors:
            try:
                page.locator(sel).first.click(timeout=10_000)
                clicked = True
                break
            except Exception:
                continue
        if not clicked:
            page.screenshot(path="ceqanet_debug_search_click.png", full_page=True)
            raise RuntimeError("Could not click Search in CEQAnet Advanced Search. Saved ceqanet_debug_search_click.png")

        # Wait for results to render
        page.wait_for_timeout(2000)

        # Click Export/Download
        download_selectors = [
            "button:has-text('Export')",
            "button:has-text('Download')",
            "a:has-text('Export')",
            "a:has-text('Download')",
            "text=Excel",
            "text=CSV",
        ]

        download = None
        for sel in download_selectors:
            try:
                with page.expect_download(timeout=30_000) as dl_info:
                    page.locator(sel).first.click(timeout=10_000)
                download = dl_info.value
                break
            except Exception:
                continue

        if download is None:
            page.screenshot(path="ceqanet_debug_no_export.png", full_page=True)
            html = page.content()
            print("[CEQANET] No export/download control found. Saved ceqanet_debug_no_export.png")
            print("[CEQANET] Page content length:", len(html))
            raise RuntimeError("CEQAnet search ran, but export/download control was not found.")

        data = download.path().read_bytes()  # type: ignore
        browser.close()
        return data


# =============================
# Main
# =============================

def main():
    if not SHEET_ID or not TAB_NAME:
        raise RuntimeError("Missing LA_SHEET_ID or LA_TAB_NAME env vars")

    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(TAB_NAME)

    hmap = header_map(ws)
    existing_ids = load_existing_ids(ws, id_col=1)

    print("CEQAnet statewide (California) | pulling records with date range in 2026...")
    download_bytes = ceqanet_download_statewide_2026()
    rows = parse_download_bytes(download_bytes)

    print(f"Downloaded + parsed rows: {len(rows)}")
    if not rows:
        print("No rows parsed. Export format may have changed or returned empty.")
        return

    now = utc_now_str()
    rows_to_append: List[List[str]] = []
    appended = 0

    for r in rows:
        if appended >= MAX_NEW:
            break

        # Typical CEQAnet columns (varies by export)
        sch = pick(r, ["SCH Number", "SCH#", "SCH", "State Clearinghouse Number"])
        title = pick(r, ["Project Title", "Title", "Project"])
        lead = pick(r, ["Lead Agency", "Agency"])
        county = pick(r, ["County"])
        city = pick(r, ["City"])
        doc_type = pick(r, ["Document Type", "Doc Type", "Type"])
        received = pick(r, ["Received", "Received Date", "Date Received"])
        posted = pick(r, ["Posted", "Posted Date", "Date Posted", "Published", "Publication Date"])
        location = pick(r, ["Location", "Project Location", "Address"])
        desc = pick(r, ["Description", "Project Description"])

        stable_key = sch or f"{title}|{lead}|{posted}|{received}|{county}|{city}"
        award_id = hashlib.md5(stable_key.encode("utf-8")).hexdigest()

        if award_id in existing_ids:
            continue

        # Map into your existing sheet schema (best-effort)
        values = {
            "Award ID": award_id,
            "Recipient (Company)": "",  # CEQAnet is project-level, not contractor-level
            "Recipient (HQ) Address": location,
            "Start Date": "2026-01-01",  # marker for your “2026 pipeline”
            "End Date": "",
            "Last Modified Date": now,
            "Award Amount (Obligated)": "",
            "NAICS Code": "",
            "NAICS Description": "",
            "Awarding Agency": lead,
            "Place of Performance": ", ".join([x for x in [city, county, "CA"] if x]),
            "Description": f"{title} | {doc_type} | Posted={posted} | Received={received} | {desc}".strip(),
            "Award Link": "",
            "Recipient Profile Link": "",
            "Web Search Link": "",

            "Company Website": "",
            "Company Phone": "",
            "Company General Email": "",
            "Responsible Person Name": "",
            "Responsible Person Role": "",
            "Responsible Person Email": "",
            "Responsible Person Phone": "",

            "confidence_score": "60",
            "prediction_rationale": "ceqanet_statewide_advanced_search(+60)",
            "target_flag": "TRUE",
            "recipient_id": award_id,
            "data_source": "CEQAnet (CA State Clearinghouse)",
            "data_confidence_level": "Medium",
            "last_verified_date": now,
            "notes": f"SCH={sch}; DocType={doc_type}; Lead={lead}",
        }

        ordered = [""] * len(hmap)
        for header, col_index in hmap.items():
            ordered[col_index - 1] = values.get(header, "")

        rows_to_append.append(ordered)
        existing_ids.add(award_id)
        appended += 1
        time.sleep(SLEEP_SECONDS)

    if rows_to_append:
        ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        print(f"✅ Appended {len(rows_to_append)} rows into {TAB_NAME}.")
    else:
        print("No new rows appended (deduped or empty export).")

    print("Done.")


if __name__ == "__main__":
    main()
