import os
import json
import time
import hashlib
import re
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# =============================
# CONFIG (LA City Open Data - Building Permits)
# =============================

# Socrata dataset id
DATASET_ID = "pi9x-tg5x"

# SODA endpoint (JSON)
SODA_URL = f"https://data.lacity.org/resource/{DATASET_ID}.json"

# Optional (rate limits are nicer if you provide an app token; not required)
SOCRATA_APP_TOKEN = os.environ.get("LA_SOCRATA_APP_TOKEN", "").strip()

# Filter year (default 2026)
TARGET_YEAR = int(os.environ.get("LA_PERMITS_YEAR", "2026"))

# Paging / throttling
PAGE_SIZE = int(os.environ.get("LA_PAGE_SIZE", "5000"))        # Socrata supports large pages; 5k is reasonable
SLEEP_SECONDS = float(os.environ.get("LA_SLEEP_SECONDS", "0.2"))

# Your output caps
MAX_NEW = int(os.environ.get("LA_MAX_NEW", "200"))

# Keep your existing allowlist (optional); permits don't have NAICS, but we can map by permit_type if you want later
ALLOWED_NAICS = {"238210", "236220", "237310", "238220", "238120"}

# Minimal mapping — permits dataset does not contain NAICS; we populate blank for now
NAICS_DESC = {
    "238210": "Electrical Contractors and Other Wiring Installation Contractors",
    "236220": "Commercial and Institutional Building Construction",
    "237310": "Highway, Street, and Bridge Construction",
    "238220": "Plumbing, Heating, and Air-Conditioning Contractors",
    "238120": "Structural Steel and Precast Concrete Contractors",
}


# =============================
# Helpers
# =============================

def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def normalize_str(x) -> str:
    return str(x).strip() if x is not None else ""


def safe_join(parts: List[str], sep: str = " ") -> str:
    return sep.join([p for p in (normalize_str(x) for x in parts) if p])


def parse_date_any(s: str) -> Optional[datetime]:
    """
    Socrata often returns ISO strings like:
      '2026-01-15T00:00:00.000'
    Sometimes they can be plain text; we do best-effort.
    """
    s = normalize_str(s)
    if not s:
        return None
    try:
        # normalize Z if present
        s2 = s.replace("Z", "+00:00")
        # If no timezone, treat as naive local -> keep naive
        return datetime.fromisoformat(s2)
    except Exception:
        # fallback: try first 10 chars YYYY-MM-DD
        m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
        if m:
            try:
                return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except Exception:
                return None
    return None


def header_map(ws) -> dict:
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("Row 1 headers are empty. Paste your column headers in row 1.")
    return {h: i + 1 for i, h in enumerate(headers)}  # 1-based


def load_existing_award_ids(ws) -> set:
    col_values = ws.col_values(1)  # Column A = Award ID
    return {v.strip() for v in col_values[1:] if v and v.strip()}


def get_gspread_client():
    creds_json = os.environ.get("LA_GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("Missing LA_GOOGLE_CREDENTIALS_JSON secret")
    creds_dict = json.loads(creds_json)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)


# =============================
# LA Permits fetch (Socrata SODA)
# =============================

def socrata_get(params: dict, timeout: int = 60) -> List[dict]:
    headers = {"User-Agent": "Mozilla/5.0"}
    if SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = SOCRATA_APP_TOKEN

    r = requests.get(SODA_URL, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


def fetch_permits_for_year(year: int, max_rows: int) -> List[dict]:
    """
    Pull rows where issue_date is within [year-01-01, year-12-31].
    We page via $limit / $offset.
    """
    start = f"{year}-01-01T00:00:00.000"
    end = f"{year}-12-31T23:59:59.999"

    out: List[dict] = []
    offset = 0

    # We only request the columns we need (faster, less brittle)
    select_cols = ",".join([
        "permit_nbr",
        "primary_address",
        "zip_code",
        "permit_group",
        "permit_type",
        "permit_sub_type",
        "use_desc",
        "submitted_date",
        "issue_date",
        "status_desc",
        "status_date",
        "valuation",
        "square_footage",
        "work_desc",
        "lat",
        "lon",
    ])

    where = f"issue_date >= '{start}' AND issue_date <= '{end}'"

    while len(out) < max_rows:
        limit = min(PAGE_SIZE, max_rows - len(out))
        params = {
            "$select": select_cols,
            "$where": where,
            "$order": "issue_date DESC",
            "$limit": str(limit),
            "$offset": str(offset),
        }

        batch = socrata_get(params=params)
        if not batch:
            break

        out.extend(batch)
        offset += len(batch)

        print(f"[SODA] fetched={len(out)} offset={offset} last_issue_date={batch[-1].get('issue_date','')}")
        time.sleep(SLEEP_SECONDS)

    return out


# =============================
# Main
# =============================

def main():
    sheet_id = os.environ.get("LA_SHEET_ID")
    tab_name = os.environ.get("LA_TAB_NAME")
    if not sheet_id or not tab_name:
        raise RuntimeError("Missing LA_SHEET_ID or LA_TAB_NAME")

    # Connect sheet
    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab_name)
    hmap = header_map(ws)
    existing_ids = load_existing_award_ids(ws)

    print(f"Downloading LA City building permits for year={TARGET_YEAR} (dataset={DATASET_ID}) ...")

    permits = fetch_permits_for_year(year=TARGET_YEAR, max_rows=MAX_NEW * 5)  # fetch extra; we dedupe and filter
    print(f"Fetched {len(permits)} raw permits from LA Open Data.")

    if not permits:
        print("No permits returned for that year filter. Try changing LA_PERMITS_YEAR or verify issue_date logic.")
        return

    now = utc_now_str()
    rows_to_append = []
    appended = 0

    # Quick debug: show a couple of issue dates
    sample_dates = [p.get("issue_date", "") for p in permits[:5]]
    print("[DEBUG] sample issue_date values:", sample_dates)

    for p in permits:
        if appended >= MAX_NEW:
            break

        permit_nbr = normalize_str(p.get("permit_nbr"))
        if not permit_nbr:
            continue

        award_id = permit_nbr
        if award_id in existing_ids:
            continue

        issue_date_raw = normalize_str(p.get("issue_date"))
        issue_dt = parse_date_any(issue_date_raw)

        # Hard safety check: ensure it really matches the TARGET_YEAR
        if not issue_dt or issue_dt.year != TARGET_YEAR:
            continue

        addr = normalize_str(p.get("primary_address"))
        zip_code = normalize_str(p.get("zip_code"))
        permit_group = normalize_str(p.get("permit_group"))
        permit_type = normalize_str(p.get("permit_type"))
        permit_sub_type = normalize_str(p.get("permit_sub_type"))
        use_desc = normalize_str(p.get("use_desc"))
        status_desc = normalize_str(p.get("status_desc"))
        valuation = normalize_str(p.get("valuation"))
        sqft = normalize_str(p.get("square_footage"))
        work_desc = normalize_str(p.get("work_desc"))

        lat = normalize_str(p.get("lat"))
        lon = normalize_str(p.get("lon"))

        # Recipient/company is not provided in this dataset (it’s a permit record, not a license registry)
        business = ""

        hq_addr = safe_join([addr, f"CA {zip_code}".strip()], sep=", ").strip(", ").strip()
        recipient_id = hashlib.md5(award_id.encode("utf-8")).hexdigest()

        # Links
        # Dataset landing page + basic query via search
        dataset_page = f"https://data.lacity.org/d/{DATASET_ID}"
        web_search = f"https://www.google.com/search?q={requests.utils.quote(permit_nbr)}+site:data.lacity.org+pi9x-tg5x"

        values = {
            "Award ID": award_id,
            "Recipient (Company)": business,
            "Recipient UEI": "",
            "Parent Recipient UEI": "",
            "Parent Recipient DUNS": "",
            "Recipient (HQ) Address": hq_addr,

            # Keep your downstream anchors; start/end are not directly in this dataset
            "Start Date": issue_dt.strftime("%Y-%m-%d"),
            "End Date": "",

            "Last Modified Date": now,
            "Award Amount (Obligated)": valuation,  # closest analog; it's permit valuation

            "NAICS Code": "",
            "NAICS Description": "",

            "Awarding Agency": "LA City Open Data (LADBS Building Permits Issued)",
            "Place of Performance": "Los Angeles, CA",
            "Description": safe_join(
                [
                    f"Permit {permit_nbr}",
                    f"Group={permit_group}" if permit_group else "",
                    f"Type={permit_type}" if permit_type else "",
                    f"SubType={permit_sub_type}" if permit_sub_type else "",
                    f"Use={use_desc}" if use_desc else "",
                    f"Status={status_desc}" if status_desc else "",
                    f"SqFt={sqft}" if sqft else "",
                    f"Work={work_desc}" if work_desc else "",
                ],
                sep=" | "
            ),
            "Award Link": dataset_page,
            "Recipient Profile Link": "",
            "Web Search Link": web_search,

            "Company Website": "",
            "Company Phone": "",
            "Company General Email": "",
            "Responsible Person Name": "",
            "Responsible Person Role": "",
            "Responsible Person Email": "",
            "Responsible Person Phone": "",

            "confidence_score": "70",
            "prediction_rationale": "la_city_open_data_permits(+70)",
            "target_flag": "TRUE",
            "recipient_id": recipient_id,
            "data_source": "LA City Open Data",
            "data_confidence_level": "Medium",
            "last_verified_date": now,
            "notes": safe_join(
                [
                    f"issue_date={issue_date_raw}" if issue_date_raw else "",
                    f"lat={lat},lon={lon}" if lat and lon else "",
                ],
                sep="; "
            ),
        }

        ordered_row = [""] * len(hmap)
        for header, col_index in hmap.items():
            ordered_row[col_index - 1] = values.get(header, "")

        rows_to_append.append(ordered_row)
        existing_ids.add(award_id)
        appended += 1

    if rows_to_append:
        ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        print(f"✅ Appended {len(rows_to_append)} rows into {tab_name}.")
    else:
        print("No rows appended. Either everything was already present (dedupe) or the year filter returned no new rows.")

    print("Done.")


if __name__ == "__main__":
    main()
