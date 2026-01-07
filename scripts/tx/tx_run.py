import os
import json
import time
import hashlib
from datetime import datetime, timezone, date
from typing import Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials

# =============================
# CONFIG
# =============================

BASE_DOMAIN = "https://data.austintexas.gov"
DATASET_ID = os.environ.get("TX_DATASET_ID", "3syk-w9eu")

# Pull since (inclusive) in YYYY-MM-DD
SINCE_DATE_STR = os.environ.get("TX_SINCE_DATE", "2025-12-15")

# Google Sheet
SHEET_ID = os.environ.get("TX_SHEET_ID")
TAB_NAME = os.environ.get("TX_TAB_NAME")
CREDS_ENV = "ATX_GOOGLE_CREDENTIALS_JSON"  # you said the secret is already renamed to this

# Controls
MAX_NEW = int(os.environ.get("TX_MAX_NEW", "2000"))
SLEEP_SECONDS = float(os.environ.get("TX_SLEEP_SECONDS", "0.05"))

# Socrata paging
PAGE_LIMIT = int(os.environ.get("TX_PAGE_LIMIT", "5000"))
REQUEST_TIMEOUT = int(os.environ.get("TX_REQUEST_TIMEOUT", "60"))

# =============================
# Helpers
# =============================

def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def parse_since_date(s: str) -> date:
    y, m, d = s.strip().split("-")
    return date(int(y), int(m), int(d))

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

def http_get_json(url: str, params: Optional[dict] = None) -> object:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; TX-permit-puller/1.0)",
        "Accept": "application/json",
    }
    r = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def stable_award_id(row: Dict[str, object]) -> str:
    for k in [":id", "id", "permit_number", "permitnum", "permit_num", "permit_id"]:
        v = row.get(k)
        if v:
            return hashlib.md5(str(v).encode("utf-8")).hexdigest()

    composite_keys = [
        "permit_type",
        "description",
        "address",
        "zip_code",
        "valuation",
        "square_footage",
        "units",
    ]
    blob = "|".join([normalize_str(row.get(k, "")) for k in composite_keys])
    return hashlib.md5(blob.encode("utf-8")).hexdigest()

# =============================
# Socrata: metadata + querying
# =============================

def fetch_dataset_meta() -> Dict[str, object]:
    url = f"{BASE_DOMAIN}/api/views/{DATASET_ID}"
    meta = http_get_json(url)
    if not isinstance(meta, dict):
        raise RuntimeError("Dataset metadata is not a dict; unexpected response.")
    return meta

def pick_issue_date_field(columns: List[Dict[str, object]]) -> str:
    candidates: List[Tuple[int, str]] = []

    for c in columns:
        name = (c.get("name") or "").lower()
        field = (c.get("fieldName") or "")
        datatype = (c.get("dataTypeName") or "").lower()

        if not field:
            continue

        if datatype in {"calendar_date", "date", "fixed_timestamp", "floating_timestamp"}:
            score = 0
            n = name
            f = field.lower()
            if "issue" in n or "issue" in f:
                score += 10
            if "issued" in n or "issued" in f:
                score += 10
            if "date" in n or "date" in f:
                score += 5
            candidates.append((score, field))

    candidates.sort(reverse=True, key=lambda x: x[0])
    if candidates:
        return candidates[0][1]

    for fallback in ["issue_date", "issued_date", "permit_issue_date"]:
        for c in columns:
            if (c.get("fieldName") or "").lower() == fallback:
                return str(c.get("fieldName"))

    raise RuntimeError("Could not identify an issue/issued date field from dataset metadata.")

def socrata_query_since(date_field: str, since_yyyy_mm_dd: str, limit: int, offset: int) -> List[Dict[str, object]]:
    since_iso = f"{since_yyyy_mm_dd}T00:00:00.000"
    url = f"{BASE_DOMAIN}/resource/{DATASET_ID}.json"
    params = {
        "$limit": limit,
        "$offset": offset,
        "$order": f"{date_field} ASC",
        "$where": f"{date_field} >= '{since_iso}'",
    }
    data = http_get_json(url, params=params)
    if not isinstance(data, list):
        return []
    return data  # type: ignore

def self_check() -> Tuple[str, str]:
    meta = fetch_dataset_meta()
    cols = meta.get("columns", [])
    if not isinstance(cols, list):
        cols = []
    cols = [c for c in cols if isinstance(c, dict)]
    date_field = pick_issue_date_field(cols)  # type: ignore
    dataset_name = normalize_str(meta.get("name", ""))
    print(f"[SELF-CHECK] dataset_id={DATASET_ID} name='{dataset_name}' date_field='{date_field}' columns={len(cols)}")
    return date_field, dataset_name

# =============================
# Main
# =============================

def main():
    if not SHEET_ID or not TAB_NAME:
        raise RuntimeError("Missing TX_SHEET_ID or TX_TAB_NAME env vars")

    since_date = parse_since_date(SINCE_DATE_STR)

    date_field, dataset_name = self_check()

    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(TAB_NAME)

    hmap = header_map(ws)
    existing_ids = load_existing_ids(ws, id_col=1)

    print(f"TX | dataset={DATASET_ID} '{dataset_name}' | since={since_date.isoformat()} | max_new={MAX_NEW}")

    now = utc_now_str()
    rows_to_append: List[List[str]] = []
    appended = 0

    offset = 0
    while appended < MAX_NEW:
        batch = socrata_query_since(
            date_field=date_field,
            since_yyyy_mm_dd=since_date.isoformat(),
            limit=PAGE_LIMIT,
            offset=offset
        )
        if not batch:
            break

        for row in batch:
            if appended >= MAX_NEW:
                break

            award_id = stable_award_id(row)
            if award_id in existing_ids:
                continue

            permit_type = normalize_str(row.get("permit_type", ""))
            description = normalize_str(row.get("description", ""))
            valuation = normalize_str(row.get("valuation", ""))
            address = normalize_str(row.get("address", "")) or normalize_str(row.get("location", ""))
            zip_code = normalize_str(row.get("zip_code", ""))
            issued_date = normalize_str(row.get(date_field, ""))

            dataset_page_link = f"{BASE_DOMAIN}/Building-and-Development/Issued-Construction-Permits/{DATASET_ID}"

            values = {
                "Award ID": award_id,
                "Recipient (Company)": "",
                "Recipient UEI": "",
                "Parent Recipient UEI": "",
                "Parent Recipient DUNS": "",
                "Recipient (HQ) Address": address,
                "Start Date": "",
                "End Date": "",
                "Last Modified Date": now,
                "Award Amount (Obligated)": valuation,
                "NAICS Code": "",
                "NAICS Description": "",
                "Awarding Agency": "City of Austin",
                "Place of Performance": ", ".join([x for x in [address, zip_code, "Austin, TX"] if x]),
                "Description": " | ".join([x for x in [
                    f"PermitType={permit_type}" if permit_type else "",
                    f"Issued={issued_date}" if issued_date else "",
                    description
                ] if x]),
                "Award Link": dataset_page_link,
                "Recipient Profile Link": "",
                "Web Search Link": "",
                "Company Website": "",
                "Company Phone": "",
                "Company General Email": "",
                "Responsible Person Name": "",
                "Responsible Person Role": "",
                "Responsible Person Email": "",
                "Responsible Person Phone": "",
                "confidence_score": "80",
                "prediction_rationale": "austin_open_data(+80)",
                "target_flag": "TRUE",
                "recipient_id": award_id,
                "data_source": "Austin Open Data (Socrata) - Issued Construction Permits",
                "data_confidence_level": "High",
                "last_verified_date": now,
                "notes": json.dumps(row, ensure_ascii=False),
            }

            ordered = [""] * len(hmap)
            for header, col_index in hmap.items():
                ordered[col_index - 1] = values.get(header, "")

            rows_to_append.append(ordered)
            existing_ids.add(award_id)
            appended += 1

            if SLEEP_SECONDS:
                time.sleep(SLEEP_SECONDS)

        offset += PAGE_LIMIT

    if rows_to_append:
        ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        print(f"✅ Appended {len(rows_to_append)} rows into {TAB_NAME}.")
    else:
        print("No new rows appended (deduped or empty).")

    print("Done.")

if __name__ == "__main__":
    main()
