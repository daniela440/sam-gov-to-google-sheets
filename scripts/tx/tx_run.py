import os
import json
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# =============================
# CONFIG
# =============================

SOCRATA_DOMAIN = "data.austintexas.gov"
DATASET_ID = os.environ.get("TX_DATASET_ID", "3syk-w9eu")
SOCRATA_BASE = f"https://{SOCRATA_DOMAIN}/resource/{DATASET_ID}.json"

# Filters
ISSUED_SINCE = os.environ.get("TX_ISSUED_SINCE", "2025-12-15")  # YYYY-MM-DD
WORK_CLASS_REQUIRED = os.environ.get("TX_WORK_CLASS_REQUIRED", "NEW")  # we only want NEW work class

# Google Sheet
SHEET_ID = os.environ.get("TX_SHEET_ID")
TAB_NAME = os.environ.get("TX_TAB_NAME", "Awards_Raw_TX")
CREDS_ENV = "ATX_GOOGLE_CREDENTIALS_JSON"  # you said the secret is already named this

# Runtime controls
MAX_NEW = int(os.environ.get("TX_MAX_NEW", "2000"))
SLEEP_SECONDS = float(os.environ.get("TX_SLEEP_SECONDS", "0.05"))
PAGE_SIZE = int(os.environ.get("TX_PAGE_SIZE", "5000"))
HTTP_TIMEOUT = int(os.environ.get("TX_HTTP_TIMEOUT", "45"))

# Optional Socrata app token (helps avoid throttling; not required)
SOCRATA_APP_TOKEN = os.environ.get("TX_SOCRATA_APP_TOKEN", "").strip()

# Website enrichment
ENRICH_WEBSITE = os.environ.get("TX_ENRICH_WEBSITE", "true").lower() == "true"
WEBSITE_LOOKUP_CAP = int(os.environ.get("TX_WEBSITE_LOOKUP_CAP", "400"))


# =============================
# HELPERS
# =============================

REQUIRED_HEADERS = [
    "Contractor Trade",
    "Contractor Company Name",
    "Contractor Full Name",
    "Contractor Address",
    "Contractor City",
    "Contractor Phone",
    "Contractor Website",
    "Issued Date",
    "Expiration Date",
    "Permit Class Mapped",
    "Work Class",
    "Project Name",
    "Description",
    "Housing Units",
    "Total Job Valuation",
]


def normalize_str(x) -> str:
    return str(x).strip() if x is not None else ""


def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def parse_iso_date_any(s: str) -> str:
    """
    Normalize Socrata timestamps to YYYY-MM-DD when possible.
    Keeps original if parsing fails.
    """
    s = normalize_str(s)
    if not s:
        return ""
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return s


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
        raise RuntimeError("Row 1 headers are empty.")
    # Exact match mapping
    return {h: i + 1 for i, h in enumerate(headers)}


def ensure_required_headers_present(ws) -> None:
    headers = ws.row_values(1)
    missing = [h for h in REQUIRED_HEADERS if h not in headers]
    if missing:
        raise RuntimeError(
            "Your sheet header row is missing required columns: "
            + ", ".join(missing)
            + ".\nFix row 1 to match exactly."
        )


def load_existing_keys(ws) -> set:
    """
    Since you removed Award ID and other prior identifiers,
    we dedupe using a stable key stored in the Note/hidden? column is gone.
    Therefore we dedupe by a composite of:
      Issued Date + Contractor Company Name + Project Name + Total Job Valuation
    (good enough to prevent most duplicates in sheet)
    """
    # Build dedupe keys from existing rows (read relevant columns by header).
    headers = ws.row_values(1)
    h = {name: idx for idx, name in enumerate(headers)}

    def col(name: str) -> int:
        return h.get(name, -1)

    idx_issued = col("Issued Date")
    idx_company = col("Contractor Company Name")
    idx_project = col("Project Name")
    idx_val = col("Total Job Valuation")

    # If any are missing, ensure_required_headers_present should have caught it.
    vals = ws.get_all_values()
    keys = set()
    for row in vals[1:]:
        issued = row[idx_issued] if idx_issued >= 0 and idx_issued < len(row) else ""
        company = row[idx_company] if idx_company >= 0 and idx_company < len(row) else ""
        project = row[idx_project] if idx_project >= 0 and idx_project < len(row) else ""
        val = row[idx_val] if idx_val >= 0 and idx_val < len(row) else ""
        k = f"{issued}|{company}|{project}|{val}".strip()
        if k and k != "|||":
            keys.add(k)
    return keys


def pick(d: Dict[str, object], keys: List[str]) -> str:
    for k in keys:
        if k in d:
            v = normalize_str(d.get(k))
            if v:
                return v
    return ""


def socrata_headers() -> Dict[str, str]:
    h = {"Accept": "application/json"}
    if SOCRATA_APP_TOKEN:
        h["X-App-Token"] = SOCRATA_APP_TOKEN
    return h


def socrata_get(params: Dict[str, str]) -> List[Dict[str, object]]:
    r = requests.get(SOCRATA_BASE, params=params, headers=socrata_headers(), timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        return []
    return data


def ddg_company_website(company_name: str, city: str = "", address: str = "", phone: str = "") -> str:
    """
    Lightweight website discovery using DuckDuckGo HTML results.
    Query uses company name + (city/address) + phone if present to disambiguate.
    """
    company_name = normalize_str(company_name)
    if not company_name:
        return ""

    parts = [company_name]
    if city:
        parts.append(city)
    if address:
        parts.append(address)
    if phone:
        parts.append(phone)
    parts.append("website")
    q = " ".join([p for p in parts if p])

    url = "https://duckduckgo.com/html/"
    try:
        resp = requests.post(url, data={"q": q}, timeout=HTTP_TIMEOUT)
        if resp.status_code != 200:
            return ""

        html = resp.text

        # Extract first result URL
        marker = 'class="result__a" href="'
        idx = html.find(marker)
        if idx == -1:
            return ""
        start = idx + len(marker)
        end = html.find('"', start)
        if end == -1:
            return ""
        found = html[start:end].strip()

        # Filter obvious non-sites, try a second result if needed
        bad = (
            "linkedin.com", "facebook.com", "instagram.com", "yelp.com", "mapquest.com",
            "opencorporates.com", "bloomberg.com", "dnb.com", "zoominfo.com", "buzzfile.com",
            "chamberofcommerce.com", "yellowpages.com", "bbb.org"
        )

        def is_bad(u: str) -> bool:
            ul = u.lower()
            return any(b in ul for b in bad)

        if not found.startswith("http"):
            return ""
        if is_bad(found):
            idx2 = html.find(marker, end)
            if idx2 != -1:
                start2 = idx2 + len(marker)
                end2 = html.find('"', start2)
                if end2 != -1:
                    found2 = html[start2:end2].strip()
                    if found2.startswith("http") and not is_bad(found2):
                        return found2
            return ""

        return found
    except Exception:
        return ""


# =============================
# CORE LOGIC
# =============================

def build_where_clause() -> str:
    """
    Filter:
      issue_date >= ISSUED_SINCE
      work_class == NEW (case-insensitive)
    """
    since_ts = f"{ISSUED_SINCE}T00:00:00.000"
    return f"issue_date >= '{since_ts}' AND upper(work_class) = '{WORK_CLASS_REQUIRED.upper()}'"


def fetch_permits() -> List[Dict[str, object]]:
    where = build_where_clause()
    print(f"TX | Fetching permits where: {where}")

    out: List[Dict[str, object]] = []
    offset = 0

    while True:
        params = {
            "$where": where,
            "$limit": str(PAGE_SIZE),
            "$offset": str(offset),
            "$order": "issue_date ASC, permit_number ASC",
        }
        batch = socrata_get(params)
        if not batch:
            break

        out.extend(batch)
        offset += len(batch)
        print(f"TX | fetched {len(batch)} (total={len(out)})")

        # Safety cap
        if len(out) >= MAX_NEW * 10:
            print("TX | safety stop: pulled enough raw records for this run.")
            break

        time.sleep(SLEEP_SECONDS)

    return out


def main():
    if not SHEET_ID:
        raise RuntimeError("Missing TX_SHEET_ID env var")
    if not TAB_NAME:
        raise RuntimeError("Missing TX_TAB_NAME env var")

    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(TAB_NAME)

    ensure_required_headers_present(ws)
    hmap = header_map(ws)
    existing_keys = load_existing_keys(ws)

    rows = fetch_permits()
    print(f"TX | total raw rows fetched: {len(rows)}")
    if not rows:
        print("TX | No rows returned for criteria.")
        return

    rows_to_append: List[List[str]] = []
    appended = 0
    website_lookups = 0

    for r in rows:
        if appended >= MAX_NEW:
            break

        # ---- Field extraction (robust picks: dataset names can vary slightly) ----
        # Required/requested fields
        permit_class_mapped = pick(r, ["permit_class_mapped", "permit_class_mapping"])
        work_class = pick(r, ["work_class"])
        project_name = pick(r, ["project_name", "permit_location", "project", "location"])
        description = pick(r, ["description", "work_description", "description_of_work"])

        issued_raw = pick(r, ["issue_date", "issued_date"])
        exp_raw = pick(r, ["expiration_date", "expires_date", "exp_date", "expiration"])

        issued_date = parse_iso_date_any(issued_raw)
        expiration_date = parse_iso_date_any(exp_raw)

        housing_units = pick(r, ["housing_units", "units", "unit_count"])
        total_job_valuation = pick(r, ["total_job_valuation", "valuation", "job_valuation", "declared_valuation"])

        contractor_trade = pick(r, ["contractor_trade", "trade"])
        contractor_company = pick(r, ["contractor_company_name", "contractor_company", "company_name"])
        contractor_full_name = pick(r, ["contractor_full_name", "contractor_name", "full_name", "name"])
        contractor_phone = pick(r, ["contractor_phone", "phone"])
        contractor_address = pick(r, ["contractor_address", "contractor_address1", "contractor_address_1", "contractor_address2", "contractor_address_2"])
        contractor_city = pick(r, ["contractor_city", "city"])

        # ---- Dedupe key ----
        dedupe_key = f"{issued_date}|{contractor_company}|{project_name}|{total_job_valuation}".strip()
        if dedupe_key in existing_keys:
            continue

        # ---- Website enrichment ----
        contractor_website = ""
        if ENRICH_WEBSITE and contractor_company and website_lookups < WEBSITE_LOOKUP_CAP:
            contractor_website = ddg_company_website(
                contractor_company,
                city=contractor_city,
                address=contractor_address,
                phone=contractor_phone
            )
            website_lookups += 1
            time.sleep(SLEEP_SECONDS)

        # ---- Build row matching the sheet headers exactly ----
        row_values = {
            "Contractor Trade": contractor_trade,
            "Contractor Company Name": contractor_company,
            "Contractor Full Name": contractor_full_name,
            "Contractor Address": contractor_address,
            "Contractor City": contractor_city,
            "Contractor Phone": contractor_phone,
            "Contractor Website": contractor_website,
            "Issued Date": issued_date,
            "Expiration Date": expiration_date,
            "Permit Class Mapped": permit_class_mapped,
            "Work Class": work_class,
            "Project Name": project_name,
            "Description": description,
            "Housing Units": housing_units,
            "Total Job Valuation": total_job_valuation,
        }

        # Order columns according to header row in the sheet
        ordered = [""] * len(hmap)
        for header, col_index in hmap.items():
            if header in row_values:
                ordered[col_index - 1] = row_values.get(header, "")
            else:
                # If sheet has any extra columns beyond the required set, leave blank.
                ordered[col_index - 1] = ""

        rows_to_append.append(ordered)
        existing_keys.add(dedupe_key)
        appended += 1

        time.sleep(SLEEP_SECONDS)

    if rows_to_append:
        ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        print(f"✅ Appended {len(rows_to_append)} rows into {TAB_NAME}.")
    else:
        print("No new rows appended (deduped or empty).")

    print(f"TX | website lookups used: {website_lookups} (cap={WEBSITE_LOOKUP_CAP})")
    print("Done.")


if __name__ == "__main__":
    main()
