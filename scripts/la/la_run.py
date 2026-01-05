import os
import re
import json
import time
import hashlib
from datetime import datetime, timezone

import requests
import gspread
from google.oauth2.service_account import Credentials


# -----------------------------
# LADBS dataset that includes contractor info
# -----------------------------
# This replaces pi9x-tg5x (which lacks contractor/license fields).
LADBS_DATASET_ID = "hbkd-qubn"  # LADBS-Permits (has contractor business name / license fields)


# -----------------------------
# Utilities
# -----------------------------
def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def normalize_str(x) -> str:
    return str(x).strip() if x is not None else ""


def first_present(record: dict, candidates: list[str]) -> str:
    for k in candidates:
        v = record.get(k)
        v = normalize_str(v)
        if v:
            return v
    return ""


def year_of_iso(d: str) -> int | None:
    if not d:
        return None
    try:
        return int(d[:4])
    except Exception:
        return None


def month_of_iso(d: str) -> int | None:
    if not d or len(d) < 7:
        return None
    try:
        return int(d[5:7])
    except Exception:
        return None


def to_float(x: str) -> float:
    s = normalize_str(x)
    if not s:
        return 0.0
    try:
        return float(s.replace(",", "").replace("$", ""))
    except Exception:
        return 0.0


# -----------------------------
# Google Sheets
# -----------------------------
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


def header_map(ws) -> dict:
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("Row 1 headers are empty. Paste your column headers in row 1.")
    return {h: i + 1 for i, h in enumerate(headers)}  # 1-based


def load_existing_award_ids(ws) -> set[str]:
    col_values = ws.col_values(1)  # Column A = Award ID
    return {v.strip() for v in col_values[1:] if v and v.strip()}


# -----------------------------
# Socrata (SODA) client
# -----------------------------
def socrata_get(domain: str, dataset_id: str, where: str | None, order: str | None, limit: int, offset: int) -> list[dict]:
    base = f"https://{domain}/resource/{dataset_id}.json"
    headers = {}

    token = os.environ.get("LA_SOCRATA_APP_TOKEN")
    if token:
        headers["X-App-Token"] = token

    params = {"$limit": limit, "$offset": offset}
    if where:
        params["$where"] = where
    if order:
        params["$order"] = order

    r = requests.get(base, params=params, headers=headers, timeout=60)
    # Helpful debugging if Socrata rejects query
    if r.status_code >= 400:
        raise RuntimeError(f"Socrata error {r.status_code}: {r.text[:500]} | URL={r.url}")
    return r.json()


# -----------------------------
# CSLB enrichment (authoritative) via license detail page
# -----------------------------
def extract_license_number(text: str) -> str:
    t = normalize_str(text)
    if not t:
        return ""

    m = re.search(r"(?:licen[cs]e|cslb|lic|lic#)\s*[:#]?\s*([0-9]{6,8})", t, flags=re.IGNORECASE)
    if m:
        return m.group(1)

    m2 = re.search(r"\b([0-9]{6,8})\b", t)
    return m2.group(1) if m2 else ""


def cslb_fetch_by_license(license_number: str, timeout: int = 30) -> dict:
    if not license_number:
        return {}

    url = f"https://www.cslb.ca.gov/{license_number}"
    r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    if r.status_code != 200:
        return {}

    html = r.text

    def rx(label: str) -> str:
        m = re.search(rf"{label}\s*</[^>]+>\s*<[^>]+>\s*([^<]+)", html, flags=re.IGNORECASE)
        return normalize_str(m.group(1)) if m else ""

    business_name = rx("Business Name")
    status = rx("License Status")
    phone = rx("Business Phone")
    address = rx("Business Address")

    classifications = []
    m = re.search(r"Classification\s*\(s\)\s*</[^>]+>\s*<[^>]+>\s*([^<]+)", html, flags=re.IGNORECASE)
    if m:
        raw = normalize_str(m.group(1))
        classifications = [c.strip() for c in re.split(r"[;,]", raw) if c.strip()]

    if not business_name:
        m2 = re.search(r"<h2[^>]*>\s*([^<]+)\s*</h2>", html, flags=re.IGNORECASE)
        if m2:
            business_name = normalize_str(m2.group(1))

    return {
        "license_number": license_number,
        "business_name": business_name,
        "address": address,
        "phone": phone,
        "status": status,
        "classifications": classifications,
        "profile_url": url,
    }


# -----------------------------
# NAICS mapping (commercial-first)
# -----------------------------
ALLOWED_NAICS = {"238210", "236220", "237310", "238220", "238120"}


def infer_naics_from_cslb(classifications: list[str]) -> str:
    text = " ".join(classifications or []).lower()

    if "c-10" in text or "elect" in text:
        return "238210"
    if "c-36" in text or "plumb" in text or "c-20" in text or "hvac" in text or "air conditioning" in text or "heating" in text:
        return "238220"
    if "struct" in text or "steel" in text or "precast" in text:
        return "238120"
    if "highway" in text or "street" in text or "bridge" in text:
        return "237310"
    return "236220"


def naics_description(naics: str) -> str:
    return {
        "238210": "Electrical Contractors and Other Wiring Installation Contractors",
        "236220": "Commercial and Institutional Building Construction",
        "237310": "Highway, Street, and Bridge Construction",
        "238220": "Plumbing, Heating, and Air-Conditioning Contractors",
        "238120": "Structural Steel and Precast Concrete Contractors",
    }.get(naics, "")


# -----------------------------
# 2026 start-date logic (based on 2025 issue date)
# -----------------------------
def start_date_from_issue_date(issue_date: str) -> str:
    y = year_of_iso(issue_date)
    m = month_of_iso(issue_date)
    if y != 2025 or m is None:
        return ""
    if 1 <= m <= 6:
        return "2026-04-01"
    if 7 <= m <= 9:
        return "2026-07-01"
    return "2026-10-01"


# -----------------------------
# DuckDuckGo website enrichment (optional)
# -----------------------------
def ddg_find_website(query: str, timeout: int = 30) -> str:
    q = normalize_str(query)
    if not q:
        return ""

    url = "https://duckduckgo.com/html/"
    r = requests.get(url, params={"q": q}, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    if r.status_code != 200:
        return ""

    html = r.text
    links = re.findall(r'href="(https?://[^"]+)"', html, flags=re.IGNORECASE)
    if not links:
        return ""

    blocklist = (
        "duckduckgo.com",
        "google.com",
        "bing.com",
        "yahoo.com",
        "facebook.com",
        "instagram.com",
        "linkedin.com",
        "yelp.com",
        "yellowpages.com",
        "bbb.org",
        "opencorporates.com",
        "dnb.com",
        "zoominfo.com",
    )

    def clean(u: str) -> str:
        return u.replace("&amp;", "&").strip()

    for u in links:
        u = clean(u)
        if any(b in u.lower() for b in blocklist):
            continue
        m = re.match(r"^(https?://[^/]+)", u)
        return m.group(1) if m else u

    return ""


# -----------------------------
# Confidence scoring
# -----------------------------
def confidence_score(valuation: float, has_license: bool, cslb_active: bool) -> tuple[int, str]:
    score = 0
    why = []

    if has_license:
        score += 35
        why.append("license_present(+35)")
    if cslb_active:
        score += 35
        why.append("cslb_active(+35)")

    if valuation >= 50_000_000:
        score += 20
        why.append("valuation>=50M(+20)")
    elif valuation >= 10_000_000:
        score += 15
        why.append("valuation>=10M(+15)")
    elif valuation >= 1_000_000:
        score += 10
        why.append("valuation>=1M(+10)")
    elif valuation >= 50_000:
        score += 5
        why.append("valuation>=50k(+5)")

    return min(score, 100), "; ".join(why) if why else "no_signals"


# -----------------------------
# Main
# -----------------------------
def main():
    sheet_id = os.environ.get("LA_SHEET_ID")
    tab_name = os.environ.get("LA_TAB_NAME")
    if not sheet_id or not tab_name:
        raise RuntimeError("Missing LA_SHEET_ID or LA_TAB_NAME")

    max_new = int(os.environ.get("LA_MAX_NEW", "10"))
    sleep_seconds = float(os.environ.get("LA_SLEEP_SECONDS", "1.0"))
    min_valuation = float(os.environ.get("LA_MIN_VALUATION", "50000"))
    issue_year = int(os.environ.get("LA_ISSUE_YEAR", "2025"))

    # Connect to sheet
    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab_name)
    hmap = header_map(ws)
    existing_ids = load_existing_award_ids(ws)

    now = utc_now_str()
    rows_to_append = []

    # IMPORTANT: hbkd-qubn has different schema than pi9x-tg5x.
    # We keep where/order flexible and schema-safe.

    issue_date_fields = ["issue_date", "issued_date", "permit_issue_date", "date_issued", "issue_dt"]
    valuation_fields = ["valuation", "permit_valuation", "estimated_cost", "est_cost", "valuation_amount"]
    permit_id_fields = ["pcis_permit", "pcis_permit_number", "pcis_permit_", "permit_nbr", "permit_number", "permit_id"]
    contractor_name_fields = [
        "contractor_s_business_name",
        "contractor_business_name",
        "contractor_name",
        "contractor",
        "contractors_business_name",
    ]
    contractor_license_fields = ["license", "license_number", "license_no", "license_nbr", "contractor_license", "license_"]
    address_fields = ["full_address", "address", "primary_address", "address_1", "site_address"]
    zip_fields = ["zip_code", "zip", "zipcode"]
    work_desc_fields = ["work_desc", "description", "permit_description", "work_description"]

    # SoQL: try to filter by issue date if field exists (we will do it in Python anyway).
    # We'll keep the WHERE minimal to avoid 400s.
    where = None
    order = None

    appended = 0
    offset = 0
    page_size = 200

    while appended < max_new:
        permits = socrata_get(
            domain="data.lacity.org",
            dataset_id=LADBS_DATASET_ID,
            where=where,
            order=order,
            limit=page_size,
            offset=offset,
        )
        if not permits:
            break
        offset += page_size

        for p in permits:
            if appended >= max_new:
                break

            permit_id = first_present(p, permit_id_fields)
            if not permit_id:
                continue
            if permit_id in existing_ids:
                continue

            issue_date = first_present(p, issue_date_fields)
            if year_of_iso(issue_date) != issue_year:
                continue

            start_date = start_date_from_issue_date(issue_date)
            if not start_date:
                continue  # strict: only mapped 2026 targets

            valuation_raw = first_present(p, valuation_fields)
            valuation = to_float(valuation_raw)
            if valuation < min_valuation:
                continue

            contractor_name = first_present(p, contractor_name_fields)
            contractor_license = first_present(p, contractor_license_fields)
            license_number = extract_license_number(contractor_license) or extract_license_number(contractor_name)

            # Enforce: require license number for CSLB join quality
            if not license_number:
                continue

            cslb = cslb_fetch_by_license(license_number)
            time.sleep(0.5)

            business_name = normalize_str(cslb.get("business_name")) or contractor_name
            cslb_status = normalize_str(cslb.get("status"))
            cslb_active = "active" in cslb_status.lower() if cslb_status else False

            # Enforce your rule: Active-only CSLB
            if not cslb_active:
                continue

            classifications = cslb.get("classifications") or []
            naics = infer_naics_from_cslb(classifications)
            if naics not in ALLOWED_NAICS:
                continue

            # Website via DDG (safe due to LA_MAX_NEW=10)
            website = ddg_find_website(f"{business_name} Los Angeles contractor") if business_name else ""
            time.sleep(0.25)

            address = first_present(p, address_fields)
            zip_code = first_present(p, zip_fields)
            pop = f"{address} {zip_code}".strip()

            work_desc = first_present(p, work_desc_fields)

            award_link = (
                f"https://data.lacity.org/resource/{LADBS_DATASET_ID}.json?"
                f"$where=pcis_permit='{permit_id}'"
            )
            cslb_profile = normalize_str(cslb.get("profile_url"))
            web_search = f"https://www.google.com/search?q={requests.utils.quote(business_name)}+Los+Angeles"

            score, rationale = confidence_score(valuation, has_license=True, cslb_active=True)
            recipient_id = hashlib.md5(permit_id.encode("utf-8")).hexdigest()

            values = {
                "Award ID": permit_id,
                "Recipient (Company)": business_name,
                "Recipient UEI": "",
                "Parent Recipient UEI": "",
                "Parent Recipient DUNS": "",
                "Recipient (HQ) Address": normalize_str(cslb.get("address")),
                "Start Date": start_date,
                "End Date": "",
                "Last Modified Date": now,
                "Award Amount (Obligated)": valuation_raw,
                "NAICS Code": naics,
                "NAICS Description": naics_description(naics),
                "Awarding Agency": "Los Angeles Department of Building and Safety",
                "Place of Performance": pop,
                "Description": work_desc,
                "Award Link": award_link,
                "Recipient Profile Link": cslb_profile,
                "Web Search Link": web_search,
                "Company Website": website,
                "Company Phone": normalize_str(cslb.get("phone")),
                "Company General Email": "",
                "Responsible Person Name": "",
                "Responsible Person Role": "",
                "Responsible Person Email": "",
                "Responsible Person Phone": "",
                "confidence_score": str(score),
                "prediction_rationale": rationale,
                "target_flag": "TRUE",
                "recipient_id": recipient_id,
                "data_source": "LA Open Data (LADBS Permits) + CSLB",
                "data_confidence_level": "High" if score >= 85 else "Medium",
                "last_verified_date": now,
                "notes": f"CSLB license {license_number}; status={cslb_status}".strip(),
            }

            ordered_row = [""] * len(hmap)
            for header, col_index in hmap.items():
                ordered_row[col_index - 1] = values.get(header, "")

            rows_to_append.append(ordered_row)
            existing_ids.add(permit_id)
            appended += 1

            time.sleep(sleep_seconds)

        time.sleep(0.5)

    if rows_to_append:
        ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        print(f"✅ Appended {len(rows_to_append)} rows into {tab_name}.")
    else:
        print("No rows appended. Most likely causes:")
        print("- hbkd-qubn rows don’t include license # for recent permits")
        print("- issue_date field naming differs and filtering skipped everything")
        print("- CSLB status not Active for extracted licenses")

    print("Done.")


if __name__ == "__main__":
    main()
