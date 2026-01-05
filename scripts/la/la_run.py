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
# LADBS permits dataset (City of LA Open Data / Socrata)
# -----------------------------
LADBS_DATASET_ID = "pi9x-tg5x"  # Building Permits Issued from 2020 to Present (N)


# -----------------------------
# Utilities
# -----------------------------
def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def normalize_str(x) -> str:
    return str(x).strip() if x is not None else ""


def first_present(record: dict, candidates: list[str]) -> str:
    for k in candidates:
        v = normalize_str(record.get(k))
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
def socrata_get(dataset_id: str, where: str | None, select: str | None, order: str | None, limit: int, offset: int) -> list[dict]:
    base = f"https://data.lacity.org/resource/{dataset_id}.json"
    headers = {}

    token = os.environ.get("LA_SOCRATA_APP_TOKEN")
    if token:
        headers["X-App-Token"] = token

    params = {"$limit": limit, "$offset": offset}
    if where:
        params["$where"] = where
    if select:
        params["$select"] = select
    if order:
        params["$order"] = order

    r = requests.get(base, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()


# -----------------------------
# CSLB enrichment (authoritative)
# Approach: scrape CSLB contractor detail by license # (<= 10/day safe)
# CSLB detail pages work as /<license_number> (e.g., https://www.cslb.ca.gov/1)
# -----------------------------
def extract_license_number(text: str) -> str:
    """
    Attempt to extract a CA contractor license number from a messy contractor field.
    We prefer 6–8 digit sequences and return the first plausible match.
    """
    t = normalize_str(text)
    if not t:
        return ""

    # Common patterns: "license # 1234567", "Lic#123456", "CSLB 1234567", etc.
    m = re.search(r"(?:licen[cs]e|cslb|lic|lic#)\s*[:#]?\s*([0-9]{6,8})", t, flags=re.IGNORECASE)
    if m:
        return m.group(1)

    # Fallback: any 6-8 digit number in text
    m2 = re.search(r"\b([0-9]{6,8})\b", t)
    return m2.group(1) if m2 else ""


def cslb_fetch_by_license(license_number: str, timeout: int = 30) -> dict:
    """
    Fetch CSLB contractor detail page and parse key fields from HTML.
    Returns dict with:
      business_name, address, phone, status, classifications (list[str])
    """
    if not license_number:
        return {}

    url = f"https://www.cslb.ca.gov/{license_number}"
    r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    if r.status_code != 200:
        return {}

    html = r.text

    # Very lightweight parsing using regex (stable enough for key fields).
    # Business name often appears in title/header area:
    business_name = ""
    m = re.search(r"Contractor's License Detail for License #\s*" + re.escape(license_number) + r".*?</h1>", html, flags=re.IGNORECASE | re.DOTALL)
    # Not always helpful; fallback to finding "Business Name" label blocks:
    if not business_name:
        m2 = re.search(r"Business Name\s*</[^>]+>\s*<[^>]+>\s*([^<]+)", html, flags=re.IGNORECASE)
        if m2:
            business_name = normalize_str(m2.group(1))

    # Status:
    status = ""
    m3 = re.search(r"License Status\s*</[^>]+>\s*<[^>]+>\s*([^<]+)", html, flags=re.IGNORECASE)
    if m3:
        status = normalize_str(m3.group(1))

    # Phone:
    phone = ""
    m4 = re.search(r"Business Phone\s*</[^>]+>\s*<[^>]+>\s*([^<]+)", html, flags=re.IGNORECASE)
    if m4:
        phone = normalize_str(m4.group(1))

    # Address (best-effort)
    address = ""
    m5 = re.search(r"Business Address\s*</[^>]+>\s*<[^>]+>\s*([^<]+)", html, flags=re.IGNORECASE)
    if m5:
        address = normalize_str(m5.group(1))

    # Classifications (can be multiple)
    classifications = []
    # This label varies; try a few patterns
    for pat in [
        r"Classification\s*\(s\)\s*</[^>]+>\s*<[^>]+>\s*([^<]+)",
        r"Classifications\s*</[^>]+>\s*<[^>]+>\s*([^<]+)",
    ]:
        m6 = re.search(pat, html, flags=re.IGNORECASE)
        if m6:
            raw = normalize_str(m6.group(1))
            # Split on commas/semicolons
            classifications = [c.strip() for c in re.split(r"[;,]", raw) if c.strip()]
            break

    # If we couldn't parse business name via label, try a conservative fallback:
    if not business_name:
        # sometimes appears as <h2>BUSINESS NAME</h2>
        m7 = re.search(r"<h2[^>]*>\s*([^<]+)\s*</h2>", html, flags=re.IGNORECASE)
        if m7:
            business_name = normalize_str(m7.group(1))

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
# We keep it conservative. If unknown -> 236220.
# -----------------------------
ALLOWED_NAICS = {"238210", "236220", "237310", "238220", "238120"}


def infer_naics_from_cslb(classifications: list[str]) -> str:
    """
    Best-effort mapping from CSLB class codes/names to NAICS.
    - Electrical -> 238210
    - Plumbing/HVAC -> 238220
    - Structural steel -> 238120
    - Heavy civil (rare in LADBS permits) -> 237310
    - Default -> 236220 (Commercial/Institutional)
    """
    text = " ".join(classifications or []).lower()

    # Common CSLB class codes:
    # C-10 Electrical, C-36 Plumbing, C-20 HVAC
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
# 2026 start-date logic (based on 2025 issue_date)
# Jan-Jun 2025 -> Q2 2026
# Jul-Sep 2025 -> Q3 2026
# Oct-Dec 2025 -> Q4 2026
# -----------------------------
def start_date_from_issue_date(issue_date: str) -> str:
    if not issue_date:
        return ""
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
# DuckDuckGo enrichment (website only, lightweight)
# -----------------------------
def ddg_find_website(query: str, timeout: int = 30) -> str:
    """
    Very lightweight DDG HTML scrape:
      - query DDG HTML endpoint
      - return first plausible website domain (excluding social/search)
    Safe because we rate-limit to <= 10/day.
    """
    q = normalize_str(query)
    if not q:
        return ""

    url = "https://duckduckgo.com/html/"
    r = requests.get(url, params={"q": q}, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    if r.status_code != 200:
        return ""

    html = r.text

    # Extract result URLs
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
        "mapquest.com",
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
        # Prefer root domain
        m = re.match(r"^(https?://[^/]+)", u)
        if m:
            return m.group(1)
        return u

    return ""


# -----------------------------
# Row scoring (simple, explainable)
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

    # Cap
    score = min(score, 100)
    return score, "; ".join(why) if why else "no_signals"


# -----------------------------
# Main
# -----------------------------
def main():
    # Required env
    sheet_id = os.environ.get("LA_SHEET_ID")
    tab_name = os.environ.get("LA_TAB_NAME")
    if not sheet_id or not tab_name:
        raise RuntimeError("Missing LA_SHEET_ID or LA_TAB_NAME")

    max_new = int(os.environ.get("LA_MAX_NEW", "10"))
    sleep_seconds = float(os.environ.get("LA_SLEEP_SECONDS", "1.0"))

    issue_year = int(os.environ.get("LA_ISSUE_YEAR", "2025"))
    min_valuation = float(os.environ.get("LA_MIN_VALUATION", "50000"))

    # Connect sheet
    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab_name)
    hmap = header_map(ws)
    existing_ids = load_existing_award_ids(ws)

    now = utc_now_str()
    rows_to_append = []

    # SoQL filter: issued in the chosen year + valuation threshold
    # Note: issue_date is a date/time field in this dataset.
    where = (
        f"issue_date >= '{issue_year}-01-01T00:00:00.000' AND "
        f"issue_date < '{issue_year+1}-01-01T00:00:00.000' AND "
        f"valuation IS NOT NULL AND "
        f"valuation >= {min_valuation}"
    )

    # Pull only the fields we need (reduces payload)
    select = ",".join([
        "permit_nbr",
        "primary_address",
        "zip_code",
        "issue_date",
        "status_desc",
        "status_date",
        "valuation",
        "work_desc",
        "permit_type",
        "permit_sub_type",
        "permit_group",
        "use_desc",
        "construction",
        "contractor",
        ":updated_at",
    ])

    # Most recent first (good for incremental runs)
    order = "issue_date DESC"

    appended = 0
    offset = 0
    page_size = 200

    while appended < max_new:
        permits = socrata_get(
            dataset_id=LADBS_DATASET_ID,
            where=where,
            select=select,
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

            permit_nbr = first_present(p, ["permit_nbr"])
            if not permit_nbr:
                continue

            # Dedupe
            if permit_nbr in existing_ids:
                continue

            issue_date = first_present(p, ["issue_date"])
            start_date = start_date_from_issue_date(issue_date)
            if not start_date:
                continue  # strict: only mapped 2026 targets

            contractor_raw = first_present(p, ["contractor"])
            license_number = extract_license_number(contractor_raw)

            # CSLB join (only if we found a license number)
            cslb = {}
            if license_number:
                cslb = cslb_fetch_by_license(license_number)
                time.sleep(0.5)  # be polite to CSLB

            business_name = normalize_str(cslb.get("business_name")) or normalize_str(contractor_raw)
            cslb_status = normalize_str(cslb.get("status"))
            cslb_active = "active" in cslb_status.lower() if cslb_status else False

            # Enforce your rule: Active-only CSLB (but only when we have CSLB data)
            # If no license number was found, we skip (commercial-first + quality control).
            if not license_number:
                continue
            if not cslb_active:
                continue

            classifications = cslb.get("classifications") or []
            naics = infer_naics_from_cslb(classifications)
            if naics not in ALLOWED_NAICS:
                continue

            # Valuation
            valuation_raw = first_present(p, ["valuation"])
            try:
                valuation = float(str(valuation_raw).replace(",", "").replace("$", ""))
            except Exception:
                valuation = 0.0

            # Optional website enrichment (DDG) - lightweight and rate-limited by max_new
            website = ddg_find_website(f"{business_name} Los Angeles contractor") if business_name else ""
            time.sleep(0.25)

            # Place of performance
            address = first_present(p, ["primary_address"])
            zip_code = first_present(p, ["zip_code"])
            pop = f"{address} {zip_code}".strip()

            # Description (permit work)
            work_desc = first_present(p, ["work_desc"])
            use_desc = first_present(p, ["use_desc"])
            permit_type = first_present(p, ["permit_type"])
            permit_sub_type = first_present(p, ["permit_sub_type"])
            permit_group = first_present(p, ["permit_group"])

            desc_parts = [x for x in [permit_group, permit_type, permit_sub_type, use_desc, work_desc] if x]
            description = " | ".join(desc_parts)

            # Links
            award_link = (
                f"https://data.lacity.org/resource/{LADBS_DATASET_ID}.json?"
                f"$where=permit_nbr='{permit_nbr}'"
            )
            cslb_profile = normalize_str(cslb.get("profile_url"))
            web_search = f"https://www.google.com/search?q={requests.utils.quote(business_name)}+Los+Angeles"

            # Confidence
            score, rationale = confidence_score(valuation, has_license=bool(license_number), cslb_active=cslb_active)

            recipient_id = hashlib.md5(permit_nbr.encode("utf-8")).hexdigest()

            values = {
                "Award ID": permit_nbr,
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
                "Description": description,
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

            # Build ordered row in header order
            ordered_row = [""] * len(hmap)
            for header, col_index in hmap.items():
                ordered_row[col_index - 1] = values.get(header, "")

            rows_to_append.append(ordered_row)
            existing_ids.add(permit_nbr)
            appended += 1

            time.sleep(sleep_seconds)

        # Gentle pacing between pages
        time.sleep(0.5)

    if rows_to_append:
        ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        print(f"✅ Appended {len(rows_to_append)} rows into {tab_name}.")
    else:
        print("No rows appended (filters may be too strict or no CSLB license numbers found).")

    print("Done.")


if __name__ == "__main__":
    main()
