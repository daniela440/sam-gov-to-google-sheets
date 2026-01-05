import os
import json
import time
import hashlib
import re
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

import requests
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials


# =============================
# CONFIG
# =============================

# CSLB Public Data Portal – List by Classification and County
CSLB_LIST_BY_COUNTY_URL = "https://www.cslb.ca.gov/onlineservices/dataportal/ListByCounty.aspx"

# Your allowed NAICS (fixed scope)
ALLOWED_NAICS = {"238210", "236220", "237310", "238220", "238120"}

# We must stay within CSLB portal limits: up to 10 classifications + up to 10 counties.
# LA County only:
TARGET_COUNTY = "Los Angeles"

# Classification selection (<=10):
# - 236220: B (General Building) (+ A sometimes overlaps but we keep A for 237310 signal)
# - 238210: C-10
# - 238220: C-20 (HVAC), C-36 (Plumbing), C-4 (Boiler/Steam)
# - 237310: A (General Engineering), C-32 (Parking/Highway)
# - 238120: C-51 (Structural Steel), C-50 (Reinforcing Steel)
TARGET_CLASSIFICATIONS = ["A", "B", "C-10", "C-20", "C-36", "C-4", "C-32", "C-51", "C-50"]  # 9 items


# CSLB classification -> NAICS mapping (conservative)
CLASS_TO_NAICS = {
    "C-10": "238210",
    "C-20": "238220",
    "C-36": "238220",
    "C-4":  "238220",
    "C-51": "238120",
    "C-50": "238120",
    "A":    "237310",
    "C-32": "237310",
    "B":    "236220",
}

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


def ddg_find_website(query: str, timeout: int = 30) -> str:
    """
    Light-touch DuckDuckGo HTML search.
    Returns a best-effort domain URL. Keep capped per run.
    """
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
        "mapquest.com",
        "wikipedia.org",
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


# =============================
# CSLB portal download (ASP.NET form)
# =============================

def _extract_aspnet_form_fields(soup: BeautifulSoup) -> Dict[str, str]:
    fields = {}
    for inp in soup.select("input"):
        name = inp.get("name")
        if not name:
            continue
        # include hidden + regular inputs
        fields[name] = inp.get("value", "")
    return fields


def _find_multi_select_names(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    """
    CSLB ListByCounty has two multi-selects:
      - classifications
      - counties
    We detect them heuristically: first two <select multiple>.
    """
    selects = soup.find_all("select")
    multi = [s for s in selects if s.has_attr("multiple")]
    if len(multi) >= 2:
        return multi[0].get("name"), multi[1].get("name")
    # fallback: if they don't use "multiple", still try 2 selects on page
    if len(selects) >= 2:
        return selects[0].get("name"), selects[1].get("name")
    return None, None


def _find_submit_button_name(soup: BeautifulSoup) -> Optional[str]:
    """
    CSLB uses an ASP.NET button; name may vary.
    We pick the first submit-ish input/button.
    """
    # input type=submit
    for inp in soup.select("input"):
        if normalize_str(inp.get("type")).lower() in {"submit", "image"}:
            return inp.get("name")
    # <button>
    btn = soup.find("button")
    return btn.get("name") if btn else None


def download_cslb_list_by_county_xls(
    session: requests.Session,
    classifications: List[str],
    counties: List[str],
    timeout: int = 60
) -> bytes:
    """
    Downloads the "Excel (.xls)" output from CSLB ListByCounty.
    Returns raw bytes.
    """

    # 1) GET page to obtain VIEWSTATE, etc.
    r0 = session.get(CSLB_LIST_BY_COUNTY_URL, timeout=timeout)
    r0.raise_for_status()
    soup0 = BeautifulSoup(r0.text, "lxml")

    form_fields = _extract_aspnet_form_fields(soup0)
    class_select_name, county_select_name = _find_multi_select_names(soup0)
    submit_name = _find_submit_button_name(soup0)

    if not class_select_name or not county_select_name:
        raise RuntimeError(
            "Could not locate classification/county select fields on CSLB page. "
            "CSLB page structure may have changed."
        )

    # 2) Prepare POST payload
    payload = dict(form_fields)

    # Multi-select values in ASP.NET are submitted as repeated keys.
    # requests supports this by using a list of tuples.
    post_items = list(payload.items())

    for c in classifications:
        post_items.append((class_select_name, c))
    for cty in counties:
        post_items.append((county_select_name, cty))

    # Some ASP.NET forms require the submit button name to be present
    if submit_name:
        post_items.append((submit_name, "Download"))

    # 3) POST
    r1 = session.post(CSLB_LIST_BY_COUNTY_URL, data=post_items, timeout=timeout)
    r1.raise_for_status()

    return r1.content


def parse_cslb_xls_html_table(xls_bytes: bytes) -> List[Dict[str, str]]:
    """
    CSLB 'xls' is commonly an HTML table.
    We parse table headers + rows to dictionaries.
    """
    text = None
    try:
        text = xls_bytes.decode("utf-8", errors="ignore")
    except Exception:
        text = ""

    if "<table" not in text.lower():
        # If CSLB ever switches to true binary XLS, we fail fast with a clear message.
        raise RuntimeError(
            "CSLB download did not look like an HTML-table XLS. "
            "If CSLB switched to a true .xls binary, we need a different parser."
        )

    soup = BeautifulSoup(text, "lxml")
    table = soup.find("table")
    if not table:
        return []

    # header
    headers = []
    thead = table.find("thead")
    if thead:
        headers = [normalize_str(th.get_text(" ", strip=True)) for th in thead.find_all(["th", "td"])]
    if not headers:
        first_row = table.find("tr")
        if first_row:
            headers = [normalize_str(cell.get_text(" ", strip=True)) for cell in first_row.find_all(["th", "td"])]

    # rows
    results = []
    rows = table.find_all("tr")
    for tr in rows[1:]:  # skip header row
        cells = [normalize_str(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
        if not cells or all(not c for c in cells):
            continue
        # pad / truncate to headers
        if len(cells) < len(headers):
            cells += [""] * (len(headers) - len(cells))
        row = {headers[i]: cells[i] for i in range(min(len(headers), len(cells)))}
        results.append(row)

    return results


# =============================
# Mapping & filtering
# =============================

def infer_naics_from_classifications(classifications_str: str) -> str:
    """
    CSLB file often provides 'Classification(s)' as a comma-separated string.
    We map to your allowed NAICS using CLASS_TO_NAICS.
    If multiple are present, we pick the first match in priority order.
    """
    raw = normalize_str(classifications_str)
    if not raw:
        return ""

    # Normalize tokens like "C10" -> "C-10" if needed
    tokens = []
    for part in re.split(r"[;,/|]\s*|\s{2,}", raw):
        p = normalize_str(part).upper()
        if not p:
            continue
        # Fix "C10" => "C-10"
        m = re.match(r"^(C)\s*-?\s*(\d+)$", p)
        if m:
            p = f"C-{m.group(2)}"
        tokens.append(p)

    # Priority: keep your trade NAICS first, then B
    priority_classes = ["C-10", "C-36", "C-20", "C-4", "C-51", "C-50", "C-32", "A", "B"]
    token_set = set(tokens)

    for c in priority_classes:
        if c in token_set and c in CLASS_TO_NAICS:
            return CLASS_TO_NAICS[c]

    # Fallback: any mapped token
    for t in tokens:
        if t in CLASS_TO_NAICS:
            return CLASS_TO_NAICS[t]

    return ""


def confidence_for_row(status: str, phone: str) -> Tuple[int, str, str]:
    """
    Simple, transparent confidence scoring.
    CSLB list is already curated; main differentiator is 'Active' + phone present.
    """
    s = normalize_str(status).lower()
    p = normalize_str(phone)

    score = 70
    rationale = ["cslb_list_by_county(+70)"]

    if "active" in s:
        score += 15
        rationale.append("status_active(+15)")
    if p:
        score += 5
        rationale.append("phone_present(+5)")

    level = "High" if score >= 85 else "Medium"
    return min(score, 100), "; ".join(rationale), level


# =============================
# Main
# =============================

def main():
    sheet_id = os.environ.get("LA_SHEET_ID")
    tab_name = os.environ.get("LA_TAB_NAME")
    if not sheet_id or not tab_name:
        raise RuntimeError("Missing LA_SHEET_ID or LA_TAB_NAME")

    # Output control
    max_new = int(os.environ.get("LA_MAX_NEW", "200"))  # total append cap per run
    sleep_seconds = float(os.environ.get("LA_SLEEP_SECONDS", "0.2"))

    # DDG enrichment (optional, capped)
    enable_ddg = os.environ.get("LA_ENABLE_DDG", "true").lower() == "true"
    ddg_cap = int(os.environ.get("LA_DDG_DAILY_CAP", "10"))  # you requested 10/day safety

    # Connect sheet
    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab_name)
    hmap = header_map(ws)
    existing_ids = load_existing_award_ids(ws)

    # 1) Download CSLB list for LA County + target classifications
    sess = requests.Session()
    sess.headers.update({"User-Agent": "Mozilla/5.0"})

    print(f"Downloading CSLB list for county={TARGET_COUNTY} classifications={TARGET_CLASSIFICATIONS} ...")
    xls_bytes = download_cslb_list_by_county_xls(
        session=sess,
        classifications=TARGET_CLASSIFICATIONS,
        counties=[TARGET_COUNTY],
    )

    # 2) Parse
    rows = parse_cslb_xls_html_table(xls_bytes)
    print(f"Parsed {len(rows)} CSLB rows from download.")

    # 3) Append into sheet (dedupe by License Number)
    now = utc_now_str()
    rows_to_append = []
    ddg_used = 0
    appended = 0

    # Try to resolve likely column names from CSLB output
    # (Portal content can change, so we use multi-candidate extraction.)
    def col(row: dict, candidates: List[str]) -> str:
        for c in candidates:
            if c in row and normalize_str(row[c]):
                return normalize_str(row[c])
        # case-insensitive match
        lower_map = {k.lower(): k for k in row.keys()}
        for c in candidates:
            k = lower_map.get(c.lower())
            if k and normalize_str(row.get(k)):
                return normalize_str(row[k])
        return ""

    for r in rows:
        if appended >= max_new:
            break

        license_no = col(r, ["License Number", "License #", "License"])
        if not license_no:
            continue

        award_id = license_no  # Award ID = CSLB license number (stable, dedupe-friendly)
        if award_id in existing_ids:
            continue

        business = col(r, ["Business Name", "Business"])
        address = col(r, ["Address", "Street Address"])
        city = col(r, ["City"])
        state = col(r, ["State"])
        zip_code = col(r, ["Zip", "Zip Code", "ZIP"])
        phone = col(r, ["Telephone Number", "Phone", "Telephone"])
        status = col(r, ["License Status", "Status"])
        classifications = col(r, ["Classification(s)", "Classifications", "Classification"])

        naics = infer_naics_from_classifications(classifications)
        if not naics or naics not in ALLOWED_NAICS:
            continue

        # Strict-ish: keep Active only (recommended for outreach)
        if "active" not in normalize_str(status).lower():
            continue

        # Optional website enrichment (DDG), capped
        website = ""
        if enable_ddg and ddg_used < ddg_cap and business:
            website = ddg_find_website(f"{business} contractor Los Angeles CA")
            ddg_used += 1
            time.sleep(0.35)

        score, rationale, conf_level = confidence_for_row(status=status, phone=phone)

        pop = safe_join([address, city, state, zip_code], sep=", ").replace(", ,", ",").strip(", ").strip()
        recipient_id = hashlib.md5(award_id.encode("utf-8")).hexdigest()

        # We do not have permit-based start/end dates in this model.
        # We anchor to a consistent 2026 target date to fit your downstream pipeline.
        start_date = "2026-07-01"

        # Links
        recipient_profile = f"https://www.cslb.ca.gov/OnlineServices/CheckLicenseII/CheckLicense.aspx"
        web_search = f"https://www.google.com/search?q={requests.utils.quote(business)}+CSLB+{award_id}"

        values = {
            "Award ID": award_id,
            "Recipient (Company)": business,
            "Recipient UEI": "",
            "Parent Recipient UEI": "",
            "Parent Recipient DUNS": "",
            "Recipient (HQ) Address": pop,  # best available from CSLB export
            "Start Date": start_date,
            "End Date": "",
            "Last Modified Date": now,
            "Award Amount (Obligated)": "",  # CSLB list does not include project value
            "NAICS Code": naics,
            "NAICS Description": NAICS_DESC.get(naics, ""),
            "Awarding Agency": "CSLB Public Data Portal (List by Classification & County)",
            "Place of Performance": "Los Angeles County, CA",
            "Description": f"CSLB licensed contractor in Los Angeles County. Classifications: {classifications}",
            "Award Link": "",  # not applicable (no award record)
            "Recipient Profile Link": recipient_profile,
            "Web Search Link": web_search,

            "Company Website": website,
            "Company Phone": phone,
            "Company General Email": "",  # CSLB explicitly does not provide email
            "Responsible Person Name": "",
            "Responsible Person Role": "",
            "Responsible Person Email": "",
            "Responsible Person Phone": "",

            "confidence_score": str(score),
            "prediction_rationale": rationale,
            "target_flag": "TRUE",
            "recipient_id": recipient_id,
            "data_source": "CSLB Public Data Portal",
            "data_confidence_level": conf_level,
            "last_verified_date": now,
            "notes": f"License {award_id}; Status={status}; County={TARGET_COUNTY}",
        }

        ordered_row = [""] * len(hmap)
        for header, col_index in hmap.items():
            ordered_row[col_index - 1] = values.get(header, "")

        rows_to_append.append(ordered_row)
        existing_ids.add(award_id)
        appended += 1

        time.sleep(sleep_seconds)

    if rows_to_append:
        ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        print(f"✅ Appended {len(rows_to_append)} rows into {tab_name}.")
    else:
        print("No rows appended. Possible causes:")
        print("- CSLB portal output schema changed (field names differ)")
        print("- LA county list returned no Active rows for selected classifications (unlikely)")
        print("- Your sheet headers don’t match expected names (Award ID / Recipient (Company) etc.)")

    print(f"DDG used: {ddg_used} (cap={ddg_cap})")
    print("Done.")


if __name__ == "__main__":
    main()
