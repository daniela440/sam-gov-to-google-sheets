import os
import json
import time
import hashlib
import re
import io
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

import requests
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

import openpyxl  # .xlsx
import xlrd      # .xls (BIFF)


# =============================
# CONFIG
# =============================

CSLB_LIST_BY_COUNTY_URL = "https://www.cslb.ca.gov/onlineservices/dataportal/ListByCounty.aspx"

# Your NAICS allowlist
ALLOWED_NAICS = {"238210", "236220", "237310", "238220", "238120"}

# LA County only
TARGET_COUNTY = "Los Angeles"

# <= 10 classifications (mapped to your NAICS set)
TARGET_CLASSIFICATIONS = ["A", "B", "C-10", "C-20", "C-36", "C-4", "C-32", "C-51", "C-50"]

CLASS_TO_NAICS = {
    "C-10": "238210",  # Electrical
    "C-20": "238220",  # HVAC
    "C-36": "238220",  # Plumbing
    "C-4":  "238220",  # Boiler/Hot Water Heating
    "C-51": "238120",  # Structural Steel
    "C-50": "238120",  # Steel/Rebar
    "A":    "237310",  # Engineering -> roads/bridge proxy
    "C-32": "237310",  # Parking/Landscaping -> civil proxy (kept per your earlier mapping)
    "B":    "236220",  # General Building
}

NAICS_DESC = {
    "238210": "Electrical Contractors and Other Wiring Installation Contractors",
    "236220": "Commercial and Institutional Building Construction",
    "237310": "Highway, Street, and Bridge Construction",
    "238220": "Plumbing, Heating, and Air-Conditioning Contractors",
    "238120": "Structural Steel and Precast Concrete Contractors",
}

DDG_BLOCKLIST = (
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

    def clean(u: str) -> str:
        return u.replace("&amp;", "&").strip()

    for u in links:
        u = clean(u)
        if any(b in u.lower() for b in DDG_BLOCKLIST):
            continue
        m = re.match(r"^(https?://[^/]+)", u)
        return m.group(1) if m else u

    return ""


def is_maintenance_or_no_data_page(html: str) -> bool:
    t = (html or "").lower()
    signals = [
        "database is unavailable",
        "scheduled maintenance",
        "please note:",
        "unavailable sundays",
        "temporarily unavailable",
        "service is currently unavailable",
    ]
    return any(s in t for s in signals)

from urllib.parse import urljoin

def _find_direct_download_link(html: str) -> Optional[str]:
    """
    Returns an absolute URL if page contains a direct download link (.xls/.xlsx or export/download endpoint).
    """
    soup = BeautifulSoup(html or "", "lxml")
    for a in soup.find_all("a"):
        href = normalize_str(a.get("href"))
        txt = normalize_str(a.get_text(" ", strip=True))
        blob = f"{href} {txt}".lower()

        if not href:
            continue

        # direct file link OR export/download-like endpoint
        if any(k in blob for k in [".xls", ".xlsx", "download", "export", "excel"]):
            # ignore javascript links
            if href.lower().startswith("javascript:"):
                continue
            return urljoin(CSLB_LIST_BY_COUNTY_URL, href)

    return None


def _is_excel_response(resp: requests.Response) -> bool:
    ct = (resp.headers.get("Content-Type") or "").lower()
    cd = (resp.headers.get("Content-Disposition") or "").lower()
    if "excel" in ct or ".xls" in cd or "attachment" in cd:
        return True
    return _looks_like_xls(resp.content) or _looks_like_xlsx(resp.content)


# =============================
# CSLB portal interaction (ASP.NET)
# =============================

def _extract_aspnet_form_fields(soup: BeautifulSoup) -> Dict[str, str]:
    fields = {}
    for inp in soup.select("input"):
        name = inp.get("name")
        if not name:
            continue
        fields[name] = inp.get("value", "")
    return fields


def _find_select_name_by_contains(soup: BeautifulSoup, must_contain: str) -> Optional[str]:
    for sel in soup.find_all("select"):
        name = normalize_str(sel.get("name"))
        if must_contain.lower() in name.lower():
            return name
    return None


def _guess_class_and_county_select_names(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    class_name = _find_select_name_by_contains(soup, "class")
    county_name = _find_select_name_by_contains(soup, "county")
    if class_name and county_name:
        return class_name, county_name

    # fallback: first two selects
    selects = soup.find_all("select")
    if len(selects) >= 2:
        return selects[0].get("name"), selects[1].get("name")
    return None, None


def _find_submit_button_name(soup: BeautifulSoup) -> Optional[Tuple[str, str]]:
    """
    Returns (name, value) for the action button that triggers download/results.

    CSLB uses an ASP.NET WebForms button that may render as input[type=button] with value 'Download'.
    """
    # 1) Look for the explicit Download button first
    for inp in soup.find_all("input"):
        t = normalize_str(inp.get("type")).lower()
        name = normalize_str(inp.get("name"))
        value = normalize_str(inp.get("value"))
        if not name:
            continue
        if t in ("submit", "button", "image") and value.lower() in ("download", "search"):
            return name, (value or "Download")

    # 2) Heuristic fallback: any input that looks like an action
    for inp in soup.find_all("input"):
        t = normalize_str(inp.get("type")).lower()
        if t not in ("submit", "button", "image"):
            continue
        name = normalize_str(inp.get("name"))
        value = normalize_str(inp.get("value"))
        blob = f"{name} {value}".lower()
        if any(k in blob for k in ["download", "search", "view", "results", "submit", "filter", "run", "show", "go"]):
            if name:
                return name, (value or "Download")

    # 3) <button> elements (rare on this page, but keep)
    for btn in soup.find_all("button"):
        name = normalize_str(btn.get("name"))
        value = normalize_str(btn.get("value")) or normalize_str(btn.get_text(" ", strip=True))
        blob = f"{name} {value}".lower()
        if any(k in blob for k in ["download", "search", "view", "results", "submit", "filter", "run", "show", "go"]):
            if name:
                return name, (value or "Download")

    return None



def _find_export_postback_target(html: str) -> Optional[Tuple[str, str]]:
    """
    Find an ASP.NET postback target/arg for export/download.
    Handles single quotes, double quotes, and HTML entity quotes.
    Returns (TARGET, ARG) if found.
    """
    if not html:
        return None

    candidates = []

    patterns = [
        r"__doPostBack\('([^']+)'\s*,\s*'([^']*)'\)",   # single quotes
        r'__doPostBack\("([^"]+)"\s*,\s*"([^"]*)"\)',  # double quotes
        r"__doPostBack\(&#39;([^&]+)&#39;\s*,\s*&#39;([^&]*)&?#39;\)",  # entity quotes (best-effort)
    ]

    for pat in patterns:
        for target, arg in re.findall(pat, html, flags=re.IGNORECASE):
            candidates.append((target, arg))

    if not candidates:
        return None

    # Choose best candidate by heuristics:
    # prefer those that look like download/export/xls/excel
    for target, arg in candidates:
        blob = f"{target} {arg}".lower()
        if any(k in blob for k in ["export", "download", "excel", "xls", "xlsx"]):
            return target, arg

    # fallback: if nothing matches keywords, just return the first
    return candidates[0]


    return None
def debug_dump_html(label: str, html: str) -> None:
    """
    Copy/paste over your existing debug_dump_html().

    Prints:
      - form controls (submit/button/image)
      - anchor candidates (href/text includes download/export/xls)
      - doPostBack calls in single-quote + double-quote formats
    """
    try:
        soup = BeautifulSoup(html or "", "lxml")
        title = soup.title.get_text(" ", strip=True) if soup.title else ""

        tables = soup.find_all("table")
        selects = soup.find_all("select")

        max_tr = 0
        if tables:
            max_tr = max(len(t.find_all("tr")) for t in tables)

        print("---- CSLB DEBUG ----")
        print(f"[{label}] title={title!r}")
        print(f"[{label}] maintenance_detected={is_maintenance_or_no_data_page(html)}")
        print(f"[{label}] html_len={len(html or ''):,}")
        print(f"[{label}] tables_count={len(tables)} max_table_tr={max_tr}")
        print(f"[{label}] selects_count={len(selects)}")

        if selects:
            names = []
            for s in selects[:12]:
                nm = normalize_str(s.get("name") or s.get("id") or "")
                if nm:
                    names.append(nm)
            print(f"[{label}] select_names_sample={names}")

        # Inputs that could trigger download
        inputs = []
        for inp in soup.find_all("input"):
            t = normalize_str(inp.get("type")).lower()
            if t in ("submit", "button", "image"):
                inputs.append(
                    (
                        t,
                        normalize_str(inp.get("id")),
                        normalize_str(inp.get("name")),
                        normalize_str(inp.get("value")),
                    )
                )
        print(f"[{label}] action_inputs_found={len(inputs)} sample={inputs[:15]}")

        # Anchors that look like download/export links
        anchors = []
        for a in soup.find_all("a"):
            href = normalize_str(a.get("href"))
            txt = normalize_str(a.get_text(" ", strip=True))
            blob = f"{href} {txt}".lower()
            if any(k in blob for k in ["download", "export", "excel", ".xls", ".xlsx", "listbycounty"]):
                anchors.append((normalize_str(a.get("id")), normalize_str(a.get("name")), href[:200], txt[:120]))
        print(f"[{label}] anchor_candidates_found={len(anchors)} sample={anchors[:15]}")

        # Postback calls (single-quote AND double-quote)
        calls = []
        calls += re.findall(r"__doPostBack\('([^']+)'\s*,\s*'([^']*)'\)", html or "", flags=re.IGNORECASE)
        calls += re.findall(r'__doPostBack\("([^"]+)"\s*,\s*"([^"]*)"\)', html or "", flags=re.IGNORECASE)

        # De-dupe
        seen = set()
        calls_u = []
        for t, a in calls:
            key = (t, a)
            if key not in seen:
                seen.add(key)
                calls_u.append(key)
        print(f"[{label}] doPostBack_calls_found={len(calls_u)} sample={calls_u[:15]}")

        print("---- END CSLB DEBUG ----")
    except Exception as e:
        print(f"[{label}] debug_dump_html failed: {e}")



def download_or_results_html(session: requests.Session, timeout: int = 60) -> Tuple[Optional[bytes], Optional[bytes]]:
    """
    Returns (download_bytes, results_html_bytes)

    - If we can trigger an export/download, download_bytes is set.
    - Otherwise, if a results table exists on the returned page, results_html_bytes is set.
    - If maintenance/no-data page, both will be None (caller should exit cleanly).
    """

    # Step 0: GET page
    r0 = session.get(CSLB_LIST_BY_COUNTY_URL, timeout=timeout)
    r0.raise_for_status()
    debug_dump_html("GET page", r0.text)

    if is_maintenance_or_no_data_page(r0.text):
        return None, None

    soup0 = BeautifulSoup(r0.text, "lxml")
    form_fields = _extract_aspnet_form_fields(soup0)

    class_select_name, county_select_name = _guess_class_and_county_select_names(soup0)
    if not class_select_name or not county_select_name:
        return None, None

    submit = _find_submit_button_name(soup0)  # (name, value) or None

    # Step 1: Apply filters (so the results table/export becomes available)
    post_items = list(form_fields.items())

    # multi-select listbox: repeated key is correct
    for c in TARGET_CLASSIFICATIONS:
        post_items.append((class_select_name, c))
    post_items.append((county_select_name, TARGET_COUNTY))

    if submit:
        submit_name, submit_value = submit
        post_items.append((submit_name, submit_value))
    else:
        # Generic ASP.NET postback fallback
        post_items.append(("__EVENTTARGET", ""))
        post_items.append(("__EVENTARGUMENT", ""))

    r1 = session.post(
        CSLB_LIST_BY_COUNTY_URL,
        data=post_items,
        timeout=timeout,
        headers={"Referer": CSLB_LIST_BY_COUNTY_URL},
    )
    r1.raise_for_status()
    debug_dump_html("POST filters", r1.text)

    if is_maintenance_or_no_data_page(r1.text):
        return None, None
    
    # Step 1.5: Some CSLB pages provide a direct download link (no doPostBack in HTML).
    direct = _find_direct_download_link(r1.text)
    if direct:
        r_dl = session.get(direct, timeout=timeout, headers={"Referer": CSLB_LIST_BY_COUNTY_URL})
        if r_dl.status_code == 200 and _is_excel_response(r_dl):
            return r_dl.content, None
        # If it returned HTML, keep going (maybe needs a postback)


    # Step 2: Try to trigger Excel export via postback (if present)
    pb = _find_export_postback_target(r1.text)
    if pb:
        target, arg = pb
        soup1 = BeautifulSoup(r1.text, "lxml")
        form_fields_2 = _extract_aspnet_form_fields(soup1)

        export_items = list(form_fields_2.items())

        # Keep selections consistent (sometimes required)
        for c in TARGET_CLASSIFICATIONS:
            export_items.append((class_select_name, c))
        export_items.append((county_select_name, TARGET_COUNTY))

        export_items.append(("__EVENTTARGET", target))
        export_items.append(("__EVENTARGUMENT", arg or ""))

        r2 = session.post(
            CSLB_LIST_BY_COUNTY_URL,
            data=export_items,
            timeout=timeout,
            headers={"Referer": CSLB_LIST_BY_COUNTY_URL},
        )
        r2.raise_for_status()

        ct = (r2.headers.get("Content-Type") or "").lower()
        if (
            ("excel" in ct)
            or ("spreadsheet" in ct)
            or ("application/octet-stream" in ct)
            or _looks_like_xls(r2.content)
            or _looks_like_xlsx(r2.content)
        ):
            return r2.content, None

        if _looks_like_html(r2.content):
            return None, r2.content

    # No export found; attempt to parse results table from r1
    return None, r1.content

# =============================
# Parsers
# =============================

def _looks_like_xlsx(b: bytes) -> bool:
    return len(b) >= 2 and b[0:2] == b"PK"  # zip


def _looks_like_xls(b: bytes) -> bool:
    return len(b) >= 8 and b[0:8] == bytes([0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1])


def _looks_like_html(b: bytes) -> bool:
    t = b[:4000].lower()
    return b"<html" in t or b"<!doctype html" in t or b"<table" in t


def parse_excel_xlsx(x: bytes) -> List[Dict[str, str]]:
    wb = openpyxl.load_workbook(io.BytesIO(x), read_only=True, data_only=True)
    ws = wb.worksheets[0]
    rows_iter = ws.iter_rows(values_only=True)

    headers = []
    out = []
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
    out = []
    for r in range(1, sheet.nrows):
        rowvals = [normalize_str(sheet.cell_value(r, c)) for c in range(sheet.ncols)]
        if not any(rowvals):
            continue
        if len(rowvals) < len(headers):
            rowvals += [""] * (len(headers) - len(rowvals))
        out.append({headers[i]: rowvals[i] for i in range(min(len(headers), len(rowvals)))})
    return out


def parse_html_table(x: bytes) -> List[Dict[str, str]]:
    text = x.decode("utf-8", errors="ignore")
    soup = BeautifulSoup(text, "lxml")

    # Prefer the largest table (some pages have layout tables)
    tables = soup.find_all("table")
    if not tables:
        return []

    def table_size(tb):
        return len(tb.find_all("tr"))

    table = sorted(tables, key=table_size, reverse=True)[0]
    rows = table.find_all("tr")
    if not rows:
        return []

    headers = [normalize_str(cell.get_text(" ", strip=True)) for cell in rows[0].find_all(["th", "td"])]
    if not headers or len(headers) < 2:
        return []

    out = []
    for tr in rows[1:]:
        cells = [normalize_str(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
        if not cells or all(not c for c in cells):
            continue
        if len(cells) < len(headers):
            cells += [""] * (len(headers) - len(cells))
        out.append({headers[i]: cells[i] for i in range(min(len(headers), len(cells)))})
    return out


def parse_cslb_payload(download_bytes: Optional[bytes], results_html_bytes: Optional[bytes]) -> List[Dict[str, str]]:
    if download_bytes:
        if _looks_like_xlsx(download_bytes):
            return parse_excel_xlsx(download_bytes)
        if _looks_like_xls(download_bytes):
            return parse_excel_xls(download_bytes)
        if _looks_like_html(download_bytes):
            return parse_html_table(download_bytes)

    if results_html_bytes:
        return parse_html_table(results_html_bytes)

    return []


# =============================
# Mapping & filtering
# =============================

def infer_naics_from_classifications(classifications_str: str) -> str:
    raw = normalize_str(classifications_str)
    if not raw:
        return ""

    tokens = []
    for part in re.split(r"[;,/|]\s*|\s{2,}", raw):
        p = normalize_str(part).upper()
        if not p:
            continue
        m = re.match(r"^(C)\s*-?\s*(\d+)$", p)
        if m:
            p = f"C-{m.group(2)}"
        tokens.append(p)

    priority = ["C-10", "C-36", "C-20", "C-4", "C-51", "C-50", "C-32", "A", "B"]
    token_set = set(tokens)

    for c in priority:
        if c in token_set and c in CLASS_TO_NAICS:
            return CLASS_TO_NAICS[c]

    for t in tokens:
        if t in CLASS_TO_NAICS:
            return CLASS_TO_NAICS[t]

    return ""


def confidence_for_row(status: str, phone: str) -> Tuple[int, str, str]:
    s = normalize_str(status).lower()
    p = normalize_str(phone)

    score = 70
    rationale = ["cslb_county_list(+70)"]

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

    max_new = int(os.environ.get("LA_MAX_NEW", "200"))
    sleep_seconds = float(os.environ.get("LA_SLEEP_SECONDS", "0.2"))

    enable_ddg = os.environ.get("LA_ENABLE_DDG", "true").lower() == "true"
    ddg_cap = int(os.environ.get("LA_DDG_DAILY_CAP", "10"))

    # Connect sheet
    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab_name)
    hmap = header_map(ws)
    existing_ids = load_existing_award_ids(ws)

    sess = requests.Session()
    sess.headers.update({"User-Agent": "Mozilla/5.0"})

    print(f"Downloading CSLB list (LA County only) for classifications={TARGET_CLASSIFICATIONS} ...")
    download_bytes, results_html_bytes = download_or_results_html(session=sess)

    # If maintenance/no-data page, exit cleanly (do not fail GitHub Action)
    if download_bytes is None and results_html_bytes is None:
        print("CSLB appears to be in maintenance / unavailable mode, or page structure is degraded.")
        print("Exiting cleanly without appending rows. Re-run after maintenance window.")
        return

    rows = parse_cslb_payload(download_bytes, results_html_bytes)
    print(f"Parsed {len(rows)} rows from CSLB payload.")

    if not rows:
        print("No parsable rows found (likely CSLB returned a page without results). Exiting cleanly.")
        return

    now = utc_now_str()
    rows_to_append = []
    ddg_used = 0
    appended = 0

    # Helper to get column case-insensitively
    def col(row: dict, candidates: List[str]) -> str:
        lower_map = {k.lower(): k for k in row.keys()}
        for c in candidates:
            k = lower_map.get(c.lower())
            if k and normalize_str(row.get(k)):
                return normalize_str(row[k])
        return ""

    for r in rows:
        if appended >= max_new:
            break

        license_no = col(r, ["License Number", "License #", "License", "Lic #", "Lic No", "License No"])
        if not license_no:
            continue

        award_id = license_no
        if award_id in existing_ids:
            continue

        business = col(r, ["Business Name", "Business", "Contractor Name", "Company", "Name"])
        address = col(r, ["Address", "Street Address", "Address Line 1"])
        city = col(r, ["City"])
        state = col(r, ["State"])
        zip_code = col(r, ["Zip", "Zip Code", "ZIP"])
        phone = col(r, ["Telephone Number", "Phone", "Telephone", "Tel"])
        status = col(r, ["License Status", "Status"])
        classifications = col(r, ["Classification(s)", "Classifications", "Classification", "Class"])

        naics = infer_naics_from_classifications(classifications)
        if not naics or naics not in ALLOWED_NAICS:
            continue

        # Keep only Active licenses
        if "active" not in normalize_str(status).lower():
            continue

        website = ""
        if enable_ddg and ddg_used < ddg_cap and business:
            website = ddg_find_website(f"{business} contractor Los Angeles County CA")
            ddg_used += 1
            time.sleep(0.40)

        score, rationale, conf_level = confidence_for_row(status=status, phone=phone)

        hq_addr = safe_join([address, city, state, zip_code], sep=", ").replace(", ,", ",").strip(", ").strip()
        recipient_id = hashlib.md5(award_id.encode("utf-8")).hexdigest()

        # Keep your downstream anchor
        start_date = "2026-07-01"

        recipient_profile = "https://www.cslb.ca.gov/OnlineServices/CheckLicenseII/CheckLicense.aspx"
        web_search = f"https://www.google.com/search?q={requests.utils.quote(business)}+CSLB+{award_id}"

        values = {
            "Award ID": award_id,
            "Recipient (Company)": business,
            "Recipient UEI": "",
            "Parent Recipient UEI": "",
            "Parent Recipient DUNS": "",
            "Recipient (HQ) Address": hq_addr,
            "Start Date": start_date,
            "End Date": "",
            "Last Modified Date": now,
            "Award Amount (Obligated)": "",
            "NAICS Code": naics,
            "NAICS Description": NAICS_DESC.get(naics, ""),
            "Awarding Agency": "CSLB Public Data Portal (List by Classification & County)",
            "Place of Performance": "Los Angeles County, CA",
            "Description": f"CSLB licensed contractor in Los Angeles County. Classifications: {classifications}",
            "Award Link": "",  # CSLB export does not provide a stable per-license deep link in this feed
            "Recipient Profile Link": recipient_profile,
            "Web Search Link": web_search,

            "Company Website": website,
            "Company Phone": phone,
            "Company General Email": "",
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
        print("No rows appended (after filters). This can happen if CSLB returned no Active rows for the classifications/county, or if columns differed.")

    print(f"DDG used: {ddg_used} (cap={ddg_cap})")
    print("Done.")


if __name__ == "__main__":
    main()
