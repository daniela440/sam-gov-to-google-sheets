import os
import json
import time
import hashlib
import re
import io
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional
from urllib.parse import urljoin

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

# <= 10 classifications
TARGET_CLASSIFICATIONS = ["A", "B", "C-10", "C-20", "C-36", "C-4", "C-32", "C-51", "C-50"]

CLASS_TO_NAICS = {
    "C-10": "238210",  # Electrical
    "C-20": "238220",  # HVAC
    "C-36": "238220",  # Plumbing
    "C-4":  "238220",  # Boiler/Hot Water Heating
    "C-51": "238120",  # Structural Steel
    "C-50": "238120",  # Steel/Rebar
    "A":    "237310",  # Engineering -> roads/bridge proxy
    "C-32": "237310",  # civil proxy
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
        "unavailable sundays",
        "temporarily unavailable",
        "service is currently unavailable",
    ]
    return any(s in t for s in signals)


# =============================
# Parsers (binary sniff)
# =============================

def _looks_like_xlsx(b: bytes) -> bool:
    return len(b) >= 2 and b[0:2] == b"PK"  # zip


def _looks_like_xls(b: bytes) -> bool:
    return len(b) >= 8 and b[0:8] == bytes([0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1])


def _looks_like_html(b: bytes) -> bool:
    t = b[:4000].lower()
    return b"<html" in t or b"<!doctype html" in t or b"<table" in t


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

    selects = soup.find_all("select")
    if len(selects) >= 2:
        return selects[0].get("name"), selects[1].get("name")
    return None, None


def _find_submit_button_name(soup: BeautifulSoup) -> Optional[Tuple[str, str]]:
    """
    CSLB uses input[type=button] value='Download' (client JS triggers postback).
    We still capture its UniqueID (name).
    """
    for inp in soup.find_all("input"):
        t = normalize_str(inp.get("type")).lower()
        name = normalize_str(inp.get("name"))
        value = normalize_str(inp.get("value"))
        if not name:
            continue
        if t in ("submit", "button", "image") and value.lower() in ("download", "search"):
            return name, (value or "Download")
    return None


def _get_select_options(soup: BeautifulSoup, select_name: str) -> List[Tuple[str, str]]:
    sel = soup.find("select", attrs={"name": select_name})
    if not sel:
        return []
    out = []
    for opt in sel.find_all("option"):
        val = normalize_str(opt.get("value"))
        txt = normalize_str(opt.get_text(" ", strip=True))
        out.append((val, txt))
    return out


def _resolve_option_value(options: List[Tuple[str, str]], desired: str) -> Optional[str]:
    d = normalize_str(desired).lower()
    for val, txt in options:
        if normalize_str(txt).lower() == d:
            return val
    for val, txt in options:
        if normalize_str(val).lower() == d:
            return val
    for val, txt in options:
        if d in normalize_str(txt).lower():
            return val
    return None


def _parse_postback_from_onclick(onclick: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Supports:
      - __doPostBack('TARGET','ARG')
      - WebForm_DoPostBackWithOptions(new WebForm_PostBackOptions("TARGET","ARG",...))
    Returns (target, arg)
    """
    s = normalize_str(onclick)
    if not s:
        return None, None

    m = re.search(r"__doPostBack\('([^']+)'\s*,\s*'([^']*)'\)", s, flags=re.IGNORECASE)
    if m:
        return m.group(1), m.group(2)

    m = re.search(
        r'WebForm_PostBackOptions\("([^"]+)"\s*,\s*"([^"]*)"',
        s,
        flags=re.IGNORECASE,
    )
    if m:
        return m.group(1), m.group(2)

    return None, None


def _find_direct_download_link(html: str) -> Optional[str]:
    soup = BeautifulSoup(html or "", "lxml")
    for a in soup.find_all("a"):
        href = normalize_str(a.get("href"))
        txt = normalize_str(a.get_text(" ", strip=True))
        blob = f"{href} {txt}".lower()
        if not href:
            continue
        if any(k in blob for k in [".xls", ".xlsx", "download", "export", "excel"]):
            if href.lower().startswith("javascript:"):
                continue
            return urljoin(CSLB_LIST_BY_COUNTY_URL, href)
    return None


def debug_dump_html(label: str, html: str) -> None:
    try:
        soup = BeautifulSoup(html or "", "lxml")
        title = soup.title.get_text(" ", strip=True) if soup.title else ""

        tables = soup.find_all("table")
        selects = soup.find_all("select")
        max_tr = max((len(t.find_all("tr")) for t in tables), default=0)

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

        inputs = []
        for inp in soup.find_all("input"):
            t = normalize_str(inp.get("type")).lower()
            if t in ("submit", "button", "image"):
                inputs.append((t, normalize_str(inp.get("id")), normalize_str(inp.get("name")), normalize_str(inp.get("value"))))
        print(f"[{label}] action_inputs_found={len(inputs)} sample={inputs[:10]}")

        btn = soup.find("input", attrs={"id": "btnSearch"})
        if btn:
            print(f"[{label}] btnSearch_onclick={normalize_str(btn.get('onclick'))[:250]}")

        print("---- END CSLB DEBUG ----")
    except Exception as e:
        print(f"[{label}] debug_dump_html failed: {e}")
def _print_selected_state(label: str, html: str, class_select_name: str, county_select_name: str) -> None:
    try:
        soup = BeautifulSoup(html or "", "lxml")

        def selected_values(select_name: str) -> List[str]:
            sel = soup.find("select", attrs={"name": select_name})
            if not sel:
                return []
            chosen = []
            for opt in sel.find_all("option"):
                if opt.has_attr("selected"):
                    chosen.append(normalize_str(opt.get("value")) or normalize_str(opt.get_text(" ", strip=True)))
            return chosen

        cls = selected_values(class_select_name)
        cty = selected_values(county_select_name)

        print(f"[{label}] server_selected_classifications_count={len(cls)} sample={cls[:10]}")
        print(f"[{label}] server_selected_counties_count={len(cty)} sample={cty[:10]}")
    except Exception as e:
        print(f"[{label}] _print_selected_state failed: {e}")


def _extract_validation_snippet(html: str, width: int = 250) -> str:
    """
    Grabs a small snippet around common validation words so you can see exactly what CSLB is complaining about.
    """
    t = html or ""
    low = t.lower()
    needles = ["please select", "required", "validation", "error"]
    for n in needles:
        idx = low.find(n)
        if idx != -1:
            start = max(0, idx - width)
            end = min(len(t), idx + width)
            return t[start:end].replace("\n", " ").replace("\r", " ")
    return ""


def download_or_results_html(session: requests.Session, timeout: int = 60) -> Tuple[Optional[bytes], Optional[bytes]]:
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
        print("Could not locate classification/county selects.")
        return None, None

    submit = _find_submit_button_name(soup0)
    if not submit:
        print("Could not locate Download button control.")
        return None, None
    submit_name, submit_value = submit

    # Resolve options (values + texts)
    class_opts = _get_select_options(soup0, class_select_name)   # (value, text)
    county_opts = _get_select_options(soup0, county_select_name)

    # Primary attempt: use actual option VALUES (what you already do)
    class_values: List[str] = []
    class_texts: List[str] = []
    for c in TARGET_CLASSIFICATIONS:
        v = _resolve_option_value(class_opts, c)
        if v:
            class_values.append(v)
        class_texts.append(c)  # the visible text you want as fallback

    county_value = _resolve_option_value(county_opts, TARGET_COUNTY)
    county_text = TARGET_COUNTY

    print("[RESOLVE] class_values_count=", len(class_values), "sample=", class_values[:5])
    print("[RESOLVE] county_value=", county_value)

    if not class_values or not county_value:
        print("Failed to resolve option values from the page.")
        return None, None

    # Pull postback target/arg from onclick
    btn = soup0.find("input", attrs={"name": submit_name})
    onclick = normalize_str(btn.get("onclick")) if btn else ""
    pb_target, pb_arg = _parse_postback_from_onclick(onclick)

    def do_post(use_text_fallback: bool) -> requests.Response:
        # IMPORTANT: remove any existing select keys and event keys first
        post_items = [
            (k, v) for (k, v) in form_fields.items()
            if k not in {class_select_name, county_select_name, "__EVENTTARGET", "__EVENTARGUMENT", "__LASTFOCUS"}
        ]

        # Add our selections
        if use_text_fallback:
            # Fallback attempt: post visible text (sometimes WebForms validators behave oddly with numeric values)
            for c in TARGET_CLASSIFICATIONS:
                post_items.append((class_select_name, c))
            post_items.append((county_select_name, county_text))
        else:
            for v in class_values:
                post_items.append((class_select_name, v))
            post_items.append((county_select_name, str(county_value)))

        # Add button field (some servers check it)
        post_items.append((submit_name, submit_value))

        # WebForms postback plumbing
        post_items.append(("__EVENTTARGET", pb_target or submit_name))
        post_items.append(("__EVENTARGUMENT", pb_arg or ""))
        post_items.append(("__LASTFOCUS", ""))

        r = session.post(
            CSLB_LIST_BY_COUNTY_URL,
            data=post_items,
            timeout=timeout,
            headers={"Referer": CSLB_LIST_BY_COUNTY_URL},
        )
        r.raise_for_status()
        return r

    # Attempt 1: value-based post
    r1 = do_post(use_text_fallback=False)

    print("[POST] status=", r1.status_code)
    print("[POST] content-type=", r1.headers.get("Content-Type"))
    print("[POST] content-disposition=", r1.headers.get("Content-Disposition"))
    print("[POST] first_bytes=", r1.content[:16])

    if _is_excel_response(r1):
        return r1.content, None

    debug_dump_html("POST page", r1.text)

    low = (r1.text or "").lower()
    has_validation = ("please select" in low) or ("required" in low) or ("validation" in low) or ("error" in low)
    print("[POST] has_validation_text=", has_validation)
    _print_selected_state("POST page", r1.text, class_select_name, county_select_name)

    if has_validation:
        snippet = _extract_validation_snippet(r1.text)
        if snippet:
            print("[POST] validation_snippet=", snippet[:400])

        # Attempt 2 (fallback): text-based post
        print("[POST] retrying with TEXT values for selections...")
        r1b = do_post(use_text_fallback=True)

        print("[POST retry] status=", r1b.status_code)
        print("[POST retry] content-type=", r1b.headers.get("Content-Type"))
        print("[POST retry] content-disposition=", r1b.headers.get("Content-Disposition"))
        print("[POST retry] first_bytes=", r1b.content[:16])

        if _is_excel_response(r1b):
            return r1b.content, None

        debug_dump_html("POST retry page", r1b.text)
        lowb = (r1b.text or "").lower()
        has_validation_b = ("please select" in lowb) or ("required" in lowb) or ("validation" in lowb) or ("error" in lowb)
        print("[POST retry] has_validation_text=", has_validation_b)
        _print_selected_state("POST retry page", r1b.text, class_select_name, county_select_name)

        # If retry still fails, return HTML so caller can exit cleanly (and logs will show why)
        return None, r1b.content

    if is_maintenance_or_no_data_page(r1.text):
        return None, None

    # Direct download link check (rare here, but keep)
    direct = _find_direct_download_link(r1.text)
    if direct:
        r_dl = session.get(direct, timeout=timeout, headers={"Referer": CSLB_LIST_BY_COUNTY_URL})
        if r_dl.status_code == 200 and _is_excel_response(r_dl):
            return r_dl.content, None
        if _looks_like_html(r_dl.content):
            return None, r_dl.content

    return None, r1.content


# =============================
# Excel/HTML table parsing
# =============================

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

    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab_name)
    hmap = header_map(ws)
    existing_ids = load_existing_award_ids(ws)

    sess = requests.Session()
    sess.headers.update({"User-Agent": "Mozilla/5.0"})

    print(f"Downloading CSLB list (LA County only) for classifications={TARGET_CLASSIFICATIONS} ...")
    download_bytes, results_html_bytes = download_or_results_html(session=sess)

    if download_bytes is None and results_html_bytes is None:
        print("CSLB appears to be in maintenance / unavailable mode, or page structure is degraded.")
        print("Exiting cleanly without appending rows. Re-run later.")
        return

    rows = parse_cslb_payload(download_bytes, results_html_bytes)
    print(f"Parsed {len(rows)} rows from CSLB payload.")

    if not rows:
        print("No parsable rows found. If [POST] has_validation_text=True, CSLB is rejecting selections.")
        return

    now = utc_now_str()
    rows_to_append = []
    ddg_used = 0
    appended = 0

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
            "Award Link": "",
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
        print("No rows appended (after filters).")

    print(f"DDG used: {ddg_used} (cap={ddg_cap})")
    print("Done.")


if __name__ == "__main__":
    main()
