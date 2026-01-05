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

# NOTE: Use the non-.aspx URL. CSLB frequently redirects and the .aspx path is less stable.
CSLB_LIST_BY_COUNTY_URL = "https://www.cslb.ca.gov/onlineservices/dataportal/ListByCounty"

ALLOWED_NAICS = {"238210", "236220", "237310", "238220", "238120"}

TARGET_COUNTY_TEXT = "Los Angeles"

# <=10 classifications (your constraint)
TARGET_CLASSIFICATIONS_TEXT = ["A", "B", "C-10", "C-20", "C-36", "C-4", "C-32", "C-51", "C-50"]

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
# General helpers
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


# =============================
# DuckDuckGo website enrichment
# =============================

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
# CSLB ASP.NET form mechanics
# =============================

def _extract_hidden_inputs(soup: BeautifulSoup) -> Dict[str, str]:
    """
    Captures __VIEWSTATE, __EVENTVALIDATION, etc.
    """
    fields = {}
    for inp in soup.select("input"):
        name = inp.get("name")
        if not name:
            continue
        t = normalize_str(inp.get("type")).lower()
        if t in ("hidden", "submit", "text", "search", ""):
            fields[name] = inp.get("value", "")
    return fields


def _find_select_by_labelish(soup: BeautifulSoup, keywords: List[str]) -> Optional[BeautifulSoup]:
    """
    Tries to find a <select> whose id/name contains any of the keywords.
    """
    selects = soup.find_all("select")
    for sel in selects:
        name = normalize_str(sel.get("name") or "")
        sid = normalize_str(sel.get("id") or "")
        blob = f"{name} {sid}".lower()
        if any(k.lower() in blob for k in keywords):
            return sel
    return None


def _option_value_for_text(select_tag: BeautifulSoup, wanted_text: str) -> Optional[str]:
    """
    Returns the option 'value' for an option whose visible text matches wanted_text (case-insensitive).
    """
    wt = wanted_text.strip().lower()
    for opt in select_tag.find_all("option"):
        txt = normalize_str(opt.get_text(" ", strip=True)).lower()
        if txt == wt:
            return opt.get("value")
    # fallback: contains
    for opt in select_tag.find_all("option"):
        txt = normalize_str(opt.get_text(" ", strip=True)).lower()
        if wt in txt:
            return opt.get("value")
    return None


def _extract_postback_targets(html: str) -> List[Tuple[str, str, str]]:
    """
    Finds __doPostBack('TARGET','ARG') occurrences and returns (target,arg,context_snippet).
    """
    out = []
    for m in re.finditer(r"__doPostBack\('([^']+)','([^']*)'\)", html):
        target, arg = m.group(1), m.group(2)
        start = max(0, m.start() - 200)
        end = min(len(html), m.end() + 200)
        ctx = html[start:end]
        out.append((target, arg, ctx))
    return out


def _pick_export_postback(html: str) -> Optional[Tuple[str, str]]:
    """
    Picks the most likely Excel/Export postback target based on nearby keywords.
    """
    cands = _extract_postback_targets(html)
    best = None
    best_score = 0
    for target, arg, ctx in cands:
        ctx_l = ctx.lower()
        score = 0
        if "excel" in ctx_l or ".xls" in ctx_l or "xls" in ctx_l:
            score += 10
        if "export" in ctx_l or "download" in ctx_l:
            score += 6
        if "button" in ctx_l or "linkbutton" in ctx_l:
            score += 2
        if score > best_score:
            best_score = score
            best = (target, arg)
    return best


def _pick_search_submit_name(soup: BeautifulSoup) -> Optional[str]:
    """
    Sometimes you must "run" the query before export becomes available.
    We try to find a submit button that looks like Search/View/Submit.
    """
    for inp in soup.find_all("input"):
        t = normalize_str(inp.get("type")).lower()
        if t != "submit":
            continue
        val = normalize_str(inp.get("value")).lower()
        if any(k in val for k in ["search", "view", "submit", "run", "go"]):
            return inp.get("name")
    return None


def _download_with_two_step_post(session: requests.Session, timeout: int = 60) -> bytes:
    """
    1) GET page, parse hidden inputs + select values
    2) POST with selected classifications/counties (+search if present) to materialize results
    3) From returned HTML, identify Excel export postback target
    4) POST again with __EVENTTARGET set to export target, expecting a file response
    """
    r0 = session.get(CSLB_LIST_BY_COUNTY_URL, timeout=timeout)
    r0.raise_for_status()

    html0 = r0.text
    soup0 = BeautifulSoup(html0, "lxml")

    # Find county + classification selects
    class_sel = _find_select_by_labelish(soup0, ["class"])
    county_sel = _find_select_by_labelish(soup0, ["county"])
    if not class_sel or not county_sel:
        snippet = re.sub(r"\s+", " ", html0[:500])
        raise RuntimeError(f"Could not locate classification/county selects on CSLB page. Snippet: {snippet}")

    class_name = class_sel.get("name")
    county_name = county_sel.get("name")
    if not class_name or not county_name:
        raise RuntimeError("Classification/county select tags missing 'name' attribute (page structure changed).")

    # Convert visible text -> option values
    county_val = _option_value_for_text(county_sel, TARGET_COUNTY_TEXT)
    if not county_val:
        raise RuntimeError(f"Could not find county option '{TARGET_COUNTY_TEXT}' on CSLB page.")

    class_vals = []
    for ctext in TARGET_CLASSIFICATIONS_TEXT:
        v = _option_value_for_text(class_sel, ctext)
        if not v:
            raise RuntimeError(f"Could not find classification option '{ctext}' on CSLB page.")
        class_vals.append(v)

    # Hidden fields
    base_fields = _extract_hidden_inputs(soup0)

    # Step 1 POST: set selections and try to "search" / materialize
    post_items = list(base_fields.items())

    # Multi-select: repeat key for each selected option (typical ASP.NET)
    for v in class_vals:
        post_items.append((class_name, v))
    post_items.append((county_name, county_val))

    search_btn = _pick_search_submit_name(soup0)
    if search_btn:
        post_items.append((search_btn, "Search"))
        # no eventtarget needed in this case
        post_items.append(("__EVENTTARGET", ""))
        post_items.append(("__EVENTARGUMENT", ""))
    else:
        # fallback: no visible search button. Leave EVENTTARGET blank.
        post_items.append(("__EVENTTARGET", ""))
        post_items.append(("__EVENTARGUMENT", ""))

    r1 = session.post(CSLB_LIST_BY_COUNTY_URL, data=post_items, timeout=timeout)
    r1.raise_for_status()

    html1 = r1.text

    # Step 2: find export postback target (Excel)
    export_pb = _pick_export_postback(html1)
    if not export_pb:
        snippet = re.sub(r"\s+", " ", html1[:700])
        raise RuntimeError(
            "Could not locate Excel export postback target on CSLB page after applying filters. "
            f"Page snippet: {snippet}"
        )

    export_target, export_arg = export_pb

    soup1 = BeautifulSoup(html1, "lxml")
    fields1 = _extract_hidden_inputs(soup1)

    post_items2 = list(fields1.items())

    # Keep selections again (ASP.NET often expects them on export postback)
    for v in class_vals:
        post_items2.append((class_name, v))
    post_items2.append((county_name, county_val))

    post_items2.append(("__EVENTTARGET", export_target))
    post_items2.append(("__EVENTARGUMENT", export_arg or ""))

    r2 = session.post(CSLB_LIST_BY_COUNTY_URL, data=post_items2, timeout=timeout)
    r2.raise_for_status()

    content = r2.content

    # If CSLB returned HTML instead of a file, capture a helpful snippet
    head = content[:2000].lower()
    if b"<html" in head or b"<!doctype" in head:
        text = content.decode("utf-8", errors="ignore")
        snippet = re.sub(r"\s+", " ", text[:700])
        raise RuntimeError(
            "CSLB export did not return an Excel/CSV file (got HTML page instead). "
            f"Snippet: {snippet}"
        )

    return content


# =============================
# Parse CSLB export bytes
# =============================

def _looks_like_xlsx(b: bytes) -> bool:
    return len(b) >= 2 and b[0:2] == b"PK"


def _looks_like_xls(b: bytes) -> bool:
    return len(b) >= 8 and b[0:8] == bytes([0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1])


def _looks_like_csv(b: bytes) -> bool:
    # rough heuristic
    sample = b[:2000]
    return (b"," in sample or b";" in sample) and (b"\n" in sample or b"\r" in sample)


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


def parse_csv_bytes(x: bytes) -> List[Dict[str, str]]:
    text = x.decode("utf-8", errors="ignore")
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        return []
    # split by commas, but tolerate quoted commas
    import csv
    reader = csv.DictReader(lines)
    out = []
    for row in reader:
        out.append({k: normalize_str(v) for k, v in row.items()})
    return out


def parse_cslb_export(data: bytes) -> List[Dict[str, str]]:
    if _looks_like_xlsx(data):
        return parse_excel_xlsx(data)
    if _looks_like_xls(data):
        return parse_excel_xls(data)
    if _looks_like_csv(data):
        return parse_csv_bytes(data)

    sniff = data[:40]
    raise RuntimeError(f"Unknown CSLB export format. First 40 bytes: {sniff!r}")


# =============================
# Mapping & filtering
# =============================

def _normalize_class_token(tok: str) -> str:
    p = normalize_str(tok).upper()
    if not p:
        return ""
    m = re.match(r"^(C)\s*-?\s*(\d+)$", p)
    if m:
        return f"C-{m.group(2)}"
    return p


def infer_naics_from_classifications(classifications_str: str) -> str:
    raw = normalize_str(classifications_str)
    if not raw:
        return ""

    tokens = []
    for part in re.split(r"[;,/|]\s*|\s{2,}", raw):
        p = _normalize_class_token(part)
        if p:
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

    max_new = int(os.environ.get("LA_MAX_NEW", "10"))  # you requested safety limit; default 10
    sleep_seconds = float(os.environ.get("LA_SLEEP_SECONDS", "1.0"))

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

    print(f"Downloading CSLB export: county={TARGET_COUNTY_TEXT} classifications={TARGET_CLASSIFICATIONS_TEXT} ...")
    payload_bytes = _download_with_two_step_post(session=sess)
    print(f"Downloaded {len(payload_bytes):,} bytes from CSLB export.")

    rows = parse_cslb_export(payload_bytes)
    print(f"Parsed {len(rows)} rows from CSLB export.")

    now = utc_now_str()
    rows_to_append = []
    ddg_used = 0
    appended = 0

    # Helper: get column case-insensitively
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

        license_no = col(r, ["License Number", "License #", "License"])
        if not license_no:
            continue

        award_id = license_no
        if award_id in existing_ids:
            continue

        business = col(r, ["Business Name", "Business"])
        address = col(r, ["Address", "Street Address"])
        phone = col(r, ["Telephone Number", "Phone", "Telephone"])
        status = col(r, ["License Status", "Status"])
        classifications = col(r, ["Classification(s)", "Classifications", "Classification"])

        # Extra address parts sometimes exist depending on format
        city = col(r, ["City"])
        state = col(r, ["State"])
        zip_code = col(r, ["Zip", "Zip Code", "ZIP"])

        naics = infer_naics_from_classifications(classifications)
        if not naics or naics not in ALLOWED_NAICS:
            continue

        if "active" not in normalize_str(status).lower():
            continue

        website = ""
        if enable_ddg and ddg_used < ddg_cap and business:
            website = ddg_find_website(f"{business} contractor Los Angeles County CA")
            ddg_used += 1
            time.sleep(0.4)

        score, rationale, conf_level = confidence_for_row(status=status, phone=phone)

        # Some exports put full address in one field; if city/state/zip are blank, keep address as-is.
        if city or state or zip_code:
            hq_addr = safe_join([address, city, state, zip_code], sep=", ").replace(", ,", ",").strip(", ").strip()
        else:
            hq_addr = address

        recipient_id = hashlib.md5(award_id.encode("utf-8")).hexdigest()

        # Start anchor to keep consistency in downstream workflows
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
            "Award Link": "",  # CSLB export isn’t per-license deep link; profile link covers validation.
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
            "notes": f"License {award_id}; Status={status}; County={TARGET_COUNTY_TEXT}",
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
        print("No rows appended. If this happens, the CSLB export headers may have changed or the export returned no matching rows.")

    print(f"DDG used: {ddg_used} (cap={ddg_cap})")
    print("Done.")


if __name__ == "__main__":
    main()
