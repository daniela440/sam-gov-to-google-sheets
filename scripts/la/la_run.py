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

import openpyxl  # for .xlsx
import xlrd      # for .xls (BIFF)


# =============================
# CONFIG
# =============================

CSLB_LIST_BY_COUNTY_URL = "https://www.cslb.ca.gov/onlineservices/dataportal/ListByCounty.aspx"

ALLOWED_NAICS = {"238210", "236220", "237310", "238220", "238120"}
TARGET_COUNTY = "Los Angeles"

# <=10 classifications
TARGET_CLASSIFICATIONS = ["A", "B", "C-10", "C-20", "C-36", "C-4", "C-32", "C-51", "C-50"]

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
# CSLB portal download (ASP.NET)
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
    # Prefer explicit names
    class_name = _find_select_name_by_contains(soup, "class")
    county_name = _find_select_name_by_contains(soup, "county")
    if class_name and county_name:
        return class_name, county_name

    # Fallback
    selects = soup.find_all("select")
    if len(selects) >= 2:
        return selects[0].get("name"), selects[1].get("name")

    return None, None


def _find_excel_postback_target(html: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Find __doPostBack('TARGET','ARG') used by the Excel export control.
    """
    if not html:
        return None, None

    matches = list(re.finditer(r"__doPostBack\('([^']+)','([^']*)'\)", html))
    if not matches:
        return None, None

    # Prefer targets containing "excel"
    for m in matches:
        tgt, arg = m.group(1), m.group(2)
        if "excel" in tgt.lower():
            return tgt, arg

    # Otherwise pick one that has excel/xls nearby
    for m in matches:
        start = max(0, m.start() - 250)
        end = min(len(html), m.end() + 250)
        context = html[start:end].lower()
        if "excel" in context or "xls" in context:
            return m.group(1), m.group(2)

    return None, None


def download_cslb_list_by_county(
    session: requests.Session,
    classifications: List[str],
    counties: List[str],
    timeout: int = 60
) -> bytes:
    # 1) GET (captures cookies + VIEWSTATE)
    r0 = session.get(CSLB_LIST_BY_COUNTY_URL, timeout=timeout)
    r0.raise_for_status()

    html0 = r0.text
    soup0 = BeautifulSoup(html0, "lxml")

    form_fields = _extract_aspnet_form_fields(soup0)
    class_select_name, county_select_name = _guess_class_and_county_select_names(soup0)

    if not class_select_name or not county_select_name:
        raise RuntimeError("Could not locate classification/county select fields on CSLB page (page structure changed).")

    # 2) Find Excel export postback target
    excel_target, excel_arg = _find_excel_postback_target(html0)
    if not excel_target:
        snippet = re.sub(r"\s+", " ", html0[:800])
        raise RuntimeError(
            "Could not find Excel export postback target on CSLB page. "
            f"Page snippet: {snippet}"
        )

    # 3) Build POST data (multi-select = repeated keys)
    post_items = list(form_fields.items())
    for c in classifications:
        post_items.append((class_select_name, c))
    for cty in counties:
        post_items.append((county_select_name, cty))

    # Trigger Excel export
    post_items.append(("__EVENTTARGET", excel_target))
    post_items.append(("__EVENTARGUMENT", excel_arg or ""))

    # 4) POST
    r1 = session.post(
        CSLB_LIST_BY_COUNTY_URL,
        data=post_items,
        timeout=timeout,
        headers={"Referer": CSLB_LIST_BY_COUNTY_URL, "User-Agent": "Mozilla/5.0"},
    )
    r1.raise_for_status()

    data = r1.content

    # If it returned HTML again, export did not fire (validation, wrong target, etc.)
    if _looks_like_html(data) and not _looks_like_xls(data) and not _looks_like_xlsx(data):
        text = data.decode("utf-8", errors="ignore")
        snippet = re.sub(r"\s+", " ", text[:1000])
        raise RuntimeError(
            "CSLB returned HTML instead of an Excel file after export postback. "
            f"Snippet: {snippet}"
        )

    return data


# =============================
# Parsers for CSLB download
# =============================

def _looks_like_xlsx(b: bytes) -> bool:
    return len(b) >= 2 and b[0:2] == b"PK"  # zip


def _looks_like_xls(b: bytes) -> bool:
    return len(b) >= 8 and b[0:8] == bytes([0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1])


def _looks_like_html(b: bytes) -> bool:
    t = b[:2000].lower()
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
    table = soup.find("table")
    if not table:
        snippet = re.sub(r"\s+", " ", text[:1000])
        raise RuntimeError(f"CSLB response was HTML but contained no table. Snippet: {snippet}")

    first_row = table.find("tr")
    if not first_row:
        return []

    headers = [normalize_str(cell.get_text(" ", strip=True)) for cell in first_row.find_all(["th", "td"])]
    out = []
    for tr in table.find_all("tr")[1:]:
        cells = [normalize_str(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
        if not cells or all(not c for c in cells):
            continue
        if len(cells) < len(headers):
            cells += [""] * (len(headers) - len(cells))
        out.append({headers[i]: cells[i] for i in range(min(len(headers), len(cells)))})
    return out


def parse_cslb_download(data: bytes) -> List[Dict[str, str]]:
    if _looks_like_xlsx(data):
        return parse_excel_xlsx(data)
    if _looks_like_xls(data):
        return parse_excel_xls(data)
    if _looks_like_html(data):
        return parse_html_table(data)
    sniff = data[:40]
    raise RuntimeError(f"Unknown CSLB download format. First 40 bytes: {sniff!r}")


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

    print(f"Downloading CSLB list for county={TARGET_COUNTY} classifications={TARGET_CLASSIFICATIONS} ...")
    payload_bytes = download_cslb_list_by_county(
        session=sess,
        classifications=TARGET_CLASSIFICATIONS,
        counties=[TARGET_COUNTY],
    )

    rows = parse_cslb_download(payload_bytes)
    print(f"Parsed {len(rows)} CSLB rows from download.")

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

        license_no = col(r, ["License Number", "License #", "License"])
        if not license_no:
            continue

        award_id = license_no
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

        if "active" not in normalize_str(status).lower():
            continue

        website = ""
        if enable_ddg and ddg_used < ddg_cap and business:
            website = ddg_find_website(f"{business} contractor Los Angeles CA")
            ddg_used += 1
            time.sleep(0.35)

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
        print("No rows appended. If this happens, CSLB output headers may differ from our column candidates.")

    print(f"DDG used: {ddg_used} (cap={ddg_cap})")
    print("Done.")


if __name__ == "__main__":
    main()
