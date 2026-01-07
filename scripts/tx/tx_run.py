import os
import re
import time
import json
import urllib.parse
from typing import Optional, List, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# =========================
# CONFIG (env-driven)
# =========================

CREDS_ENV = "ATX_GOOGLE_CREDENTIALS_JSON"

SHEET_ID = os.environ.get("TX_SHEET_ID")
TAB_NAME = os.environ.get("TX_TAB_NAME", "Awards_Raw_TX")

# Column letters (1-indexed in gspread update)
# We LOOK at these columns:
COL_COMPANY = os.environ.get("TX_COL_COMPANY", "B")
COL_CONTACT = os.environ.get("TX_COL_CONTACT", "C")  # optional
COL_ADDRESS = os.environ.get("TX_COL_ADDRESS", "D")
COL_CITY    = os.environ.get("TX_COL_CITY", "E")
COL_PHONE   = os.environ.get("TX_COL_PHONE", "F")

# We WRITE the website to this column:
COL_WEBSITE = os.environ.get("TX_COL_WEBSITE", "G")

# Limit how many websites we enrich per run/day
MAX_ENRICH = int(os.environ.get("TX_MAX_ENRICH", "10"))

# DuckDuckGo HTML endpoint (simple)
DDG_URL = "https://duckduckgo.com/html/"

# Throttling
SLEEP_BETWEEN = float(os.environ.get("TX_SLEEP_SECONDS", "2.0"))
HTTP_TIMEOUT = int(os.environ.get("TX_HTTP_TIMEOUT", "25"))

# Exclude obvious non-company destinations
BAD_DOMAINS = {
    "facebook.com", "m.facebook.com",
    "instagram.com",
    "linkedin.com",
    "x.com", "twitter.com",
    "yelp.com",
    "bbb.org",
    "mapquest.com",
    "yellowpages.com",
    "angi.com", "homeadvisor.com",
    "opencorporates.com",
    "buzzfile.com",
    "dnb.com",
}


# =========================
# Helpers
# =========================

def col_to_index(col_letter: str) -> int:
    """Convert Excel column letter(s) to 1-based index."""
    col_letter = col_letter.strip().upper()
    n = 0
    for ch in col_letter:
        if "A" <= ch <= "Z":
            n = n * 26 + (ord(ch) - ord("A") + 1)
    return n

def norm(s: Optional[str]) -> str:
    return (s or "").strip()

def normalize_phone(phone: str) -> str:
    digits = re.sub(r"\D+", "", phone or "")
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits

def root_domain(url: str) -> str:
    url = url.strip()
    url = re.sub(r"^https?://", "", url, flags=re.I)
    url = url.split("/")[0]
    return url.lower()

def is_bad_domain(url: str) -> bool:
    d = root_domain(url)
    if not d:
        return True
    # strip leading www.
    if d.startswith("www."):
        d = d[4:]
    # exact or suffix match
    return any(d == bd or d.endswith("." + bd) for bd in BAD_DOMAINS)

def choose_best_url(urls: List[str], company: str) -> Optional[str]:
    """Pick a good candidate: prefer non-bad domains, prefer shorter/homepage-like."""
    cleaned = []
    for u in urls:
        u = norm(u)
        if not u:
            continue
        if not u.lower().startswith("http"):
            continue
        if is_bad_domain(u):
            continue
        cleaned.append(u)

    if not cleaned:
        return None

    # Prefer homepage-ish URLs (no deep paths), then shortest
    cleaned.sort(key=lambda u: (u.count("/"), len(u)))
    return cleaned[0]

def build_query(company: str, contact: str, address: str, city: str, phone: str) -> str:
    parts = [company]
    if contact:
        parts.append(contact)
    if city:
        parts.append(city)
    # phone is strong when present
    ph = normalize_phone(phone)
    if ph:
        parts.append(ph)
    # address can help, but keep it light
    if address:
        # keep first ~40 chars to avoid overly long query
        parts.append(address[:40])
    return " ".join([p for p in parts if p]).strip()

def ddg_search_urls(query: str, timeout: int = 25) -> List[str]:
    """
    Uses DuckDuckGo HTML results (basic).
    Returns list of result URLs (best-effort).
    """
    params = {"q": query}
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        )
    }
    r = requests.get(DDG_URL, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    html = r.text

    # DDG /html/ typically uses links like: <a class="result__a" href="...">
    urls = re.findall(r'class="result__a"[^>]+href="([^"]+)"', html)
    # Some are redirect links; decode if needed
    out = []
    for u in urls:
        u = u.replace("&amp;", "&")
        # If DDG uses "/l/?kh=-1&uddg=<ENCODED>"
        m = re.search(r"[?&]uddg=([^&]+)", u)
        if m:
            try:
                decoded = urllib.parse.unquote(m.group(1))
                out.append(decoded)
                continue
            except Exception:
                pass
        out.append(u)
    return out


# =========================
# Google Sheets
# =========================

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


def main():
    if not SHEET_ID:
        raise RuntimeError("Missing TX_SHEET_ID env var")

    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(TAB_NAME)

    # Column indexes (1-based)
    idx_company = col_to_index(COL_COMPANY)
    idx_contact = col_to_index(COL_CONTACT)
    idx_address = col_to_index(COL_ADDRESS)
    idx_city    = col_to_index(COL_CITY)
    idx_phone   = col_to_index(COL_PHONE)
    idx_website = col_to_index(COL_WEBSITE)

    # Read all values once (efficient enough for typical sheet sizes)
    all_values = ws.get_all_values()
    if len(all_values) < 2:
        print("Sheet has no data rows.")
        return

    # Build candidate rows: website empty in column T
    # all_values is 0-based lists; row 1 is headers
    candidates: List[Tuple[int, str, str, str, str, str]] = []
    for r_i in range(1, len(all_values)):  # start at row 2 (index 1)
        row = all_values[r_i]

        # Safely read columns (may be shorter than expected)
        def cell(idx_1based: int) -> str:
            j = idx_1based - 1
            return norm(row[j]) if j < len(row) else ""

        website = cell(idx_website)
        if website:
            continue

        company = cell(idx_company)
        if not company:
            continue

        contact = cell(idx_contact)
        address = cell(idx_address)
        city    = cell(idx_city)
        phone   = cell(idx_phone)

        candidates.append((r_i + 1, company, contact, address, city, phone))  # store real sheet row number

    if not candidates:
        print("No rows need website enrichment (column T already filled).")
        return

    print(f"Rows needing website (blank {COL_WEBSITE}): {len(candidates)}")
    to_process = candidates[:MAX_ENRICH]
    print(f"Processing up to {MAX_ENRICH} rows this run: {len(to_process)}")

    updates = []
    processed = 0

    for sheet_row_num, company, contact, address, city, phone in to_process:
        query = build_query(company, contact, address, city, phone)
        print(f"[{processed+1}/{len(to_process)}] Row {sheet_row_num} | query={query}")

        website = ""
        try:
            urls = ddg_search_urls(query, timeout=HTTP_TIMEOUT)
            website = choose_best_url(urls, company) or ""
        except Exception as e:
            print(f"  ⚠️ search failed: {e}")

        updates.append((sheet_row_num, website))
        processed += 1
        time.sleep(SLEEP_BETWEEN)

    # Apply updates (batch style: individual cells, but grouped)
    if updates:
        cell_list = []
        for row_num, website in updates:
            # write into column T
            cell_list.append(gspread.Cell(row_num, idx_website, website))
        ws.update_cells(cell_list, value_input_option="USER_ENTERED")
        print(f"✅ Updated {len(updates)} website cells in column {COL_WEBSITE}.")

    print("Done.")


if __name__ == "__main__":
    main()
