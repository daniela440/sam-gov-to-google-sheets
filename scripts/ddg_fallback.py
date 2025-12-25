import os
import json
import time
import re
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials


# ========= ENV CONFIG =========
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
GOOGLE_CREDENTIALS_JSON = os.environ["GOOGLE_CREDENTIALS_JSON"]

SHEET_TAB_NAME = os.environ.get("SHEET_TAB_NAME", "Companies_Enrichment")
BLACKLIST_TAB_NAME = os.environ.get("BLACKLIST_TAB_NAME", "Blacklist_Rules")

# How many rows to process per run
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))

# Polite pacing between searches (seconds)
DDG_SLEEP_SECONDS = float(os.environ.get("DDG_SLEEP_SECONDS", "2.5"))


# ========= SHEET COLUMN MAP (1-indexed) =========
# A = Recipient (Company)
# C = Recipient (HQ) Address
# J = row_type
# O = DDG website output
# P = ddg_lookup_status
# Q = ddg_debug
COL_COMPANY = 1           # A
COL_ADDRESS = 3           # C
COL_ROW_TYPE = 10         # J

COL_DDG_WEBSITE_OUT = 15  # O
COL_DDG_STATUS_OUT = 16   # P
COL_DDG_DEBUG_OUT = 17    # Q

ROW_TYPE_COMPANY = "COMPANY"


# ========= HARD BLOCKED DOMAINS =========
BLOCKED_DOMAINS = {
    "facebook.com", "www.facebook.com",
    "linkedin.com", "www.linkedin.com",
    "yelp.com", "www.yelp.com",
    "bbb.org", "www.bbb.org",
    "mapquest.com", "www.mapquest.com",
    "opencorporates.com", "www.opencorporates.com",
    "dnb.com", "www.dnb.com",
    "bloomberg.com", "www.bloomberg.com",
    "crunchbase.com", "www.crunchbase.com",
    "instagram.com", "www.instagram.com",
    "x.com", "www.x.com",
    "twitter.com", "www.twitter.com",
    "chamberofcommerce.com", "www.chamberofcommerce.com",
    "yellowpages.com", "www.yellowpages.com",
    "angi.com", "www.angi.com",
    "homeadvisor.com", "www.homeadvisor.com",

    # common directory/business profile sites you mentioned
    "bizapedia.com", "www.bizapedia.com",
    "buzzfile.com", "www.buzzfile.com",
    "allbiz.com", "www.allbiz.com",
    "buildzoom.com", "www.buildzoom.com",
    "thebluebook.com", "www.thebluebook.com",
    "opengovus.com", "www.opengovus.com",
    "opencorpdata.com", "www.opencorpdata.com",
    "govcb.com", "www.govcb.com",
}


def normalize_domain_from_anything(url_or_domain: str) -> str:
    """
    Normalize URL/domain:
    - lower
    - strip scheme
    - strip path/query/hash
    - strip leading www.
    """
    if not url_or_domain:
        return ""

    s = str(url_or_domain).strip().lower()

    if not re.match(r"^https?://", s):
        s = "https://" + s

    try:
        p = urlparse(s)
        host = (p.netloc or "").lower().strip()
    except Exception:
        return ""

    if host.startswith("www."):
        host = host[4:]

    return host


def canonical_https(url_or_domain: str) -> str:
    d = normalize_domain_from_anything(url_or_domain)
    return f"https://{d}" if d else ""


def load_blacklist_rules(ws_blacklist):
    """
    Blacklist_Rules headers expected:
      rule_type | match_value | reason | example_url | enabled
    rule_type: EXACT_DOMAIN or DOMAIN_CONTAINS
    """
    values = ws_blacklist.get_all_values()
    if not values or len(values) < 2:
        return set(), []

    header = [str(h or "").strip().lower() for h in values[0]]
    required = ["rule_type", "match_value", "enabled"]
    for r in required:
        if r not in header:
            raise ValueError(
                f'Blacklist_Rules must include headers: {", ".join(required)}; found={values[0]}'
            )

    i_type = header.index("rule_type")
    i_val = header.index("match_value")
    i_enabled = header.index("enabled")

    exact_domains = set()
    contains_patterns = []

    for row in values[1:]:
        while len(row) <= max(i_type, i_val, i_enabled):
            row.append("")

        rule_type = str(row[i_type] or "").strip().upper()
        match_value = str(row[i_val] or "").strip().lower()
        enabled = str(row[i_enabled] or "").strip().upper()

        if enabled not in ("TRUE", "YES", "1"):
            continue
        if not match_value:
            continue

        if rule_type == "EXACT_DOMAIN":
            exact_domains.add(normalize_domain_from_anything(match_value) or match_value)
        elif rule_type == "DOMAIN_CONTAINS":
            contains_patterns.append(match_value)

    return exact_domains, contains_patterns


def is_blacklisted(domain: str, exact_domains: set, contains_patterns: list) -> bool:
    if not domain:
        return True

    d = domain.lower().strip()

    if d in BLOCKED_DOMAINS:
        return True
    if d in exact_domains:
        return True
    for pat in contains_patterns:
        if pat and pat in d:
            return True

    return False


def ddg_fetch_result_links(query: str) -> list[str]:
    """
    Uses DuckDuckGo HTML endpoint and returns top result hrefs.
    """
    ddg_url = "https://html.duckduckgo.com/html/"
    r = requests.post(
        ddg_url,
        data={"q": query},
        timeout=30,
        headers={"User-Agent": "Mozilla/5.0 (compatible; DDGFallback/1.0)"}
    )
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "lxml")
    links = soup.select("a.result__a")

    hrefs = []
    for a in links[:15]:
        href = (a.get("href") or "").strip()
        if href:
            hrefs.append(href)

    return hrefs


def choose_best_candidate(hrefs: list[str], exact_domains: set, contains_patterns: list) -> tuple[str, str]:
    """
    Returns (website, debug_note)
    - Picks the first acceptable domain after blacklist filtering.
    """
    checked = 0

    for href in hrefs:
        checked += 1

        # Skip obvious files
        if re.search(r"\.(pdf|doc|docx|xls|xlsx)$", href, re.IGNORECASE):
            continue

        domain = normalize_domain_from_anything(href)
        if not domain:
            continue

        if is_blacklisted(domain, exact_domains, contains_patterns):
            continue

        return canonical_https(domain), f"picked={domain};checked={checked}"

    return "", f"no_acceptable_result;checked={checked}"


def main():
    # Google auth
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_info = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    client = gspread.authorize(creds)

    sh = client.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(SHEET_TAB_NAME)
    ws_blacklist = sh.worksheet(BLACKLIST_TAB_NAME)

    exact_domains, contains_patterns = load_blacklist_rules(ws_blacklist)

    # Read A:Q (so we can safely access up to column Q)
    values = ws.get_values("A:Q")
    if not values or len(values) < 2:
        print("No data found in Companies_Enrichment.")
        return

    updates = []
    processed = 0

    for i in range(1, len(values)):
        row_num = i + 1
        row = values[i]

        # Ensure row has at least 17 cols (Q)
        while len(row) < 17:
            row.append("")

        company = (row[COL_COMPANY - 1] or "").strip()
        address = (row[COL_ADDRESS - 1] or "").strip()
        row_type = (row[COL_ROW_TYPE - 1] or "").strip().upper()

        existing_ddg_site = (row[COL_DDG_WEBSITE_OUT - 1] or "").strip()

        # Only COMPANY rows
        if row_type != ROW_TYPE_COMPANY:
            continue
        if not company:
            continue

        # Don't redo work if DDG already filled column O
        if existing_ddg_site:
            continue

        query = f"{company} {address}".strip()

        ddg_site = ""
        ddg_status = ""
        ddg_debug = ""

        if not query:
            ddg_status = "SKIP_NO_QUERY"
            ddg_debug = "empty_company_and_address"
        else:
            try:
                hrefs = ddg_fetch_result_links(query)
                ddg_site, ddg_debug = choose_best_candidate(hrefs, exact_domains, contains_patterns)
                ddg_status = "FOUND" if ddg_site else "NOT_FOUND"
            except Exception as e:
                ddg_status = "ERROR"
                ddg_debug = f"{type(e).__name__}: {str(e)[:140]}"

        updates.append((row_num, ddg_site, ddg_status, ddg_debug))
        processed += 1

        time.sleep(DDG_SLEEP_SECONDS)
        if processed >= BATCH_SIZE:
            break

    if not updates:
        print("Nothing to update (no eligible COMPANY rows needing DDG website).")
        return

    # Batch update O:P:Q
    batch = []
    for row_num, site, status, debug in updates:
        batch.append({
            "range": f"O{row_num}:Q{row_num}",
            "values": [[site, status, debug]]
        })

    ws.batch_update(batch)

    ok = sum(1 for _, site, _, _ in updates if site)
    print(f"Done. Processed {processed} companies; wrote {ok} DDG sites; updated {len(updates)} rows (O:Q).")


if __name__ == "__main__":
    main()
