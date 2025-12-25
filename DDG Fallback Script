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

# Limit per run (default 50 for DDG; adjust as you like)
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))

# Polite pacing between searches
DDG_SLEEP_SECONDS = float(os.environ.get("DDG_SLEEP_SECONDS", "2.5"))


# ========= SHEET COLUMN MAP (1-indexed) =========
# A = Recipient (Company)
# C = Recipient (HQ) Address
# J = row_type
# L = Website output (shared)
# O = ddg_lookup_status (new)
# P = ddg_debug (new)
COL_COMPANY = 1           # A
COL_ADDRESS = 3           # C
COL_ROW_TYPE = 10         # J
COL_WEBSITE_OUT = 12      # L
COL_DDG_STATUS_OUT = 15   # O
COL_DDG_DEBUG_OUT = 16    # P

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
}


def normalize_domain_from_anything(url_or_domain: str) -> str:
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


def ddg_search_candidates(query: str) -> list[str]:
    ddg_url = "https://html.duckduckgo.com/html/"
    r = requests.post(
        ddg_url,
        data={"q": query},
        timeout=30,
        headers={"User-Agent": "Mozilla/5.0 (compatible; CompanyWebsiteFinder/1.0)"}
    )
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "lxml")
    links = soup.select("a.result__a")

    out = []
    for a in links[:12]:
        href = (a.get("href") or "").strip()
        if not href:
            continue
        out.append(href)
    return out


def choose_best_candidate(hrefs: list[str], exact_domains: set, contains_patterns: list) -> tuple[str, str]:
    """
    Returns (website, debug_note)
    """
    for href in hrefs:
        # Skip obvious files
        if re.search(r"\.(pdf|doc|docx|xls|xlsx)$", href, re.IGNORECASE):
            continue

        domain = normalize_domain_from_anything(href)
        if not domain:
            continue

        if is_blacklisted(domain, exact_domains, contains_patterns):
            continue

        return canonical_https(domain), f"picked={domain}"

    return "", "no_acceptable_result"


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

    # Read A:P so O/P exist in range for safety
    values = ws.get_values("A:P")
    if not values or len(values) < 2:
        print("No data found in Companies_Enrichment.")
        return

    updates = []
    processed = 0

    for i in range(1, len(values)):
        row_num = i + 1
        row = values[i]
        while len(row) < 16:  # up to P
            row.append("")

        company = (row[COL_COMPANY - 1] or "").strip()
        address = (row[COL_ADDRESS - 1] or "").strip()
        row_type = (row[COL_ROW_TYPE - 1] or "").strip().upper()
        existing_site = (row[COL_WEBSITE_OUT - 1] or "").strip()

        # Only company rows, only if website is blank
        if row_type != ROW_TYPE_COMPANY:
            continue
        if not company:
            continue
        if existing_site:
            continue

        query = f"{company} {address}".strip()
        ddg_status = ""
        ddg_debug = ""
        website = ""

        try:
            hrefs = ddg_search_candidates(query)
            website, ddg_debug = choose_best_candidate(hrefs, exact_domains, contains_patterns)
            ddg_status = "FOUND" if website else "NOT_FOUND"
        except Exception as e:
            ddg_status = "ERROR"
            ddg_debug = str(e)[:160]

        updates.append((row_num, website, ddg_status, ddg_debug))
        processed += 1

        time.sleep(DDG_SLEEP_SECONDS)
        if processed >= BATCH_SIZE:
            break

    if not updates:
        print("Nothing to update (no eligible COMPANY rows with blank website).")
        return

    # Write Website (L) + DDG status/debug (O/P)
    batch = []
    for row_num, website, ddg_status, ddg_debug in updates:
        batch.append({
            "range": f"L{row_num}:L{row_num}",
            "values": [[website]]
        })
        batch.append({
            "range": f"O{row_num}:P{row_num}",
            "values": [[ddg_status, ddg_debug]]
        })

    ws.batch_update(batch)
    ok = sum(1 for _, w, _, _ in updates if w)
    print(f"Done. Processed {processed} companies; wrote {ok} websites; updated {len(updates)} rows (L + O:P).")


if __name__ == "__main__":
    main()
