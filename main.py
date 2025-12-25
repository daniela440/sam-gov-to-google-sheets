import os
import json
import time
import re
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials


# ====== CONFIG (from GitHub Secrets) ======
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]

# New tab names (defaults)
SHEET_TAB_NAME = os.environ.get("SHEET_TAB_NAME", "Companies_Enrichment")
BLACKLIST_TAB_NAME = os.environ.get("BLACKLIST_TAB_NAME", "Blacklist_Rules")

# How many rows to process per run (safe batch size)
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))

# Polite pacing between searches (seconds)
SLEEP_SECONDS = float(os.environ.get("SLEEP_SECONDS", "2.5"))

GOOGLE_CREDENTIALS_JSON = os.environ["GOOGLE_CREDENTIALS_JSON"]


# Hard blocks: social, directories, etc. (keep small + stable)
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


# ====== COLUMN DEFINITIONS (1-indexed for readability) ======
# Companies_Enrichment per your request:
# A = Recipient (Company)
# C = Recipient (HQ) Address
# J = row_type
# L = website output
COL_COMPANY_NAME = 1   # A
COL_HQ_ADDRESS   = 3   # C
COL_ROW_TYPE     = 10  # J
COL_WEBSITE_OUT  = 12  # L

ROW_TYPE_COMPANY = "COMPANY"


def normalize_domain_from_anything(url_or_domain: str) -> str:
    """
    Normalize URL/domain similar to your Sheets formula:
    - lower
    - strip scheme
    - strip path/query/hash
    - strip leading www.
    """
    if not url_or_domain:
        return ""

    s = str(url_or_domain).strip().lower()

    # If it's not a URL, add a scheme so urlparse works
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


def load_blacklist_rules(ws_blacklist):
    """
    Reads Blacklist_Rules tab with columns:
      rule_type | match_value | reason | example_url | enabled

    Returns:
      exact_domains: set[str]
      contains_patterns: list[str]
    """
    values = ws_blacklist.get_all_values()
    if not values or len(values) < 2:
        return set(), []

    header = [str(h or "").strip().lower() for h in values[0]]
    required = ["rule_type", "match_value", "enabled"]
    for r in required:
        if r not in header:
            raise ValueError(
                f'Blacklist_Rules must include headers: {", ".join(required)} '
                f"(found: {values[0]})"
            )

    i_type = header.index("rule_type")
    i_val = header.index("match_value")
    i_enabled = header.index("enabled")

    exact_domains = set()
    contains_patterns = []

    for row in values[1:]:
        # pad row
        while len(row) <= max(i_type, i_val, i_enabled):
            row.append("")

        rule_type = str(row[i_type] or "").strip().upper()
        match_value = str(row[i_val] or "").strip().lower()
        enabled = str(row[i_enabled] or "").strip().upper()

        if enabled not in ("TRUE", "YES", "1"):
            continue
        if not match_value:
            continue

        # store normalized values
        if rule_type == "EXACT_DOMAIN":
            # allow users to paste full URLs; normalize to domain
            exact_domains.add(normalize_domain_from_anything(match_value) or match_value)
        elif rule_type == "DOMAIN_CONTAINS":
            contains_patterns.append(match_value)

    return exact_domains, contains_patterns


def is_blacklisted(domain: str, exact_domains: set, contains_patterns: list) -> bool:
    if not domain:
        return True

    d = domain.lower().strip()

    # hard-block list
    if d in BLOCKED_DOMAINS:
        return True

    # sheet-driven rules
    if d in exact_domains:
        return True

    for pat in contains_patterns:
        if pat and pat in d:
            return True

    return False


def extract_official_site_from_ddg(company: str, address: str, exact_domains: set, contains_patterns: list) -> str:
    """
    DuckDuckGo HTML endpoint search:
    - take first acceptable result
    - reject by blacklist rules
    - reject obvious files
    - return https://{domain}
    """
    query = f"{company} {address}".strip()
    if not query:
        return ""

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

    for a in links[:12]:
        href = (a.get("href") or "").strip()
        if not href:
            continue

        # Skip obvious PDFs or odd files
        if re.search(r"\.(pdf|doc|docx|xls|xlsx)$", href, re.IGNORECASE):
            continue

        domain = normalize_domain_from_anything(href)
        if not domain:
            continue

        if is_blacklisted(domain, exact_domains, contains_patterns):
            continue

        return f"https://{domain}"

    return ""


def main():
    # Google auth
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_info = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    client = gspread.authorize(creds)

    # Open sheets
    sh = client.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(SHEET_TAB_NAME)
    ws_blacklist = sh.worksheet(BLACKLIST_TAB_NAME)

    # Load blacklist rules
    exact_domains, contains_patterns = load_blacklist_rules(ws_blacklist)

    # Read a wide enough range to include L (12th column).
    # Using A:L ensures we can read row_type (J) and website output (L).
    values = ws.get_values("A:L")

    if not values or len(values) < 2:
        print("No data found.")
        return

    rows_to_update = []
    processed = 0

    # Iterate rows (skip header)
    for i in range(1, len(values)):
        row_num = i + 1  # sheet rows are 1-indexed

        row = values[i]
        # Ensure row has at least 12 columns (L is 12th)
        while len(row) < 12:
            row.append("")

        company = (row[COL_COMPANY_NAME - 1] or "").strip()   # A
        address = (row[COL_HQ_ADDRESS - 1] or "").strip()     # C
        row_type = (row[COL_ROW_TYPE - 1] or "").strip().upper()  # J
        existing_site = (row[COL_WEBSITE_OUT - 1] or "").strip()  # L

        # Process only company header rows
        if row_type != ROW_TYPE_COMPANY:
            continue

        # Skip if missing inputs
        if not company:
            continue
        if not address:
            continue

        # Skip if already populated
        if existing_site:
            continue

        website = extract_official_site_from_ddg(company, address, exact_domains, contains_patterns)

        # If we found one, store normalized domain (or keep full https://domain; your call)
        # You asked: "add the website to column L" — I’ll store https://domain for consistency.
        rows_to_update.append((row_num, website))

        processed += 1
        time.sleep(SLEEP_SECONDS)

        if processed >= BATCH_SIZE:
            break

    if not rows_to_update:
        print("Nothing to update (no eligible COMPANY rows with blank website in this batch).")
        return

    # Batch update Column L
    data = [{"range": f"L{row_num}", "values": [[website]]} for row_num, website in rows_to_update]
    ws.batch_update(data)

    print(f"Done. Processed {processed} companies; updated {len(rows_to_update)} cells in column L.")


if __name__ == "__main__":
    main()
