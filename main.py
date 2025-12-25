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

SAM_API_KEY = os.environ["SAM_API_KEY"]

# Limit per run (default 10)
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "10"))

# pacing between calls
SLEEP_SECONDS = float(os.environ.get("SLEEP_SECONDS", "1.0"))
DDG_SLEEP_SECONDS = float(os.environ.get("DDG_SLEEP_SECONDS", "2.5"))


# ========= SHEET COLUMN MAP (1-indexed) =========
# A = Recipient (Company)
# B = Recipient UEI
# C = Recipient (HQ) Address
# J = row_type
# L = Website output
# M = sam_lookup_status output
COL_COMPANY = 1          # A
COL_UEI = 2              # B
COL_ADDRESS = 3          # C
COL_ROW_TYPE = 10        # J
COL_WEBSITE_OUT = 12     # L
COL_STATUS_OUT = 13      # M

ROW_TYPE_COMPANY = "COMPANY"


# ========= HARD BLOCKED DOMAINS =========
# Keep small/stable; everything else lives in Blacklist_Rules.
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
    """
    Normalize URL/domain similarly to your Google Sheets formula:
    - lower
    - strip scheme
    - strip path/query/hash
    - strip leading www.
    """
    if not url_or_domain:
        return ""

    s = str(url_or_domain).strip().lower()

    # add scheme if missing so urlparse behaves
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


# ========= SAM.gov LOOKUP =========
def sam_lookup_entity_by_uei(uei: str) -> dict:
    """
    Query SAM.gov Entity Information API by UEI.
    Endpoint:
      https://api.sam.gov/entity-information/v4/entities?api_key=...&ueiSAM=...
    """
    uei = (uei or "").strip()
    if not uei:
        return {}

    url = "https://api.sam.gov/entity-information/v4/entities"
    params = {
        "api_key": SAM_API_KEY,
        "ueiSAM": uei,
        "includeSections": "coreData,entityRegistration"
    }

    try:
        r = requests.get(url, params=params, timeout=30, headers={"Accept": "application/json"})
    except Exception:
        return {}

    if r.status_code != 200:
        return {}

    try:
        return r.json()
    except Exception:
        return {}


def find_candidate_urls(obj) -> list[str]:
    """
    Recursively walk JSON and collect strings likely to be URLs.
    We do not assume a single fixed schema path.
    """
    candidates = []

    def walk(x):
        if isinstance(x, dict):
            for k, v in x.items():
                if isinstance(v, str):
                    if re.search(r"(website|web|url)", str(k), re.IGNORECASE):
                        candidates.append(v)
                    elif re.match(r"^https?://", v.strip(), re.IGNORECASE):
                        candidates.append(v)
                else:
                    walk(v)
        elif isinstance(x, list):
            for item in x:
                walk(item)

    walk(obj)

    # de-dupe preserving order
    seen = set()
    out = []
    for u in candidates:
        u = (u or "").strip()
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def choose_best_official_site(candidates: list[str], exact_domains: set, contains_patterns: list) -> str:
    """
    Heuristics:
    - normalize to domain
    - reject blacklisted
    - reject file links
    - prefer non-subdomain and shorter domains
    """
    best = ""
    best_score = -10**9

    for raw in candidates:
        if re.search(r"\.(pdf|doc|docx|xls|xlsx)$", raw, re.IGNORECASE):
            continue

        domain = normalize_domain_from_anything(raw)
        if not domain:
            continue

        if is_blacklisted(domain, exact_domains, contains_patterns):
            continue

        parts = domain.split(".")
        subdomain_penalty = max(0, len(parts) - 2)
        score = 100 - len(domain) - (10 * subdomain_penalty)

        if score > best_score:
            best_score = score
            best = canonical_https(domain)

    return best


# ========= DDG FALLBACK =========
def ddg_search_best_site(company: str, address: str, exact_domains: set, contains_patterns: list) -> str:
    """
    Only called when SAM returned NOT_FOUND.
    - Search DDG HTML
    - Take first acceptable non-blacklisted domain
    """
    query = f"{company} {address}".strip()
    if not query:
        return ""

    ddg_url = "https://html.duckduckgo.com/html/"
    try:
        r = requests.post(
            ddg_url,
            data={"q": query},
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0 (compatible; CompanyWebsiteFinder/1.0)"}
        )
        r.raise_for_status()
    except Exception:
        return ""

    soup = BeautifulSoup(r.text, "lxml")
    links = soup.select("a.result__a")

    for a in links[:12]:
        href = (a.get("href") or "").strip()
        if not href:
            continue

        # Skip obvious files
        if re.search(r"\.(pdf|doc|docx|xls|xlsx)$", href, re.IGNORECASE):
            continue

        domain = normalize_domain_from_anything(href)
        if not domain:
            continue

        if is_blacklisted(domain, exact_domains, contains_patterns):
            continue

        return canonical_https(domain)

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

    sh = client.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(SHEET_TAB_NAME)
    ws_blacklist = sh.worksheet(BLACKLIST_TAB_NAME)

    exact_domains, contains_patterns = load_blacklist_rules(ws_blacklist)

    # Read A:M (so we can check status too if needed)
    values = ws.get_values("A:M")
    if not values or len(values) < 2:
        print("No data found in Companies_Enrichment.")
        return

    updates = []
    processed = 0

    for i in range(1, len(values)):
        row_num = i + 1
        row = values[i]
        while len(row) < 13:
            row.append("")

        company = (row[COL_COMPANY - 1] or "").strip()
        uei = (row[COL_UEI - 1] or "").strip()
        address = (row[COL_ADDRESS - 1] or "").strip()
        row_type = (row[COL_ROW_TYPE - 1] or "").strip().upper()
        existing_site = (row[COL_WEBSITE_OUT - 1] or "").strip()
        existing_status = (row[COL_STATUS_OUT - 1] or "").strip().upper()

        if row_type != ROW_TYPE_COMPANY:
            continue
        if not company:
            continue
        if existing_site:
            continue

        # If you want to avoid reprocessing permanent outcomes, uncomment:
        # if existing_status in ("FOUND", "NOT_FOUND", "NO_UEI"):
        #     continue

        website = ""
        status = ""

        if not uei:
            status = "NO_UEI"
        else:
            payload = sam_lookup_entity_by_uei(uei)
            if not payload:
                status = "API_ERROR"
            else:
                candidates = find_candidate_urls(payload)
                website = choose_best_official_site(candidates, exact_domains, contains_patterns)

                if website:
                    status = "FOUND"
                else:
                    # SAM found no usable website. Fallback to DDG (only in this case).
                    status = "NOT_FOUND"

                    if address:  # DDG works better with address
                        time.sleep(DDG_SLEEP_SECONDS)
                        ddg_site = ddg_search_best_site(company, address, exact_domains, contains_patterns)
                        if ddg_site:
                            website = ddg_site
                            # distinguish fallback success
                            status = "FOUND_DDG_FALLBACK"

        updates.append((row_num, website, status))
        processed += 1

        time.sleep(SLEEP_SECONDS)
        if processed >= BATCH_SIZE:
            break

    if not updates:
        print("Nothing to update (no eligible COMPANY rows with blank website).")
        return

    # Write L (website) + M (status) together
    batch = []
    for row_num, website, status in updates:
        batch.append({
            "range": f"L{row_num}:M{row_num}",
            "values": [[website, status]]
        })

    ws.batch_update(batch)

    ok = sum(1 for _, w, _ in updates if w)
    print(f"Done. Processed {processed} companies; wrote {ok} websites; updated {len(updates)} rows (L:M).")


if __name__ == "__main__":
    main()
