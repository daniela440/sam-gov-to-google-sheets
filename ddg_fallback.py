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

# Limit per run (default 50)
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))

# Polite pacing between searches
DDG_SLEEP_SECONDS = float(os.environ.get("DDG_SLEEP_SECONDS", "2.5"))

# Quick homepage validation (seconds)
HOMEPAGE_TIMEOUT = float(os.environ.get("HOMEPAGE_TIMEOUT", "10"))

# Minimum score required to accept a DDG candidate
MIN_ACCEPT_SCORE = int(os.environ.get("MIN_ACCEPT_SCORE", "40"))


# ========= SHEET COLUMN MAP (1-indexed) =========
# A = Recipient (Company)
# C = Recipient (HQ) Address
# J = row_type
# L = Website output (shared)
# O = ddg_lookup_status
# P = ddg_debug
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

# ========= SUSPICIOUS KEYWORDS IN DOMAINS (AUTO-REJECT) =========
# Stops new "registry/directory/report/list" variants without manual blacklisting.
SUSPICIOUS_DOMAIN_CONTAINS = [
    "registry", "directory", "report", "naics", "cage", "bidhub", "bid-hub",
    "opengov", "opencorp", "bizprofile", "buzzfile", "allbiz", "buildzoom",
    "thebluebook", "bluebook", "corporationwiki", "cortera", "dnb", "dun",
    "sec", "sos", "companyregistry", "usaspending", "sam.gov", "govcb",
    "govt", "governmentbid", "contractorinfo", "contractor-info", "listings",
    "listing", "database", "search", "companies", "companysearch", "entitysearch",
]

# ========= HOMEPAGE PATTERNS THAT SCREAM "DIRECTORY/REGISTRY" =========
HOMEPAGE_BAD_PHRASES = [
    "business registry",
    "company registry",
    "company profile",
    "company directory",
    "search the database",
    "search our database",
    "entity search",
    "cage code",
    "naics code",
    "government contractors",
    "contractor directory",
    "find company information",
    "lookup company",
    "free company search",
    "public records",
    "state records",
    "registered agent",
    "sec filings",
]


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


def normalize_company_tokens(company: str) -> list[str]:
    """
    Create a small set of meaningful tokens from company name for scoring.
    Removes corporate suffixes and very short noise words.
    """
    if not company:
        return []

    c = company.lower()
    c = re.sub(r"[^a-z0-9\s]", " ", c)
    raw = [t for t in c.split() if t]

    stop = {
        "inc", "incorporated", "llc", "ltd", "limited", "co", "company", "corp",
        "corporation", "group", "holdings", "holding", "the", "and", "of", "services",
        "service", "solutions", "international", "global", "industries", "industry",
    }
    tokens = []
    for t in raw:
        if t in stop:
            continue
        if len(t) < 3:
            continue
        tokens.append(t)

    # Keep it small (for predictable scoring)
    return tokens[:4]


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

    # Auto-reject suspicious domain patterns (registry/directory/report/etc.)
    for pat in SUSPICIOUS_DOMAIN_CONTAINS:
        if pat and pat in d:
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
        headers={"User-Agent": "Mozilla/5.0 (compatible; CompanyWebsiteFinder/2.0)"}
    )
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "lxml")
    links = soup.select("a.result__a")

    out = []
    for a in links[:15]:
        href = (a.get("href") or "").strip()
        if href:
            out.append(href)
    return out


def score_candidate(domain: str, company_tokens: list[str]) -> int:
    """
    Higher score = more likely to be official website.
    """
    if not domain:
        return -10**9

    d = domain.lower().strip()
    score = 0

    # Penalize long domains and deep subdomains
    score -= min(len(d), 80)  # length penalty
    parts = d.split(".")
    sub_penalty = max(0, len(parts) - 2)
    score -= 15 * sub_penalty

    # Reward presence of company tokens in the domain
    for t in company_tokens:
        if t and t in d:
            score += 30

    # Small reward for "brand-like" short domains
    if len(d) <= 18:
        score += 10

    return score


def homepage_looks_like_directory(url: str) -> tuple[bool, str]:
    """
    Quick validation: fetch homepage text and look for directory/registry phrases.
    Returns (is_bad, reason).
    """
    if not url:
        return True, "no_url"

    try:
        r = requests.get(
            url,
            timeout=HOMEPAGE_TIMEOUT,
            headers={"User-Agent": "Mozilla/5.0 (compatible; CompanyWebsiteFinder/2.0)"}
        )
    except Exception as e:
        # If homepage fetch fails, we do NOT automatically reject (some sites block bots).
        # We just note it.
        return False, f"homepage_fetch_failed:{type(e).__name__}"

    if r.status_code >= 400:
        return False, f"homepage_http_{r.status_code}"

    text = (r.text or "").lower()
    # strip scripts/styles quickly to reduce noise (simple heuristic)
    text = re.sub(r"<script.*?>.*?</script>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style.*?>.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)

    for phrase in HOMEPAGE_BAD_PHRASES:
        if phrase in text:
            return True, f"homepage_phrase:{phrase}"

    return False, "homepage_ok"


def choose_best_candidate(hrefs: list[str], company: str, exact_domains: set, contains_patterns: list) -> tuple[str, str, int]:
    """
    Returns (website, debug_note, score)
    - Filters blacklisted domains
    - Scores remaining candidates
    - Validates homepage for directory/registry fingerprints
    - Accepts only if score >= MIN_ACCEPT_SCORE
    """
    company_tokens = normalize_company_tokens(company)
    candidates = []

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

        score = score_candidate(domain, company_tokens)
        candidates.append((score, domain))

    if not candidates:
        return "", f"no_acceptable_domain;checked={checked}", -10**9

    # Highest score first
    candidates.sort(reverse=True, key=lambda x: x[0])

    # Try top few candidates with homepage validation
    for score, domain in candidates[:6]:
        url = canonical_https(domain)
        is_bad, reason = homepage_looks_like_directory(url)
        if is_bad:
            continue

        if score < MIN_ACCEPT_SCORE:
            return "", f"low_confidence;top={domain};score={score};checked={checked};homepage={reason}", score

        return url, f"picked={domain};score={score};checked={checked};homepage={reason}", score

    return "", f"rejected_by_homepage_validation;top={candidates[0][1]};checked={checked}", candidates[0][0]


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
        while len(row) < 16:
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

        if not query:
            ddg_status = "SKIP_NO_QUERY"
            ddg_debug = "empty_company_and_address"
        else:
            try:
                hrefs = ddg_search_candidates(query)
                website, ddg_debug, score = choose_best_candidate(
                    hrefs, company, exact_domains, contains_patterns
                )

                if website:
                    ddg_status = "FOUND"
                else:
                    # distinguish cases that need human review vs truly not found
                    if ddg_debug.startswith("low_confidence"):
                        ddg_status = "REVIEW"
                    else:
                        ddg_status = "NOT_FOUND"

            except Exception as e:
                ddg_status = "ERROR"
                ddg_debug = f"{type(e).__name__}: {str(e)[:140]}"

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
            "values": [[ddg_status, ddg_debug[:160]]]
        })

    ws.batch_update(batch)

    ok = sum(1 for _, w, _, _ in updates if w)
    review = sum(1 for _, w, s, _ in updates if (not w and s == "REVIEW"))
    print(
        f"Done. Processed {processed} companies; wrote {ok} websites; "
        f"flagged {review} for REVIEW; updated {len(updates)} rows (L + O:P)."
    )


if __name__ == "__main__":
    main()
