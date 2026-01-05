import os
import json
import time
import hashlib
import re
from datetime import datetime, timezone
from urllib.parse import quote, urlparse, unquote

import requests
import gspread
from google.oauth2.service_account import Credentials


# -----------------------------
# Config
# -----------------------------
ALLOWED_NAICS = {"238210", "236220", "237310", "238220", "238120"}

# NYC Open Data datasets (Socrata IDs)
DATASET_JOB_FILINGS = "w9ak-ipjd"        # DOB NOW: Build – Job Application Filings
DATASET_APPROVED_PERMITS = "rbx6-tga4"   # DOB NOW: Build – Approved Permits

# DuckDuckGo enrichment controls (safe defaults)
DEFAULT_DDG_ENRICH_LIMIT = 10     # max companies enriched per run
DEFAULT_DDG_SLEEP_SECONDS = 2.0   # delay between DDG calls (be conservative)
DEFAULT_SITE_FETCH_TIMEOUT = 15   # seconds

# Domains we generally do NOT want as "company website"
EXCLUDED_DOMAINS = {
    "facebook.com", "m.facebook.com", "linkedin.com", "instagram.com", "twitter.com", "x.com",
    "yelp.com", "opengovus.com", "buzzfile.com", "dnb.com", "bloomberg.com",
    "mapquest.com", "wikipedia.org", "yellowpages.com", "manta.com",
    "glassdoor.com", "indeed.com", "crunchbase.com",
    "data.cityofnewyork.us", "nyc.gov",
}


def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def normalize_str(x) -> str:
    return str(x).strip() if x is not None else ""


def first_present(record: dict, candidates: list[str]) -> str:
    for k in candidates:
        v = record.get(k)
        v = normalize_str(v)
        if v:
            return v
    return ""


def socrata_get(dataset_id: str, where: str | None, limit: int, offset: int) -> list[dict]:
    base = f"https://data.cityofnewyork.us/resource/{dataset_id}.json"
    headers = {}
    token = os.environ.get("NYC_SOCRATA_APP_TOKEN")
    if token:
        headers["X-App-Token"] = token

    params = {"$limit": limit, "$offset": offset}
    if where:
        params["$where"] = where

    r = requests.get(base, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()


def get_gspread_client():
    creds_json = os.environ.get("NYC_GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("Missing NYC_GOOGLE_CREDENTIALS_JSON secret")
    creds_dict = json.loads(creds_json)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)


def header_map(ws) -> dict:
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("Row 1 headers are empty. Paste your column headers in row 1.")
    return {h: i + 1 for i, h in enumerate(headers)}  # 1-based index


def load_existing_award_ids(ws) -> set[str]:
    col_values = ws.col_values(1)  # Column A = Award ID
    return {v.strip() for v in col_values[1:] if v and v.strip()}


def extract_job_filing_number(record: dict) -> str:
    return first_present(record, ["job_filing_number"])


def year_of(d: str) -> int | None:
    if not d:
        return None
    try:
        return int(d[:4])
    except Exception:
        return None


def month_of(d: str) -> int | None:
    if not d or len(d) < 7:
        return None
    try:
        return int(d[5:7])
    except Exception:
        return None


def infer_naics(job: dict, permit: dict | None) -> str:
    """
    Conservative NAICS inference from work-type signals.
    If uncertain, default to 236220 (allowed).
    """
    filing_status = first_present(job, ["filing_status"])
    permit_status = first_present(permit or {}, ["permit_status"])
    permit_work_type = first_present(permit or {}, ["work_type", "work_permit"])
    plumbing = first_present(job, ["plumbing_work_type"])
    sprinkler = first_present(job, ["sprinkler_work_type"])
    mechanical = first_present(job, ["mechanical_systems_work_type_"])
    boiler = first_present(job, ["boiler_equipment_work_type_"])
    structural = first_present(job, ["structural_work_type_"])

    text = f"{filing_status} {permit_status} {permit_work_type} {plumbing} {sprinkler} {mechanical} {boiler} {structural}".lower()

    if "elect" in text:
        return "238210"
    if "plumb" in text or "sprink" in text or "hvac" in text or "mechanic" in text or "boiler" in text:
        return "238220"
    if "steel" in text or "struct" in text or "precast" in text:
        return "238120"
    if "bridge" in text or "highway" in text or "street" in text or "road" in text:
        return "237310"

    return "236220"


def naics_description(naics: str) -> str:
    return {
        "238210": "Electrical Contractors and Other Wiring Installation Contractors",
        "236220": "Commercial and Institutional Building Construction",
        "237310": "Highway, Street, and Bridge Construction",
        "238220": "Plumbing, Heating, and Air-Conditioning Contractors",
        "238120": "Structural Steel and Precast Concrete Contractors",
    }.get(naics, "")


def score_and_quarter_start(job: dict, permit: dict | None) -> tuple[int, str, str]:
    """
    Uses NYC DOB fields:
      - filing_date
      - current_status_date
      - first_permit_date
      - filing_status
      - permit_status (from permits dataset)
      - initial_cost
    """
    score = 0
    rationale = []

    filing_status = first_present(job, ["filing_status"])
    permit_status = first_present(permit or {}, ["permit_status"])

    filing_date = first_present(job, ["filing_date"])
    current_status_date = first_present(job, ["current_status_date"])
    first_permit_date = first_present(job, ["first_permit_date"])

    status_text = f"{filing_status} {permit_status}".lower()
    if "permit" in status_text:
        score += 15
        rationale.append("status_contains_permit(+15)")
    if "approv" in status_text:
        score += 10
        rationale.append("status_contains_approved(+10)")

    if first_permit_date:
        score += 25
        rationale.append("first_permit_date_present(+25)")

    fp_y = year_of(first_permit_date)
    cs_y = year_of(current_status_date)
    f_y = year_of(filing_date)

    if fp_y == 2025:
        score += 20
        rationale.append("first_permit_year=2025(+20)")
    elif cs_y == 2025:
        score += 15
        rationale.append("current_status_year=2025(+15)")
    elif f_y == 2025:
        score += 15
        rationale.append("filing_year=2025(+15)")

    job_type = first_present(job, ["job_type"]).lower()
    if "new" in job_type:
        score += 15
        rationale.append("job_type_new(+15)")
    elif "alter" in job_type:
        score += 10
        rationale.append("job_type_alteration(+10)")

    cost_raw = first_present(job, ["initial_cost"])
    cost = 0.0
    if cost_raw:
        try:
            cost = float(cost_raw.replace(",", "").replace("$", ""))
        except Exception:
            cost = 0.0

    if cost >= 50_000_000:
        score += 15
        rationale.append("initial_cost>=50M(+15)")
    elif cost >= 10_000_000:
        score += 10
        rationale.append("initial_cost>=10M(+10)")
    elif cost >= 1_000_000:
        score += 5
        rationale.append("initial_cost>=1M(+5)")

    start_date = ""
    if score >= 70:
        q2, q3, q4 = "2026-04-01", "2026-07-01", "2026-10-01"
        anchor = first_permit_date or current_status_date or filing_date

        if year_of(anchor) == 2025:
            m = month_of(anchor)
            if m is not None:
                if 1 <= m <= 6:
                    start_date = q2
                    rationale.append("quarter=Q2(anchor Jan-Jun 2025)")
                elif 7 <= m <= 9:
                    start_date = q3
                    rationale.append("quarter=Q3(anchor Jul-Sep 2025)")
                else:
                    start_date = q4
                    rationale.append("quarter=Q4(anchor Oct-Dec 2025)")
            else:
                start_date = q3
                rationale.append("quarter=Q3(default_no_month)")
        else:
            start_date = q3
            rationale.append("quarter=Q3(default_no_2025_anchor)")

    return score, start_date, "; ".join(rationale) if rationale else "No rationale"


def build_links(job_filing_number: str) -> tuple[str, str, str]:
    """
    Returns:
      - api_link: JSON endpoint filtered by job_filing_number
      - ui_link: dataset UI page (human friendly)
      - job_search_link: search by job_filing_number (useful for verification)
    """
    api_link = (
        f"https://data.cityofnewyork.us/resource/{DATASET_JOB_FILINGS}.json"
        f"?$where=job_filing_number='{job_filing_number}'"
    )
    ui_link = f"https://data.cityofnewyork.us/Housing-Development/DOB-NOW-Build-Job-Application-Filings/{DATASET_JOB_FILINGS}"
    job_search_link = f"https://www.google.com/search?q={quote(job_filing_number + ' site:data.cityofnewyork.us')}"
    return api_link, ui_link, job_search_link


# -----------------------------
# Recipient cleanup (fix PR/junk)
# -----------------------------
BAD_NAME_TOKENS = {
    "", "0", "00", "000",
    "n/a", "na", "none", "null", "unknown",
    "pr", "p/r", "p r",
    "tbd", "test",
}

def clean_name(x: str) -> str:
    x = normalize_str(x)
    x = re.sub(r"\s+", " ", x).strip()
    return x

def is_bad_company_name(x: str) -> bool:
    if not x:
        return True
    s = clean_name(x)
    if not s:
        return True

    token = re.sub(r"[^a-z0-9]+", "", s.lower())
    bad_tokens_norm = {re.sub(r"[^a-z0-9]+", "", t.lower()) for t in BAD_NAME_TOKENS}
    if token in bad_tokens_norm:
        return True

    if len(s) <= 2:
        return True
    if re.fullmatch(r"[A-Za-z]\.?", s):
        return True

    return False

def pick_recipient_company(job: dict) -> tuple[str, str]:
    """
    Returns (recipient_name, recipient_source)
    We do not add new columns, but we store the source in notes for traceability.
    """
    owner_business = clean_name(first_present(job, ["owner_s_business_name"]))
    filing_business = clean_name(first_present(job, ["filing_representative_business_name"]))

    if not is_bad_company_name(owner_business):
        return owner_business, "owner_s_business_name"

    if not is_bad_company_name(filing_business):
        return filing_business, "filing_representative_business_name"

    rep_fn = clean_name(first_present(job, ["filing_representative_first_name"]))
    rep_ln = clean_name(first_present(job, ["filing_representative_last_name"]))
    rep_full = f"{rep_fn} {rep_ln}".strip()
    if rep_full:
        return rep_full, "filing_representative_person"

    app_fn = clean_name(first_present(job, ["applicant_first_name"]))
    app_ln = clean_name(first_present(job, ["applicant_last_name"]))
    app_full = f"{app_fn} {app_ln}".strip()
    if app_full:
        return app_full, "applicant_person"

    return "", "missing"


# -----------------------------
# DuckDuckGo enrichment (website + best-effort phone/email)
# -----------------------------
def ddg_html_search(query: str, timeout: int = 30) -> str:
    """
    DuckDuckGo HTML endpoint. Not a guaranteed stable API; keep volume low.
    """
    url = "https://duckduckgo.com/html/"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; ProcurementLeadBot/1.0; +https://example.com/bot)"
    }
    params = {"q": query}
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text


def extract_result_urls_from_ddg_html(html: str, max_urls: int = 8) -> list[str]:
    """
    Extracts outbound result links from DuckDuckGo HTML.
    DDG often uses redirect URLs like /l/?uddg=<encoded>
    """
    urls = []

    # Common pattern: href="/l/?kh=-1&uddg=<ENCODED_URL>"
    for m in re.finditer(r'href="(/l/\?[^"]*uddg=[^"&]+[^"]*)"', html):
        href = m.group(1)
        # get uddg param
        um = re.search(r"uddg=([^&]+)", href)
        if not um:
            continue
        raw = um.group(1)
        try:
            target = unquote(raw)
        except Exception:
            target = raw

        if target.startswith("http://") or target.startswith("https://"):
            urls.append(target)
        if len(urls) >= max_urls:
            break

    # Fallback: sometimes results are direct absolute URLs
    if not urls:
        for m in re.finditer(r'href="(https?://[^"]+)"', html):
            u = m.group(1)
            if "duckduckgo.com" not in u:
                urls.append(u)
            if len(urls) >= max_urls:
                break

    # De-dupe while preserving order
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out[:max_urls]


def domain_of(url: str) -> str:
    try:
        p = urlparse(url)
        host = (p.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def is_excluded_domain(host: str) -> bool:
    if not host:
        return True
    if host in EXCLUDED_DOMAINS:
        return True
    # exclude subdomains of excluded
    for d in EXCLUDED_DOMAINS:
        if host.endswith("." + d):
            return True
    return False


def pick_best_official_website(company: str, city_hint: str, state_hint: str, timeout: int = 30) -> str:
    """
    Best-effort: DDG search for official domain.
    Strategy:
      - Query: "<company> <city> <state> official website"
      - Extract top results; choose first non-excluded domain
    """
    if not company or is_bad_company_name(company):
        return ""

    query_parts = [company]
    if city_hint:
        query_parts.append(city_hint)
    if state_hint:
        query_parts.append(state_hint)
    query_parts.append("official website")
    query = " ".join([p for p in query_parts if p]).strip()

    try:
        html = ddg_html_search(query, timeout=timeout)
        urls = extract_result_urls_from_ddg_html(html, max_urls=8)
        for u in urls:
            host = domain_of(u)
            if not host or is_excluded_domain(host):
                continue
            # Prefer a clean homepage URL
            scheme = urlparse(u).scheme or "https"
            return f"{scheme}://{host}"
    except Exception:
        return ""

    return ""


def fetch_homepage_contact_signals(website: str, timeout: int = DEFAULT_SITE_FETCH_TIMEOUT) -> tuple[str, str]:
    """
    Best-effort parse of homepage HTML to find:
      - mailto: address
      - tel: phone
    Returns (email, phone)
    """
    if not website:
        return "", ""

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; ProcurementLeadBot/1.0; +https://example.com/bot)"
    }

    try:
        r = requests.get(website, headers=headers, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
        html = r.text or ""
    except Exception:
        return "", ""

    # mailto
    email = ""
    m = re.search(r"mailto:([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})", html, flags=re.I)
    if m:
        email = m.group(1).strip()

    # tel (very permissive)
    phone = ""
    t = re.search(r"tel:\s*([0-9+().\-\s]{7,})", html, flags=re.I)
    if t:
        phone = re.sub(r"\s+", " ", t.group(1)).strip()

    return email, phone


def build_company_search_link(company: str, city: str, state: str, job_site: str) -> str:
    q = f"{company} {city} {state}".strip()
    street_hint = normalize_str(job_site.split(",")[0])
    if street_hint:
        q = f"{q} {street_hint}".strip()
    return f"https://www.google.com/search?q={quote(q)}"


def main():
    # Required env
    sheet_id = os.environ.get("NYC_SHEET_ID")
    tab_name = os.environ.get("NYC_TAB_NAME")
    if not sheet_id or not tab_name:
        raise RuntimeError("Missing NYC_SHEET_ID or NYC_TAB_NAME")

    # Runtime controls
    batch_size = int(os.environ.get("NYC_BATCH_SIZE", "200"))
    max_pages = int(os.environ.get("NYC_MAX_PAGES", "5"))
    sleep_seconds = float(os.environ.get("NYC_SLEEP_SECONDS", "0.25"))

    # Scoring controls
    min_score = int(os.environ.get("NYC_MIN_SCORE", "70"))
    discovery_mode = os.environ.get("NYC_DISCOVERY_MODE", "false").lower() == "true"

    # Enrichment controls
    ddg_enrich_limit = int(os.environ.get("NYC_DDG_ENRICH_LIMIT", str(DEFAULT_DDG_ENRICH_LIMIT)))
    ddg_sleep_seconds = float(os.environ.get("NYC_DDG_SLEEP_SECONDS", str(DEFAULT_DDG_SLEEP_SECONDS)))
    ddg_enable_phone_email = os.environ.get("NYC_DDG_ENABLE_PHONE_EMAIL", "true").lower() == "true"

    # 2025-only filter
    where_2025 = (
        "("
        "first_permit_date >= '2025-01-01T00:00:00.000' AND first_permit_date < '2026-01-01T00:00:00.000'"
        ") OR ("
        "current_status_date >= '2025-01-01T00:00:00.000' AND current_status_date < '2026-01-01T00:00:00.000'"
        ") OR ("
        "filing_date >= '2025-01-01T00:00:00.000' AND filing_date < '2026-01-01T00:00:00.000'"
        ")"
    )

    # Debug counters
    c_jobs_pulled = 0
    c_with_award_id = 0
    c_new_not_dupe = 0
    c_naics_allowed = 0
    c_scored_ge_min = 0
    c_assigned_q2026 = 0
    c_appended = 0
    c_enriched_ddg = 0
    c_website_found = 0
    c_phone_found = 0
    c_email_found = 0

    # Connect to Google Sheet
    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab_name)
    hmap = header_map(ws)
    existing_ids = load_existing_award_ids(ws)

    # Pull permits (join by job_filing_number)
    permits_by_job = {}
    for page in range(max_pages):
        permits = socrata_get(DATASET_APPROVED_PERMITS, where=None, limit=batch_size, offset=page * batch_size)
        if not permits:
            break
        for p in permits:
            jid = extract_job_filing_number(p)
            if jid:
                permits_by_job[jid] = p
        time.sleep(sleep_seconds)

    rows_to_append = []
    now = utc_now_str()

    # For rate-limited enrichment
    enrich_budget_remaining = max(0, ddg_enrich_limit)

    for page in range(max_pages):
        jobs = socrata_get(DATASET_JOB_FILINGS, where=where_2025, limit=batch_size, offset=page * batch_size)
        if not jobs:
            break

        c_jobs_pulled += len(jobs)

        for job in jobs:
            award_id = extract_job_filing_number(job)
            if not award_id:
                continue
            c_with_award_id += 1

            if award_id in existing_ids:
                continue
            c_new_not_dupe += 1

            permit = permits_by_job.get(award_id)

            naics = infer_naics(job, permit)
            if naics not in ALLOWED_NAICS:
                continue
            c_naics_allowed += 1

            score, start_date, rationale = score_and_quarter_start(job, permit)
            if score >= min_score:
                c_scored_ge_min += 1
            else:
                continue

            if discovery_mode and not start_date:
                start_date = "2026-07-01"
                rationale = rationale + "; discovery_mode_forced_Q3_2026"

            if start_date in {"2026-04-01", "2026-07-01", "2026-10-01"}:
                c_assigned_q2026 += 1
            else:
                continue

            # Recipient name + source (stored in notes, no new columns)
            company, company_source = pick_recipient_company(job)

            # Addresses: job site vs mailing
            job_site = f"{first_present(job, ['house_no'])} {first_present(job, ['street_name'])}, {first_present(job, ['borough'])} {first_present(job, ['zip'])}".strip()
            owner_addr = f"{first_present(job, ['owner_s_street_name'])}, {first_present(job, ['city'])} {first_present(job, ['state'])} {first_present(job, ['zip'])}".strip()
            filing_rep_addr = f"{first_present(job, ['filing_representative_street_name'])}, {first_present(job, ['filing_representative_city'])} {first_present(job, ['filing_representative_state'])} {first_present(job, ['filing_representative_zip'])}".strip()
            hq_addr = owner_addr if normalize_str(owner_addr) else filing_rep_addr

            desc = first_present(job, ["job_description"])
            est_cost = first_present(job, ["initial_cost"])

            api_link, ui_link, job_search_link = build_links(award_id)

            # Better outreach-ready web search link (company + location)
            city_hint = first_present(job, ["filing_representative_city", "city"])
            state_hint = first_present(job, ["filing_representative_state", "state"])
            web_search_company = build_company_search_link(company, city_hint, state_hint, job_site)

            # -----------------------------
            # DDG enrichment (website + optional phone/email)
            # -----------------------------
            website = ""
            phone = ""
            email = ""

            if enrich_budget_remaining > 0 and company and not is_bad_company_name(company):
                website = pick_best_official_website(company, city_hint, state_hint, timeout=30)
                c_enriched_ddg += 1
                enrich_budget_remaining -= 1
                time.sleep(ddg_sleep_seconds)

                if website:
                    c_website_found += 1

                    if ddg_enable_phone_email:
                        e, p = fetch_homepage_contact_signals(website)
                        if e:
                            email = e
                            c_email_found += 1
                        if p:
                            phone = p
                            c_phone_found += 1

                        # be polite to websites too
                        time.sleep(1.0)

            recipient_id = hashlib.md5(award_id.encode("utf-8")).hexdigest()

            values = {
                "Award ID": award_id,
                "Recipient (Company)": company,
                "Recipient UEI": "",
                "Parent Recipient UEI": "",
                "Parent Recipient DUNS": "",
                "Recipient (HQ) Address": hq_addr,
                "Start Date": start_date,
                "End Date": "",
                "Last Modified Date": now,
                "Award Amount (Obligated)": est_cost,
                "NAICS Code": naics,
                "NAICS Description": naics_description(naics),
                "Awarding Agency": "",
                "Place of Performance": job_site,
                "Description": desc,

                # Prefer human-friendly UI link in Award Link
                "Award Link": ui_link,
                "Recipient Profile Link": "",
                "Web Search Link": web_search_company,

                # enrichment fields (best-effort; do not invent)
                "Company Website": website,
                "Company Phone": phone,
                "Company General Email": email,
                "Responsible Person Name": "",
                "Responsible Person Role": "",
                "Responsible Person Email": "",
                "Responsible Person Phone": "",

                # tracking/scoring
                "job_type": first_present(job, ["job_type"]),
                "job_status": first_present(job, ["filing_status"]),
                "filed_date": first_present(job, ["filing_date"]),
                "permit_issued_date": first_present(job, ["first_permit_date"]),
                "confidence_score": str(score),
                "prediction_rationale": rationale,
                "target_flag": "TRUE",
                "recipient_id": recipient_id,
                "data_source": "NYC Open Data (DOB NOW Build)",
                "data_confidence_level": "High" if score >= 85 else "Medium",
                "last_verified_date": now,
                "notes": (
                    f"recipient_source={company_source}; "
                    f"api_link={api_link}; "
                    f"job_search_link={job_search_link}"
                ),
            }

            ordered_row = [""] * len(hmap)
            for header, col_index in hmap.items():
                ordered_row[col_index - 1] = values.get(header, "")

            rows_to_append.append(ordered_row)
            existing_ids.add(award_id)

        time.sleep(sleep_seconds)

    if rows_to_append:
        ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        c_appended = len(rows_to_append)
        print(f"✅ Appended {c_appended} rows.")
    else:
        print("No new target rows found (with current thresholds/filters).")

    print("---- NYC PIPELINE DEBUG COUNTS ----")
    print(f"Jobs pulled (2025-filtered): {c_jobs_pulled}")
    print(f"With Award ID: {c_with_award_id}")
    print(f"New (not duplicate): {c_new_not_dupe}")
    print(f"NAICS allowed: {c_naics_allowed}")
    print(f"Score >= min_score ({min_score}): {c_scored_ge_min}")
    print(f"Assigned Q2–Q4 2026 start: {c_assigned_q2026}")
    print(f"Rows appended: {c_appended}")
    print("---- ENRICHMENT (DDG) ----")
    print(f"DDG enrichment attempts: {c_enriched_ddg} (limit per run={ddg_enrich_limit})")
    print(f"Websites found: {c_website_found}")
    print(f"Phones found: {c_phone_found}")
    print(f"Emails found: {c_email_found}")


if __name__ == "__main__":
    main()
