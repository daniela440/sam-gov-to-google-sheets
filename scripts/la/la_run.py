import os
import io
import json
import time
import hashlib
import csv
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


# =============================
# CONFIG
# =============================

CEQANET_URL = "https://ceqanet.lci.ca.gov/"

# CEQAnet UI in your screenshots expects dd.mm.yyyy (with dots)
DATE_START = os.environ.get("LA_CEQANET_DATE_START", "01.01.2026")
DATE_END   = os.environ.get("LA_CEQANET_DATE_END",   "31.12.2026")

# Google Sheet (reuse your existing env vars)
SHEET_ID = os.environ.get("LA_SHEET_ID")
TAB_NAME = os.environ.get("LA_TAB_NAME")
CREDS_ENV = "LA_GOOGLE_CREDENTIALS_JSON"

# Controls
MAX_NEW = int(os.environ.get("LA_MAX_NEW", "500"))
SLEEP_SECONDS = float(os.environ.get("LA_SLEEP_SECONDS", "0.10"))

# Detail scraping controls (this is what gets you “real” non-government contacts)
ENRICH_DETAILS = os.environ.get("LA_ENRICH_DETAILS", "true").lower() == "true"
DETAIL_CAP = int(os.environ.get("LA_DETAIL_CAP", "150"))  # scraping details is slower

# Only keep contacts likely to be “private side” (what you want)
# (You can expand later.)
PREFERRED_CONTACT_TYPES = {
    "Project Applicant",
    "Consulting Firm",
    "Applicant",
    "Consultant",
    "Developer",
    "Owner",
    "Engineer",
    "Architect",
    "Contractor",
}

# =============================
# Helpers
# =============================

def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def normalize_str(x) -> str:
    return str(x).strip() if x is not None else ""


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


def header_map(ws) -> dict:
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("Row 1 headers are empty. Paste your column headers in row 1.")
    return {h: i + 1 for i, h in enumerate(headers)}  # 1-based


def load_existing_ids(ws, id_col: int = 1) -> set:
    col_values = ws.col_values(id_col)
    return {v.strip() for v in col_values[1:] if v and v.strip()}


def pick(row: Dict[str, str], candidates: List[str]) -> str:
    lower_map = {k.lower(): k for k in row.keys()}
    for c in candidates:
        k = lower_map.get(c.lower())
        if k:
            v = normalize_str(row.get(k))
            if v:
                return v
    return ""


def parse_csv_bytes(download_bytes: bytes) -> List[Dict[str, str]]:
    text = download_bytes.decode("utf-8", errors="ignore")
    text = text.lstrip("\ufeff")  # strip BOM if present
    reader = csv.DictReader(io.StringIO(text))
    out: List[Dict[str, str]] = []
    for r in reader:
        out.append({normalize_str(k): normalize_str(v) for k, v in (r or {}).items()})
    return out


def stable_award_id(sch: str, title: str, lead: str, received: str, county: str, city: str) -> str:
    key = sch or f"{title}|{lead}|{received}|{county}|{city}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def looks_like_construction_project(title: str, desc: str, dev_type: str) -> bool:
    """
    You said “ALL construction of any type”. CEQAnet is already project-level.
    We keep everything and optionally flag likely construction-related by keywords.
    """
    blob = f"{title} {desc} {dev_type}".lower()
    keywords = [
        "construction", "build", "building", "renov", "remodel", "tenant improvement",
        "addition", "expansion", "demolition", "grading", "excavation", "bridge",
        "road", "highway", "pipeline", "facility", "warehouse", "hotel", "apartment",
        "housing", "subdivision", "plant", "solar", "wind", "station", "infrastructure",
        "industrial", "commercial", "residential", "transportation"
    ]
    return any(k in blob for k in keywords)


# =============================
# CEQAnet automation (Playwright)
# =============================

def _click_first(page, selectors: List[str], timeout_ms: int = 10_000) -> bool:
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            loc.wait_for(state="visible", timeout=timeout_ms)
            loc.click(timeout=timeout_ms)
            return True
        except Exception:
            continue
    return False


def _fill_date_field(page, label_candidates: List[str], value: str) -> bool:
    # Try label-based
    for lab in label_candidates:
        try:
            page.get_by_label(lab, exact=False).fill(value, timeout=5_000)
            return True
        except Exception:
            pass
    return False


def ceqanet_download_csv_for_2026(timeout_ms: int = 180_000) -> Tuple[bytes, str]:
    """
    Runs Advanced Search for the date range and downloads the CSV.
    Returns (csv_bytes, results_page_url).

    FIXES:
      - Do not depend on clicking "Advanced Search" from homepage.
      - Try direct Advanced Search URLs first.
      - Fall back to robust click strategies.
      - Verify we are on the Advanced Search form by waiting for "Get Results".
    """
    advanced_url_candidates = [
        # Common ASP.NET MVC-ish patterns (CEQAnet has used these variants historically)
        "https://ceqanet.lci.ca.gov/Search/AdvancedSearch",
        "https://ceqanet.lci.ca.gov/Search/Advanced",
        "https://ceqanet.lci.ca.gov/AdvancedSearch",
        "https://ceqanet.lci.ca.gov/search/advancedsearch",
        "https://ceqanet.lci.ca.gov/search/advanced",
    ]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # Step 1: land on site
        page.goto(CEQANET_URL, wait_until="domcontentloaded", timeout=timeout_ms)
        page.wait_for_timeout(800)

        # Step 2: try direct Advanced Search URLs
        on_advanced = False
        for u in advanced_url_candidates:
            try:
                page.goto(u, wait_until="domcontentloaded", timeout=45_000)
                # Confirm by presence of the "Get Results" button or Start Range label
                if page.locator("text=Get Results").first.is_visible(timeout=3_000) or \
                   page.locator("text=Start Range").first.is_visible(timeout=3_000):
                    on_advanced = True
                    break
            except Exception:
                continue

        # Step 3: click fallback if direct URLs failed
        if not on_advanced:
            # Go back to home and try clicking from nav/header in multiple ways
            page.goto(CEQANET_URL, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(1200)

            # These match your screenshots: top nav contains “Advanced Search”
            clicked = False

            # Role-based click is often the most reliable
            try:
                page.get_by_role("link", name="Advanced Search", exact=False).click(timeout=8_000)
                clicked = True
            except Exception:
                pass

            if not clicked:
                clicked = _click_first(page, [
                    "a:has-text('Advanced Search')",
                    "text=Advanced Search",
                    "a[href*='Advanced' i]",
                    "a[href*='advanced' i]",
                    "a:has-text('Advanced')",
                    "text=Advanced",
                ], timeout_ms=10_000)

            if not clicked:
                page.screenshot(path="ceqanet_debug_no_advanced.png", full_page=True)
                raise RuntimeError(
                    "Could not open Advanced Search. Saved ceqanet_debug_no_advanced.png"
                )

        # Step 4: verify Advanced Search form is present
        try:
            page.wait_for_selector("text=Get Results", timeout=30_000)
        except PWTimeoutError:
            page.screenshot(path="ceqanet_debug_not_on_advanced.png", full_page=True)
            raise RuntimeError(
                "Navigation did not land on Advanced Search form. Saved ceqanet_debug_not_on_advanced.png"
            )

        page.wait_for_timeout(500)

        # Step 5: Fill date range (dd.mm.yyyy)
        ok_start = _fill_date_field(page, ["Start Range", "Start", "From", "Begin"], DATE_START)
        ok_end   = _fill_date_field(page, ["End Range", "End", "To"], DATE_END)

        # Fallback: fill first two visible text inputs
        if not (ok_start and ok_end):
            inputs = page.locator("input[type='text']:visible")
            if inputs.count() >= 2:
                inputs.nth(0).fill(DATE_START)
                inputs.nth(1).fill(DATE_END)

        # IMPORTANT: We want ALL construction-ish work; we do NOT filter Development Type here.
        # Leave County/City as (Any) for statewide.

        # Step 6: Click "Get Results"
        clicked = _click_first(page, [
            "button:has-text('Get Results')",
            "input[value='Get Results']",
            "text=Get Results",
        ], timeout_ms=12_000)
        if not clicked:
            page.screenshot(path="ceqanet_debug_no_get_results.png", full_page=True)
            raise RuntimeError("Could not click Get Results. Saved ceqanet_debug_no_get_results.png")

        # Step 7: Wait for results page
        page.wait_for_timeout(1500)

        # Step 8: Download CSV
        try:
            page.wait_for_selector("text=Download CSV", timeout=30_000)
        except PWTimeoutError:
            page.screenshot(path="ceqanet_debug_results_no_download.png", full_page=True)
            raise RuntimeError(
                "Results loaded, but Download CSV button not found. Saved ceqanet_debug_results_no_download.png"
            )

        download = None
        try:
            with page.expect_download(timeout=60_000) as dl_info:
                ok_dl = _click_first(page, [
                    "text=Download CSV",
                    "button:has-text('Download CSV')",
                    "a:has-text('Download CSV')",
                    "a[href*='csv' i]",
                ], timeout_ms=12_000)
                if not ok_dl:
                    page.screenshot(path="ceqanet_debug_no_download_csv.png", full_page=True)
                    raise RuntimeError("Could not click Download CSV on results page.")
            download = dl_info.value
        except Exception as e:
            page.screenshot(path="ceqanet_debug_download_failed.png", full_page=True)
            raise RuntimeError(f"CSV download failed. Saved ceqanet_debug_download_failed.png. Error={e}")

        csv_bytes = download.path().read_bytes()  # type: ignore
        results_url = page.url

        browser.close()
        return csv_bytes, results_url


def ceqanet_scrape_contacts_from_sch(timeout_ms: int, sch_number: str) -> List[Dict[str, str]]:
    """
    Scrapes non-government-ish contacts from the SCH detail page by:
      - running an SCH search on the site (Find by SCH Number)
      - opening the detail page
      - extracting Contact Information section text and parsing into structured fields
    """
    contacts: List[Dict[str, str]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        page.goto(CEQANET_URL, wait_until="domcontentloaded", timeout=timeout_ms)

        # Use global search / direct SCH finder workflow
        # We try several paths because CEQAnet navigation changes.
        _click_first(page, [
            "text=Search",
            "a:has-text('Search')",
            "text=Advanced Search",
        ])

        page.wait_for_timeout(500)

        # Many CEQAnet pages have "Find by SCH Number" button on search page
        ok_find = _click_first(page, [
            "text=Find by SCH Number",
            "button:has-text('Find by SCH Number')",
            "a:has-text('Find by SCH Number')",
        ], timeout_ms=5_000)

        if ok_find:
            page.wait_for_timeout(600)

        # Try to locate an SCH input box
        sch_filled = False
        for sel in [
            "input[aria-label*='SCH' i]",
            "input[name*='SCH' i]",
            "input[id*='SCH' i]",
            "input[type='text']:visible",
        ]:
            try:
                loc = page.locator(sel).first
                loc.fill(sch_number, timeout=5_000)
                sch_filled = True
                break
            except Exception:
                continue

        if not sch_filled:
            browser.close()
            return contacts

        # Submit / search
        clicked = _click_first(page, [
            "button:has-text('Search')",
            "button:has-text('Get Results')",
            "input[value='Search']",
            "text=Search",
        ], timeout_ms=8_000)
        if not clicked:
            browser.close()
            return contacts

        # Click SCH number in results
        try:
            page.wait_for_timeout(1200)
            page.locator(f"text={sch_number}").first.click(timeout=10_000)
        except Exception:
            browser.close()
            return contacts

        page.wait_for_timeout(1200)

        # Extract text between "Contact Information" and "Location"
        body_text = page.locator("body").inner_text(timeout=10_000)

        start_idx = body_text.lower().find("contact information")
        if start_idx == -1:
            browser.close()
            return contacts

        tail = body_text[start_idx:]
        end_idx = tail.lower().find("\nlocation")
        if end_idx != -1:
            section = tail[:end_idx]
        else:
            section = tail

        # Parse into contacts by scanning label/value pairs
        labels = {
            "name",
            "agency name",
            "job title",
            "contact types",
            "address",
            "phone",
            "email",
        }

        lines = [ln.strip() for ln in section.splitlines() if ln.strip()]
        current: Dict[str, str] = {}

        def flush():
            nonlocal current
            if current:
                contacts.append(current)
                current = {}

        i = 0
        while i < len(lines):
            key = lines[i].strip().lower()
            if key in labels:
                # new contact detection: if we see "Name" again and current already has a name, flush
                if key == "name" and current.get("name"):
                    flush()

                # value = next non-label line
                j = i + 1
                val = ""
                while j < len(lines):
                    nxt = lines[j].strip()
                    if nxt.lower() in labels:
                        break
                    val = nxt
                    break
                if val:
                    current[key] = val
                i = j
            else:
                i += 1

        flush()

        browser.close()

    # Normalize keys
    normed: List[Dict[str, str]] = []
    for c in contacts:
        normed.append({
            "name": c.get("name", ""),
            "agency_name": c.get("agency name", ""),
            "job_title": c.get("job title", ""),
            "contact_types": c.get("contact types", ""),
            "address": c.get("address", ""),
            "phone": c.get("phone", ""),
            "email": c.get("email", ""),
        })

    # Prefer non-government-ish contacts
    filtered: List[Dict[str, str]] = []
    for c in normed:
        ctype = (c.get("contact_types") or "").strip()
        # Skip lead/public agency employees unless there is no alternative
        if ctype.lower() in {"lead/public agency", "lead public agency"}:
            continue
        filtered.append(c)

    # If filtering removed everything, return raw normed
    return filtered or normed


# =============================
# Main
# =============================

def main():
    if not SHEET_ID or not TAB_NAME:
        raise RuntimeError("Missing LA_SHEET_ID or LA_TAB_NAME env vars")

    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(TAB_NAME)

    hmap = header_map(ws)
    existing_ids = load_existing_ids(ws, id_col=1)

    print(f"CEQAnet (California) | pulling records for date range {DATE_START} → {DATE_END} ...")
    csv_bytes, results_url = ceqanet_download_csv_for_2026()
    rows = parse_csv_bytes(csv_bytes)

    print(f"Downloaded + parsed rows: {len(rows)}")
    if not rows:
        print("No rows parsed. Export format may have changed or returned empty.")
        return

    now = utc_now_str()
    rows_to_append: List[List[str]] = []
    appended = 0
    detail_used = 0

    for r in rows:
        if appended >= MAX_NEW:
            break

        # Common CEQAnet CSV columns seen on results page exports:
        sch = pick(r, ["SCH Number", "SCH", "SCH#", "State Clearinghouse Number"])
        title = pick(r, ["Title", "Project Title", "Project"])
        lead = pick(r, ["Lead/Public Agency", "Lead Agency", "Agency"])
        received = pick(r, ["Received", "Received Date", "Date Received"])
        doc_type = pick(r, ["Type", "Document Type", "Doc Type"])
        county = pick(r, ["County"])
        city = pick(r, ["City"])
        dev_type = pick(r, ["Development Type", "Dev Type", "Development"])
        # Some exports include “Location”, others do not.
        location = pick(r, ["Location", "Project Location", "Address"])

        award_id = stable_award_id(
            sch=sch,
            title=title,
            lead=lead,
            received=received,
            county=county,
            city=city,
        )

        if award_id in existing_ids:
            continue

        # Optional detail scrape for Applicant/Consulting Firm contacts
        contacts: List[Dict[str, str]] = []
        if ENRICH_DETAILS and sch and detail_used < DETAIL_CAP:
            try:
                contacts = ceqanet_scrape_contacts_from_sch(timeout_ms=150_000, sch_number=sch)
                detail_used += 1
            except Exception as e:
                print(f"[DETAIL] Failed for SCH={sch}: {e}")

        # Choose a “best” private-side contact
        chosen = None
        if contacts:
            # Prefer a contact whose Contact Types matches our preferred set
            for c in contacts:
                ctype = (c.get("contact_types") or "").strip()
                if ctype in PREFERRED_CONTACT_TYPES:
                    chosen = c
                    break
            # fallback: first contact
            if not chosen:
                chosen = contacts[0]

        # For your sheet schema: “Recipient (Company)” should be a real org to contact.
        # Best proxy from CEQAnet is usually Applicant or Consulting Firm agency name.
        recipient_company = ""
        if chosen:
            recipient_company = chosen.get("agency_name", "")

        # Also keep a flag for likely construction; you said “all”, but this helps later segmentation.
        is_constructionish = looks_like_construction_project(title=title, desc="", dev_type=dev_type)

        # Put SCH detail page into “Award Link” as your primary reference (best we can do reliably)
        # We do not have a stable URL pattern here without live probing, so we store the results URL + SCH.
        award_link = f"{results_url} (SCH={sch})" if results_url else f"SCH={sch}"

        # Pack all contacts into notes (so you can parse later if needed)
        contacts_json = json.dumps(contacts, ensure_ascii=False)

        values = {
            "Award ID": award_id,
            "Recipient (Company)": recipient_company,
            "Recipient UEI": "",
            "Parent Recipient UEI": "",
            "Parent Recipient DUNS": "",
            "Recipient (HQ) Address": location or (chosen.get("address", "") if chosen else ""),
            "Start Date": "2026-01-01",  # marker for your 2026 pipeline
            "End Date": "",
            "Last Modified Date": now,
            "Award Amount (Obligated)": "",
            "NAICS Code": "",              # CEQAnet does not provide NAICS
            "NAICS Description": "",       # CEQAnet does not provide NAICS
            "Awarding Agency": lead,
            "Place of Performance": ", ".join([x for x in [city, county, "CA"] if x]),
            "Description": f"{title} | {doc_type} | DevType={dev_type} | Received={received}".strip(),
            "Award Link": award_link,
            "Recipient Profile Link": "",
            "Web Search Link": "",

            "Company Website": "",
            "Company Phone": (chosen.get("phone", "") if chosen else ""),
            "Company General Email": (chosen.get("email", "") if chosen else ""),
            "Responsible Person Name": (chosen.get("name", "") if chosen else ""),
            "Responsible Person Role": (chosen.get("job_title", "") if chosen else ""),
            "Responsible Person Email": (chosen.get("email", "") if chosen else ""),
            "Responsible Person Phone": (chosen.get("phone", "") if chosen else ""),

            "confidence_score": "65" if contacts else "55",
            "prediction_rationale": "ceqanet_search(+55); detail_contacts(+10)" if contacts else "ceqanet_search(+55)",
            "target_flag": "TRUE",
            "recipient_id": award_id,
            "data_source": "CEQAnet (CA State Clearinghouse)",
            "data_confidence_level": "Medium",
            "last_verified_date": now,
            "notes": f"SCH={sch}; County={county}; City={city}; construction_hint={is_constructionish}; contacts={contacts_json}",
        }

        ordered = [""] * len(hmap)
        for header, col_index in hmap.items():
            ordered[col_index - 1] = values.get(header, "")

        rows_to_append.append(ordered)
        existing_ids.add(award_id)
        appended += 1
        time.sleep(SLEEP_SECONDS)

    if rows_to_append:
        ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        print(f"✅ Appended {len(rows_to_append)} rows into {TAB_NAME}.")
    else:
        print("No new rows appended (deduped or empty export).")

    print(f"Detail pages scraped: {detail_used} (cap={DETAIL_CAP})")
    print("Done.")


if __name__ == "__main__":
    main()
