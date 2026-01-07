import os
import io
import json
import time
import csv
import hashlib
import re
from datetime import datetime, timezone, date
from typing import Dict, List, Optional, Tuple, Set

import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


# =============================
# CONFIG
# =============================

CEQANET_URL = "https://ceqanet.lci.ca.gov/"

# Advanced Search date range (dd.mm.yyyy)
DATE_START = os.environ.get("LA_CEQANET_DATE_START", "15.12.2025")
DATE_END = os.environ.get("LA_CEQANET_DATE_END", "31.12.2026")

# Fallback filtering (ISO date)
SINCE_ISO = os.environ.get("LA_CEQANET_SINCE", "2025-12-15")
SINCE_DATE = datetime.strptime(SINCE_ISO, "%Y-%m-%d").date()

DOC_TYPES = ["NOP", "NOE"]

# Google Sheet
SHEET_ID = os.environ.get("LA_SHEET_ID")
TAB_NAME = os.environ.get("LA_TAB_NAME")
CREDS_ENV = "LA_GOOGLE_CREDENTIALS_JSON"

# Controls
MAX_NEW = int(os.environ.get("LA_MAX_NEW", "500"))
SLEEP_SECONDS = float(os.environ.get("LA_SLEEP_SECONDS", "0.10"))

# Detail scraping controls
ENRICH_DETAILS = os.environ.get("LA_ENRICH_DETAILS", "true").lower() == "true"
DETAIL_CAP = int(os.environ.get("LA_DETAIL_CAP", "200"))

# Recent fallback paging controls
MAX_RECENT_PAGES = int(os.environ.get("LA_CEQANET_RECENT_PAGES", "40"))
MAX_DETAIL_VISITS = int(os.environ.get("LA_CEQANET_DETAIL_VISITS", "800"))  # cap for fallback mode

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
    text = download_bytes.decode("utf-8", errors="ignore").lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(text))
    out: List[Dict[str, str]] = []
    for r in reader:
        out.append({normalize_str(k): normalize_str(v) for k, v in (r or {}).items()})
    return out


def stable_award_id(ceqa_id: str, sch: str, title: str, doc_type: str, received: str) -> str:
    key = ceqa_id or sch or f"{title}|{doc_type}|{received}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def looks_like_construction_project(title: str, desc: str, dev_type: str) -> bool:
    blob = f"{title} {desc} {dev_type}".lower()
    keywords = [
        "construction", "build", "building", "renov", "remodel", "tenant improvement",
        "addition", "expansion", "demolition", "grading", "excavation",
        "road", "highway", "bridge", "utility", "pipeline", "facility",
        "warehouse", "hotel", "apartment", "housing", "subdivision",
        "infrastructure", "industrial", "commercial", "residential",
        "repair", "rehab", "reparation", "restoration",
    ]
    return any(k in blob for k in keywords)


def parse_date_flexible(s: str) -> Optional[date]:
    """
    CEQAnet 'Received' formats can vary. Try common patterns.
    Returns a date or None.
    """
    s = normalize_str(s)
    if not s:
        return None

    # normalize separators
    s2 = s.replace("\\", "/").replace("-", "/").replace(".", "/")
    s2 = re.sub(r"\s+", "", s2)

    # try yyyy/mm/dd
    m = re.match(r"^(20\d{2})/(\d{1,2})/(\d{1,2})$", s2)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return date(y, mo, d)

    # try mm/dd/yyyy
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(20\d{2})$", s2)
    if m:
        mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return date(y, mo, d)

    # try dd/mm/yyyy (less common but possible if localized)
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(20\d{2})$", s2)
    if m:
        # ambiguous with mm/dd; keep as mm/dd above already.
        pass

    return None


def ceqa_detail_url_from_row(row: Dict[str, str]) -> str:
    ceqa_id = pick(row, ["CEQA #", "CEQA Number", "CEQAnet ID", "Entry ID", "CEQA ID", "ID"])
    ceqa_id = ceqa_id.replace("-", "").replace(" ", "")
    if ceqa_id.isdigit() and len(ceqa_id) == 10 and ceqa_id.startswith("20"):
        return f"{CEQANET_URL}{ceqa_id}"

    url = pick(row, ["URL", "Link", "Detail Link", "Record Link"])
    if url.startswith("http"):
        return url

    return ""


# =============================
# Debug utilities
# =============================

def _dump_debug(page, prefix: str) -> None:
    try:
        print(f"[DEBUG] url={page.url}")
    except Exception:
        pass
    try:
        print(f"[DEBUG] title={page.title()}")
    except Exception:
        pass
    try:
        html = page.content()
        with open(f"{prefix}.html", "w", encoding="utf-8") as f:
            f.write(html)
        print(f"[DEBUG] saved {prefix}.html")
    except Exception as e:
        print(f"[DEBUG] could not save html: {e}")
    try:
        page.screenshot(path=f"{prefix}.png", full_page=True)
        print(f"[DEBUG] saved {prefix}.png")
    except Exception as e:
        print(f"[DEBUG] could not screenshot: {e}")


# =============================
# Playwright helpers
# =============================

def _click_first(page, selectors: List[str], timeout_ms: int = 10_000) -> bool:
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            loc.wait_for(state="visible", timeout=timeout_ms)
            loc.click(timeout=timeout_ms, force=True)
            return True
        except Exception:
            continue
    return False


def _dismiss_banners(page) -> None:
    candidates = [
        "button:has-text('Accept')",
        "button:has-text('I Accept')",
        "button:has-text('Agree')",
        "button:has-text('I Agree')",
        "button:has-text('OK')",
        "button:has-text('Got it')",
        "text=Accept All",
        "text=I Agree",
        "text=OK",
        "button[aria-label*='accept' i]",
        "button[aria-label*='agree' i]",
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel).first
            if loc.is_visible(timeout=800):
                loc.click(timeout=1500, force=True)
                page.wait_for_timeout(250)
        except Exception:
            pass


def _is_service_unavailable(page) -> bool:
    try:
        t = (page.title() or "").lower()
        if "service unavailable" in t:
            return True
    except Exception:
        pass
    try:
        body = (page.locator("body").inner_text(timeout=2_000) or "").lower()
        if "service unavailable" in body or "503" in body:
            return True
    except Exception:
        pass
    return False


def _is_on_advanced_form(page) -> bool:
    try:
        if page.locator("text=Start Range").first.is_visible(timeout=1200) and \
           page.locator("text=End Range").first.is_visible(timeout=1200) and \
           page.locator("text=Document Type").first.is_visible(timeout=1200):
            return True
    except Exception:
        pass
    try:
        if page.locator("input[placeholder*='dd' i]").count() >= 2:
            return True
    except Exception:
        pass
    return False


def _fill_input_by_row_text(page, row_text: str, value: str, timeout_ms: int = 10_000) -> bool:
    xp = f"xpath=(//*[normalize-space()='{row_text}']/following::input[1])[1]"
    try:
        loc = page.locator(xp).first
        loc.wait_for(state="visible", timeout=timeout_ms)
        loc.fill(value, timeout=timeout_ms)
        return True
    except Exception:
        return False


def _select_by_row_text(page, row_text: str, option_text: str, timeout_ms: int = 10_000) -> bool:
    xp = f"xpath=(//*[normalize-space()='{row_text}']/following::select[1])[1]"
    try:
        sel = page.locator(xp).first
        sel.wait_for(state="visible", timeout=timeout_ms)
        sel.select_option(label=option_text, timeout=timeout_ms)
        return True
    except Exception:
        # fallback by partial match
        try:
            sel = page.locator(xp).first
            options = sel.locator("option")
            cnt = options.count()
            for i in range(cnt):
                txt = normalize_str(options.nth(i).inner_text())
                if option_text.lower() in txt.lower():
                    val = options.nth(i).get_attribute("value")
                    if val:
                        sel.select_option(value=val)
                        return True
        except Exception:
            pass
        return False


def _open_advanced_search(page, timeout_ms: int = 60_000) -> None:
    """
    Attempt to open Advanced Search. If the endpoint is returning "Service unavailable",
    raise a RuntimeError (caller will fallback to /Search/Recent).
    """
    advanced_url_candidates = [
        "https://ceqanet.lci.ca.gov/Search/AdvancedSearch",
        "https://ceqanet.lci.ca.gov/advancedsearch",
        "https://ceqanet.lci.ca.gov/search/advancedsearch",
        "https://ceqanet.lci.ca.gov/Search/Advanced",
        "https://ceqanet.lci.ca.gov/search/advanced",
    ]

    for u in advanced_url_candidates:
        try:
            page.goto(u, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(800)
            _dismiss_banners(page)
            page.wait_for_timeout(300)

            if _is_service_unavailable(page):
                _dump_debug(page, "ceqanet_debug_advanced_service_unavailable")
                raise RuntimeError("Advanced Search is Service unavailable (503).")

            if _is_on_advanced_form(page):
                return
        except RuntimeError:
            raise
        except Exception:
            continue

    _dump_debug(page, "ceqanet_debug_no_advanced")
    raise RuntimeError("Could not open Advanced Search (not found or blocked).")


# =============================
# Detail scraper (CEQA detail page like /2026010057)
# =============================

def _extract_kv_section(section_text: str) -> Dict[str, str]:
    labels = {
        "ceqanet id",
        "lead agency",
        "document title",
        "document type",
        "received",
        "present land use",
        "proposed project",
        "project description",
        "state review period end",
        "public review period end",
    }

    lines = [ln.strip() for ln in section_text.splitlines() if ln.strip()]
    kv: Dict[str, str] = {}
    i = 0
    while i < len(lines):
        k = lines[i].strip().lower()
        if k in labels:
            j = i + 1
            val = ""
            while j < len(lines):
                nxt = lines[j].strip()
                if nxt.lower() in labels:
                    break
                val = nxt
                break
            if val:
                kv[k] = val
            i = j
        else:
            i += 1
    return kv


def scrape_ceqa_detail_in_page(page, detail_url: str, timeout_ms: int = 120_000) -> Dict[str, object]:
    """
    Uses an existing Playwright page (reused) to scrape detail URL.
    """
    out: Dict[str, object] = {"summary": {}, "contacts": [], "location": {}}

    page.goto(detail_url, wait_until="domcontentloaded", timeout=timeout_ms)
    page.wait_for_timeout(700)
    _dismiss_banners(page)

    body_text = page.locator("body").inner_text(timeout=20_000)

    s_idx = body_text.lower().find("summary")
    c_idx = body_text.lower().find("contact information")
    l_idx = body_text.lower().find("\nlocation")

    if s_idx != -1 and c_idx != -1 and c_idx > s_idx:
        summary_text = body_text[s_idx:c_idx]
        out["summary"] = _extract_kv_section(summary_text)

    contacts: List[Dict[str, str]] = []
    if c_idx != -1:
        tail = body_text[c_idx:]
        end = tail.lower().find("\nlocation")
        section = tail[:end] if end != -1 else tail

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
                if key == "name" and current.get("name"):
                    flush()
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
    out["contacts"] = normed

    if l_idx != -1:
        loc_text = body_text[l_idx:]
        loc_fields = [
            "cities", "counties", "regions", "cross streets", "zip",
            "total acres", "parcel(s)", "state highways", "township",
            "range", "section", "base"
        ]
        loc_lines = [ln.strip() for ln in loc_text.splitlines() if ln.strip()]
        loc: Dict[str, str] = {}
        i = 0
        while i < len(loc_lines):
            k = loc_lines[i].strip().lower()
            if k in loc_fields and (i + 1) < len(loc_lines):
                loc[k] = loc_lines[i + 1].strip()
                i += 2
            else:
                i += 1
        out["location"] = loc

    return out


def pick_preferred_contact(contacts: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    if not contacts:
        return None
    for c in contacts:
        ctype = (c.get("contact_types") or "").strip()
        if ctype in PREFERRED_CONTACT_TYPES:
            return c
    return contacts[0]


# =============================
# Advanced Search mode (CSV export)
# =============================

def ceqanet_download_csv_for_range_and_doc_type(
    start_date_ddmmYYYY: str,
    end_date_ddmmYYYY: str,
    doc_type: str,
    timeout_ms: int = 180_000
) -> Tuple[bytes, str]:
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context(
            accept_downloads=True,
            viewport={"width": 1366, "height": 768},
            locale="en-US",
        )
        page = context.new_page()

        _open_advanced_search(page, timeout_ms=timeout_ms)
        page.wait_for_timeout(500)

        ok_start = _fill_input_by_row_text(page, "Start Range", start_date_ddmmYYYY)
        ok_end = _fill_input_by_row_text(page, "End Range", end_date_ddmmYYYY)
        if not ok_start or not ok_end:
            _dump_debug(page, "ceqanet_debug_date_fill_failed")
            raise RuntimeError("Could not fill Start/End Range. Saved ceqanet_debug_date_fill_failed.*")

        ok_doc = _select_by_row_text(page, "Document Type", doc_type)
        if not ok_doc:
            _dump_debug(page, "ceqanet_debug_doc_type_select_failed")
            raise RuntimeError(f"Could not select Document Type={doc_type}. Saved ceqanet_debug_doc_type_select_failed.*")

        clicked = _click_first(page, [
            "button:has-text('Get Results')",
            "input[value='Get Results']",
            "text=Get Results",
        ], timeout_ms=20_000)

        if not clicked:
            _dump_debug(page, "ceqanet_debug_no_get_results")
            raise RuntimeError("Could not click Get Results. Saved ceqanet_debug_no_get_results.*")

        try:
            page.wait_for_selector("text=Download CSV", timeout=60_000)
        except PWTimeoutError:
            _dump_debug(page, "ceqanet_debug_results_no_download")
            raise RuntimeError("Results loaded but no Download CSV found. Saved ceqanet_debug_results_no_download.*")

        try:
            with page.expect_download(timeout=120_000) as dl_info:
                ok_dl = _click_first(page, [
                    "text=Download CSV",
                    "button:has-text('Download CSV')",
                    "a:has-text('Download CSV')",
                    "a[href*='csv' i]",
                ], timeout_ms=20_000)
                if not ok_dl:
                    _dump_debug(page, "ceqanet_debug_no_download_click")
                    raise RuntimeError("Could not click Download CSV. Saved ceqanet_debug_no_download_click.*")

            download = dl_info.value
            csv_bytes = open(download.path(), "rb").read()
        except Exception as e:
            _dump_debug(page, "ceqanet_debug_download_failed")
            raise RuntimeError(f"CSV download failed. Saved ceqanet_debug_download_failed.* Error={e}")

        results_url = page.url
        browser.close()
        return csv_bytes, results_url


# =============================
# Recent fallback mode
# =============================

def collect_detail_urls_from_recent(page, max_pages: int) -> List[str]:
    """
    Collects CEQAnet detail URLs from /Search/Recent by scanning for 10-digit IDs.
    This is intentionally DOM-agnostic.
    """
    detail_urls: List[str] = []
    seen: Set[str] = set()

    page.goto("https://ceqanet.lci.ca.gov/Search/Recent", wait_until="domcontentloaded", timeout=120_000)
    page.wait_for_timeout(1000)
    _dismiss_banners(page)

    for pageno in range(max_pages):
        page.wait_for_timeout(600)

        # Grab all hrefs and pull 10-digit CEQAnet IDs (e.g., 2026010057)
        hrefs = page.locator("a[href]").evaluate_all("els => els.map(e => e.getAttribute('href'))")
        for h in hrefs or []:
            if not h:
                continue
            m = re.search(r"(20\d{8})", h)
            if not m:
                continue
            ceqa_id = m.group(1)
            u = f"{CEQANET_URL}{ceqa_id}"
            if u not in seen:
                seen.add(u)
                detail_urls.append(u)

        # Try to go next page
        next_clicked = _click_first(page, [
            "a:has-text('Next')",
            "button:has-text('Next')",
            "a[aria-label*='Next' i]",
            "button[aria-label*='Next' i]",
        ], timeout_ms=6_000)

        if not next_clicked:
            break

    return detail_urls


# =============================
# Main
# =============================

def main():
    if not SHEET_ID or not TAB_NAME:
        raise RuntimeError("Missing LA_SHEET_ID or LA_TAB_NAME env vars")

    print(f"CEQAnet (CA) | target doc types={DOC_TYPES} | since={SINCE_ISO}")
    print(f"Advanced Search range attempt: {DATE_START} → {DATE_END}")

    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(TAB_NAME)
    hmap = header_map(ws)
    existing_ids = load_existing_ids(ws, id_col=1)

    now = utc_now_str()

    # -------------------------
    # Attempt Advanced Search CSV mode
    # -------------------------
    all_rows: List[Dict[str, str]] = []
    any_results_url = ""

    advanced_ok = True
    try:
        for dt in DOC_TYPES:
            csv_bytes, results_url = ceqanet_download_csv_for_range_and_doc_type(DATE_START, DATE_END, dt)
            any_results_url = results_url or any_results_url
            part = parse_csv_bytes(csv_bytes)
            print(f"Downloaded rows for {dt}: {len(part)}")
            all_rows.extend(part)

        if not all_rows:
            # Treat empty export as suspicious and fallback
            print("⚠️ Advanced Search returned 0 rows; switching to /Search/Recent fallback.")
            advanced_ok = False

    except Exception as e:
        # If advanced search is unavailable or blocked in CI, fallback to Recent
        print(f"⚠️ Advanced Search mode failed: {e}")
        advanced_ok = False

    # -------------------------
    # Data production
    # -------------------------
    rows_to_append: List[List[str]] = []
    appended = 0

    if advanced_ok:
        # Use CSV rows + optional detail enrichment
        detail_used = 0

        for r in all_rows:
            if appended >= MAX_NEW:
                break

            sch = pick(r, ["SCH Number", "SCH", "SCH#", "State Clearinghouse Number"])
            title = pick(r, ["Title", "Project Title", "Project", "Document Title"])
            lead = pick(r, ["Lead/Public Agency", "Lead Agency", "Agency", "Lead Agency Title"])
            received = pick(r, ["Received", "Received Date", "Date Received"])
            doc_type = pick(r, ["Type", "Document Type", "Doc Type"])
            county = pick(r, ["County"])
            city = pick(r, ["City"])
            dev_type = pick(r, ["Development Type", "Dev Type", "Development"])
            location_csv = pick(r, ["Location", "Project Location", "Address"])

            # Filter doc types strictly
            if doc_type and doc_type.strip().upper() not in set(DOC_TYPES):
                continue

            detail_url = ceqa_detail_url_from_row(r)
            ceqa_id = ""
            if detail_url.startswith(CEQANET_URL):
                ceqa_id = detail_url.replace(CEQANET_URL, "").strip("/")

            award_id = stable_award_id(ceqa_id=ceqa_id, sch=sch, title=title, doc_type=doc_type, received=received)
            if award_id in existing_ids:
                continue

            # If we don't have a detail URL from CSV, we can still proceed but will have less data
            detail_summary: Dict[str, str] = {}
            detail_contacts: List[Dict[str, str]] = []
            detail_location: Dict[str, str] = {}

            # Enrich with detail page (best effort)
            if ENRICH_DETAILS and detail_url and detail_used < DETAIL_CAP:
                try:
                    with sync_playwright() as p:
                        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
                        context = browser.new_context(viewport={"width": 1366, "height": 768}, locale="en-US")
                        page = context.new_page()
                        d = scrape_ceqa_detail_in_page(page, detail_url, timeout_ms=150_000)
                        detail_summary = (d.get("summary") or {})  # type: ignore
                        detail_contacts = (d.get("contacts") or [])  # type: ignore
                        detail_location = (d.get("location") or {})  # type: ignore
                        browser.close()
                    detail_used += 1
                except Exception as e:
                    print(f"[DETAIL] Failed for URL={detail_url}: {e}")

            # Filter by received date since 2025-12-15 when possible (detail > csv)
            submitted = (detail_summary.get("received") or received or "").strip()
            submitted_dt = parse_date_flexible(submitted)
            if submitted_dt and submitted_dt < SINCE_DATE:
                continue

            chosen = pick_preferred_contact(detail_contacts)

            present_land_use = (detail_summary.get("present land use") or "").strip()
            proposed = (detail_summary.get("proposed project") or "").strip()
            proj_desc = (detail_summary.get("project description") or "").strip()
            description_blob = " | ".join([x for x in [present_land_use, proj_desc, proposed] if x]) or title

            stage = (detail_summary.get("document type") or doc_type or "").strip()
            state_review_end = (detail_summary.get("state review period end") or "").strip()
            public_review_end = (detail_summary.get("public review period end") or "").strip()

            loc_city = (detail_location.get("cities") or city or "").strip()
            loc_county = (detail_location.get("counties") or county or "").strip()
            place_of_perf = ", ".join([x for x in [loc_city, loc_county, "CA"] if x])

            recipient_company = (chosen.get("agency_name", "") if chosen else "")
            is_constructionish = looks_like_construction_project(title=title, desc=description_blob, dev_type=dev_type)
            contacts_json = json.dumps(detail_contacts, ensure_ascii=False)

            award_link = detail_url or (any_results_url if any_results_url else f"SCH={sch}")

            values = {
                "Award ID": award_id,
                "Recipient (Company)": recipient_company,
                "Recipient UEI": "",
                "Parent Recipient UEI": "",
                "Parent Recipient DUNS": "",
                "Recipient (HQ) Address": (chosen.get("address", "") if chosen else "") or location_csv,
                "Start Date": "2026-01-01",
                "End Date": "",
                "Last Modified Date": now,
                "Award Amount (Obligated)": "",
                "NAICS Code": "",
                "NAICS Description": "",
                "Awarding Agency": lead,
                "Place of Performance": place_of_perf,
                "Description": (
                    f"Stage={stage}"
                    f" | Submitted={submitted}"
                    f" | ReviewEnd(State)={state_review_end}"
                    f" | ReviewEnd(Public)={public_review_end}"
                    f" | {description_blob}"
                ).strip(),
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
                "confidence_score": "70" if detail_contacts else "55",
                "prediction_rationale": "ceqanet_adv_search(+55); detail_page(+15)" if detail_contacts else "ceqanet_adv_search(+55)",
                "target_flag": "TRUE",
                "recipient_id": award_id,
                "data_source": "CEQAnet (CA State Clearinghouse)",
                "data_confidence_level": "Medium",
                "last_verified_date": now,
                "notes": (
                    f"SINCE={SINCE_ISO}; SCH={sch}; DocType={stage}; Submitted={submitted}; "
                    f"DevType={dev_type}; construction_hint={is_constructionish}; "
                    f"DetailLoc={json.dumps(detail_location, ensure_ascii=False)}; "
                    f"Contacts={contacts_json}"
                ),
            }

            ordered = [""] * len(hmap)
            for header, col_index in hmap.items():
                ordered[col_index - 1] = values.get(header, "")

            rows_to_append.append(ordered)
            existing_ids.add(award_id)
            appended += 1
            time.sleep(SLEEP_SECONDS)

        print(f"Advanced Search mode prepared rows: {len(rows_to_append)}")

    else:
        # -------------------------
        # Recent fallback mode
        # -------------------------
        print("➡️ Using /Search/Recent fallback (Advanced Search unavailable in CI).")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            context = browser.new_context(viewport={"width": 1366, "height": 768}, locale="en-US")
            page = context.new_page()

            detail_urls = collect_detail_urls_from_recent(page, max_pages=MAX_RECENT_PAGES)
            print(f"Collected detail URLs from Recent: {len(detail_urls)} (pages scanned={MAX_RECENT_PAGES})")

            # Mid-run self-check: if Recent yields nothing, dump debug and fail
            if not detail_urls:
                _dump_debug(page, "ceqanet_debug_recent_empty")
                raise RuntimeError("Recent fallback yielded 0 detail URLs. Saved ceqanet_debug_recent_empty.*")

            visited = 0
            detail_used = 0

            for u in detail_urls:
                if appended >= MAX_NEW:
                    break
                if visited >= MAX_DETAIL_VISITS:
                    break

                visited += 1

                try:
                    d = scrape_ceqa_detail_in_page(page, u, timeout_ms=150_000)
                except Exception as e:
                    print(f"[DETAIL] Failed {u}: {e}")
                    continue

                detail_summary = (d.get("summary") or {})  # type: ignore
                detail_contacts = (d.get("contacts") or [])  # type: ignore
                detail_location = (d.get("location") or {})  # type: ignore
                detail_used += 1

                stage = (detail_summary.get("document type") or "").strip().upper()
                submitted = (detail_summary.get("received") or "").strip()
                title = (detail_summary.get("document title") or "").strip()
                lead = (detail_summary.get("lead agency") or "").strip()

                # Fallback filters (these are your actual requirements)
                if stage and stage not in set(DOC_TYPES):
                    continue

                submitted_dt = parse_date_flexible(submitted)
                if submitted_dt and submitted_dt < SINCE_DATE:
                    continue
                # If we cannot parse date, keep it (better to include than miss), but mark in notes.

                chosen = pick_preferred_contact(detail_contacts)

                present_land_use = (detail_summary.get("present land use") or "").strip()
                proposed = (detail_summary.get("proposed project") or "").strip()
                proj_desc = (detail_summary.get("project description") or "").strip()
                description_blob = " | ".join([x for x in [present_land_use, proj_desc, proposed] if x]) or title

                loc_city = (detail_location.get("cities") or "").strip()
                loc_county = (detail_location.get("counties") or "").strip()
                place_of_perf = ", ".join([x for x in [loc_city, loc_county, "CA"] if x])

                ceqa_id = u.replace(CEQANET_URL, "").strip("/")

                award_id = stable_award_id(ceqa_id=ceqa_id, sch="", title=title, doc_type=stage, received=submitted)
                if award_id in existing_ids:
                    continue

                is_constructionish = looks_like_construction_project(title=title, desc=description_blob, dev_type="")
                contacts_json = json.dumps(detail_contacts, ensure_ascii=False)

                values = {
                    "Award ID": award_id,
                    "Recipient (Company)": (chosen.get("agency_name", "") if chosen else ""),
                    "Recipient UEI": "",
                    "Parent Recipient UEI": "",
                    "Parent Recipient DUNS": "",
                    "Recipient (HQ) Address": (chosen.get("address", "") if chosen else ""),
                    "Start Date": "2026-01-01",
                    "End Date": "",
                    "Last Modified Date": now,
                    "Award Amount (Obligated)": "",
                    "NAICS Code": "",
                    "NAICS Description": "",
                    "Awarding Agency": lead,
                    "Place of Performance": place_of_perf,
                    "Description": (
                        f"Stage={stage or 'UNKNOWN'}"
                        f" | Submitted={submitted or 'UNKNOWN'}"
                        f" | {description_blob}"
                    ).strip(),
                    "Award Link": u,
                    "Recipient Profile Link": "",
                    "Web Search Link": "",
                    "Company Website": "",
                    "Company Phone": (chosen.get("phone", "") if chosen else ""),
                    "Company General Email": (chosen.get("email", "") if chosen else ""),
                    "Responsible Person Name": (chosen.get("name", "") if chosen else ""),
                    "Responsible Person Role": (chosen.get("job_title", "") if chosen else ""),
                    "Responsible Person Email": (chosen.get("email", "") if chosen else ""),
                    "Responsible Person Phone": (chosen.get("phone", "") if chosen else ""),
                    "confidence_score": "75",
                    "prediction_rationale": "ceqanet_recent(+50); detail_page(+25)",
                    "target_flag": "TRUE",
                    "recipient_id": award_id,
                    "data_source": "CEQAnet (CA State Clearinghouse) - Recent fallback",
                    "data_confidence_level": "Medium",
                    "last_verified_date": now,
                    "notes": (
                        f"SINCE={SINCE_ISO}; date_parsed={submitted_dt.isoformat() if submitted_dt else 'NO'}; "
                        f"construction_hint={is_constructionish}; "
                        f"DetailLoc={json.dumps(detail_location, ensure_ascii=False)}; "
                        f"Contacts={contacts_json}"
                    ),
                }

                ordered = [""] * len(hmap)
                for header, col_index in hmap.items():
                    ordered[col_index - 1] = values.get(header, "")

                rows_to_append.append(ordered)
                existing_ids.add(award_id)
                appended += 1
                time.sleep(SLEEP_SECONDS)

            browser.close()

        print(f"Recent fallback mode prepared rows: {len(rows_to_append)}")

    # -------------------------
    # Write to Google Sheet
    # -------------------------
    if rows_to_append:
        ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        print(f"✅ Appended {len(rows_to_append)} rows into {TAB_NAME}.")
    else:
        print("No new rows appended (deduped / filtered / empty).")

    print("Done.")


if __name__ == "__main__":
    main()
