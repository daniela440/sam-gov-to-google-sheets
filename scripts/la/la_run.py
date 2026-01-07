import os
import io
import json
import time
import csv
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


# =============================
# CONFIG
# =============================

CEQANET_URL = "https://ceqanet.lci.ca.gov/"

# CEQAnet date format in UI: dd.mm.yyyy
DATE_START = os.environ.get("LA_CEQANET_DATE_START", "15.12.2025")
DATE_END   = os.environ.get("LA_CEQANET_DATE_END",   "31.12.2026")

# Document types you requested
DOC_TYPES = ["NOP", "NOE"]

# Google Sheet (reuse your existing env vars)
SHEET_ID = os.environ.get("LA_SHEET_ID")
TAB_NAME = os.environ.get("LA_TAB_NAME")
CREDS_ENV = "LA_GOOGLE_CREDENTIALS_JSON"

# Controls
MAX_NEW = int(os.environ.get("LA_MAX_NEW", "500"))
SLEEP_SECONDS = float(os.environ.get("LA_SLEEP_SECONDS", "0.10"))

# Detail scraping controls
ENRICH_DETAILS = os.environ.get("LA_ENRICH_DETAILS", "true").lower() == "true"
DETAIL_CAP = int(os.environ.get("LA_DETAIL_CAP", "200"))  # details are slower

# Prefer these contact types for your “responsible people”
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
    # Prefer CEQA ID if present (e.g., 2026010057)
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


def ceqa_detail_url_from_row(row: Dict[str, str]) -> str:
    """
    Try to build a detail URL like https://ceqanet.lci.ca.gov/2026010057
    based on whatever CEQAnet exports in CSV.
    """
    # Common possibilities in exports:
    # - "CEQA #" or "CEQA Number" or "CEQAnet ID" or "Entry ID"
    ceqa_id = pick(row, ["CEQA #", "CEQA Number", "CEQAnet ID", "Entry ID", "CEQA ID", "ID"])
    ceqa_id = ceqa_id.replace("-", "").replace(" ", "")
    if ceqa_id.isdigit() and len(ceqa_id) == 10 and ceqa_id.startswith("20"):
        return f"{CEQANET_URL}{ceqa_id}"

    # Some exports include direct URL
    url = pick(row, ["URL", "Link", "Detail Link", "Record Link"])
    if url.startswith("http"):
        return url

    return ""


# =============================
# Playwright: robust field helpers (match your screenshots)
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


def _fill_input_by_row_text(page, row_text: str, value: str, timeout_ms: int = 10_000) -> bool:
    """
    Finds a row by its left-hand text (e.g., 'Start Range') and fills the first input to the right.
    CEQAnet advanced search UI is typically a two-column layout.
    """
    xp = (
        f"xpath=(//*[normalize-space()='{row_text}']"
        f"/following::input[1])[1]"
    )
    try:
        loc = page.locator(xp).first
        loc.wait_for(state="visible", timeout=timeout_ms)
        loc.fill(value, timeout=timeout_ms)
        return True
    except Exception:
        return False


def _select_by_row_text(page, row_text: str, option_text: str, timeout_ms: int = 10_000) -> bool:
    """
    Selects an option in the first <select> found to the right of the row label text.
    """
    xp = (
        f"xpath=(//*[normalize-space()='{row_text}']"
        f"/following::select[1])[1]"
    )
    try:
        sel = page.locator(xp).first
        sel.wait_for(state="visible", timeout=timeout_ms)
        sel.select_option(label=option_text, timeout=timeout_ms)
        return True
    except Exception:
        # fallback: try selecting by partial match via evaluating options
        try:
            sel = page.locator(xp).first
            options = sel.locator("option")
            cnt = options.count()
            target_val = None
            for i in range(cnt):
                txt = normalize_str(options.nth(i).inner_text())
                if option_text.lower() in txt.lower():
                    target_val = options.nth(i).get_attribute("value")
                    break
            if target_val is not None:
                sel.select_option(value=target_val)
                return True
        except Exception:
            pass
        return False


def _open_advanced_search(page, timeout_ms: int = 60_000) -> None:
    page.goto(CEQANET_URL, wait_until="domcontentloaded", timeout=timeout_ms)
    page.wait_for_timeout(1200)

    # Try clicking the top nav “Advanced Search” (shown in your screenshot)
    clicked = False
    try:
        page.get_by_role("link", name="Advanced Search", exact=False).click(timeout=8_000, force=True)
        clicked = True
    except Exception:
        pass

    if not clicked:
        clicked = _click_first(page, [
            "a:has-text('Advanced Search')",
            "a[href*='advanced' i]",
            "text=Advanced Search",
        ], timeout_ms=12_000)

    if not clicked:
        page.screenshot(path="ceqanet_debug_no_advanced.png", full_page=True)
        raise RuntimeError("Could not open Advanced Search. Saved ceqanet_debug_no_advanced.png")

    # Verify the form loaded (presence of Start Range / Get Results)
    try:
        page.wait_for_selector("text=Start Range", timeout=20_000)
        page.wait_for_selector("text=Get Results", timeout=20_000)
    except PWTimeoutError:
        page.screenshot(path="ceqanet_debug_not_on_advanced.png", full_page=True)
        raise RuntimeError("Not on Advanced Search form. Saved ceqanet_debug_not_on_advanced.png")


def ceqanet_download_csv_for_range_and_doc_type(
    start_date_ddmmYYYY: str,
    end_date_ddmmYYYY: str,
    doc_type: str,
    timeout_ms: int = 180_000
) -> Tuple[bytes, str]:
    """
    Runs Advanced Search for date range + doc type, downloads CSV.
    Returns (csv_bytes, results_page_url).
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        _open_advanced_search(page, timeout_ms=timeout_ms)
        page.wait_for_timeout(600)

        # Fill dates using row text (matches screenshot labels exactly)
        ok_start = _fill_input_by_row_text(page, "Start Range", start_date_ddmmYYYY)
        ok_end   = _fill_input_by_row_text(page, "End Range", end_date_ddmmYYYY)

        if not ok_start or not ok_end:
            page.screenshot(path="ceqanet_debug_date_fill_failed.png", full_page=True)
            raise RuntimeError("Could not fill Start/End Range. Saved ceqanet_debug_date_fill_failed.png")

        # Document Type dropdown
        ok_doc = _select_by_row_text(page, "Document Type", doc_type)
        if not ok_doc:
            page.screenshot(path="ceqanet_debug_doc_type_select_failed.png", full_page=True)
            raise RuntimeError(f"Could not select Document Type={doc_type}. Saved ceqanet_debug_doc_type_select_failed.png")

        # Click Get Results (there are two buttons; click the first visible)
        clicked = _click_first(page, [
            "button:has-text('Get Results')",
            "input[value='Get Results']",
            "text=Get Results",
        ], timeout_ms=15_000)

        if not clicked:
            page.screenshot(path="ceqanet_debug_no_get_results.png", full_page=True)
            raise RuntimeError("Could not click Get Results. Saved ceqanet_debug_no_get_results.png")

        # Wait for results page to show download control
        try:
            page.wait_for_selector("text=Download CSV", timeout=45_000)
        except PWTimeoutError:
            page.screenshot(path="ceqanet_debug_results_no_download.png", full_page=True)
            raise RuntimeError("Results loaded but no Download CSV found. Saved ceqanet_debug_results_no_download.png")

        # Download CSV
        try:
            with page.expect_download(timeout=90_000) as dl_info:
                ok_dl = _click_first(page, [
                    "text=Download CSV",
                    "button:has-text('Download CSV')",
                    "a:has-text('Download CSV')",
                    "a[href*='csv' i]",
                ], timeout_ms=15_000)
                if not ok_dl:
                    page.screenshot(path="ceqanet_debug_no_download_csv.png", full_page=True)
                    raise RuntimeError("Could not click Download CSV.")
            download = dl_info.value
            path = download.path()
            csv_bytes = open(path, "rb").read()
        except Exception as e:
            page.screenshot(path="ceqanet_debug_download_failed.png", full_page=True)
            raise RuntimeError(f"CSV download failed. Saved ceqanet_debug_download_failed.png. Error={e}")

        results_url = page.url
        browser.close()
        return csv_bytes, results_url


# =============================
# Detail scraper (CEQA detail page like /2026010057)
# =============================

def _extract_kv_section(section_text: str) -> Dict[str, str]:
    """
    CEQAnet detail pages render as label/value pairs. We parse by scanning known labels.
    """
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
            # value is next non-label line
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


def scrape_ceqa_detail(detail_url: str, timeout_ms: int = 120_000) -> Dict[str, object]:
    """
    Returns:
      {
        "summary": {...},
        "contacts": [ {name, agency_name, job_title, contact_types, address, phone, email}, ... ],
        "location": {...}
      }
    """
    out: Dict[str, object] = {"summary": {}, "contacts": [], "location": {}}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        page.goto(detail_url, wait_until="domcontentloaded", timeout=timeout_ms)
        page.wait_for_timeout(900)

        body_text = page.locator("body").inner_text(timeout=20_000)

        # Summary is typically between "Summary" and "Contact Information"
        s_idx = body_text.lower().find("summary")
        c_idx = body_text.lower().find("contact information")
        l_idx = body_text.lower().find("\nlocation")

        if s_idx != -1 and c_idx != -1 and c_idx > s_idx:
            summary_text = body_text[s_idx:c_idx]
            out["summary"] = _extract_kv_section(summary_text)

        # Contacts between "Contact Information" and "Location"
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

        # Normalize contacts
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

        # Location section (simple parse)
        if l_idx != -1:
            loc_text = body_text[l_idx:]
            # pull a compact set of location fields if present
            loc_fields = ["cities", "counties", "regions", "cross streets", "zip", "total acres", "parcel(s)", "state highways", "township", "range", "section", "base"]
            loc_lines = [ln.strip() for ln in loc_text.splitlines() if ln.strip()]
            loc: Dict[str, str] = {}
            i = 0
            while i < len(loc_lines):
                k = loc_lines[i].strip().lower()
                if k in loc_fields:
                    if i + 1 < len(loc_lines):
                        v = loc_lines[i + 1].strip()
                        loc[k] = v
                        i += 2
                    else:
                        i += 1
                else:
                    i += 1
            out["location"] = loc

        browser.close()

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

    print(f"CEQAnet (CA) | pulling records from {DATE_START} → {DATE_END} for doc types: {DOC_TYPES}")

    all_rows: List[Dict[str, str]] = []
    any_results_url = ""

    # Run 2 searches: NOP and NOE
    for dt in DOC_TYPES:
        csv_bytes, results_url = ceqanet_download_csv_for_range_and_doc_type(DATE_START, DATE_END, dt)
        any_results_url = results_url or any_results_url
        part = parse_csv_bytes(csv_bytes)
        print(f"Downloaded rows for {dt}: {len(part)}")
        all_rows.extend(part)

    print(f"Total downloaded rows (combined): {len(all_rows)}")
    if not all_rows:
        print("No rows parsed (empty export).")
        return

    now = utc_now_str()
    rows_to_append: List[List[str]] = []
    appended = 0
    detail_used = 0

    for r in all_rows:
        if appended >= MAX_NEW:
            break

        # CSV fields (vary by export; we try multiple names)
        sch = pick(r, ["SCH Number", "SCH", "SCH#", "State Clearinghouse Number"])
        title = pick(r, ["Title", "Project Title", "Project"])
        lead = pick(r, ["Lead/Public Agency", "Lead Agency", "Agency"])
        received = pick(r, ["Received", "Received Date", "Date Received"])
        doc_type = pick(r, ["Type", "Document Type", "Doc Type"])
        county = pick(r, ["County"])
        city = pick(r, ["City"])
        dev_type = pick(r, ["Development Type", "Dev Type", "Development"])
        location_csv = pick(r, ["Location", "Project Location", "Address"])

        # Detail URL + CEQA ID
        detail_url = ceqa_detail_url_from_row(r)
        ceqa_id = ""
        if detail_url.startswith(CEQANET_URL):
            ceqa_id = detail_url.replace(CEQANET_URL, "").strip("/")

        award_id = stable_award_id(ceqa_id=ceqa_id, sch=sch, title=title, doc_type=doc_type, received=received)
        if award_id in existing_ids:
            continue

        detail_summary: Dict[str, str] = {}
        detail_contacts: List[Dict[str, str]] = []
        detail_location: Dict[str, str] = {}

        if ENRICH_DETAILS and detail_url and detail_used < DETAIL_CAP:
            try:
                d = scrape_ceqa_detail(detail_url, timeout_ms=150_000)
                detail_summary = d.get("summary", {}) or {}
                detail_contacts = d.get("contacts", []) or []
                detail_location = d.get("location", {}) or {}
                detail_used += 1
            except Exception as e:
                print(f"[DETAIL] Failed for URL={detail_url}: {e}")

        chosen = pick_preferred_contact(detail_contacts)

        # Project description: prefer detail page content
        present_land_use = (detail_summary.get("present land use") or "").strip()
        proposed = (detail_summary.get("proposed project") or "").strip()
        proj_desc = (detail_summary.get("project description") or "").strip()

        description_blob = " | ".join([x for x in [present_land_use, proj_desc, proposed] if x])
        if not description_blob:
            description_blob = title

        # Stage/doc type: prefer detail page
        stage = (detail_summary.get("document type") or doc_type or "").strip()

        # Received/submitted date: prefer detail page
        submitted = (detail_summary.get("received") or received or "").strip()

        # “When” if possible: review end dates
        state_review_end = (detail_summary.get("state review period end") or "").strip()
        public_review_end = (detail_summary.get("public review period end") or "").strip()

        # Where: prefer detail location cities/counties; fall back to CSV
        loc_city = (detail_location.get("cities") or city or "").strip()
        loc_county = (detail_location.get("counties") or county or "").strip()
        place_of_perf = ", ".join([x for x in [loc_city, loc_county, "CA"] if x])

        # Private-side company: best proxy is Applicant / Consulting Firm agency name
        recipient_company = (chosen.get("agency_name", "") if chosen else "")

        is_constructionish = looks_like_construction_project(title=title, desc=description_blob, dev_type=dev_type)

        contacts_json = json.dumps(detail_contacts, ensure_ascii=False)

        # Award link = actual detail link when available
        award_link = detail_url or (any_results_url if any_results_url else f"SCH={sch}")

        values = {
            "Award ID": award_id,
            "Recipient (Company)": recipient_company,
            "Recipient UEI": "",
            "Parent Recipient UEI": "",
            "Parent Recipient DUNS": "",
            "Recipient (HQ) Address": (chosen.get("address", "") if chosen else "") or location_csv,
            "Start Date": "2026-01-01",  # marker for your 2026 pipeline
            "End Date": "",
            "Last Modified Date": now,
            "Award Amount (Obligated)": "",
            "NAICS Code": "",
            "NAICS Description": "",
            "Awarding Agency": lead,
            "Place of Performance": place_of_perf,

            # Put the info you asked for directly into Description
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
                f"CEQA_ID={ceqa_id}; SCH={sch}; DocType={stage}; Submitted={submitted}; "
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

    if rows_to_append:
        ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        print(f"✅ Appended {len(rows_to_append)} rows into {TAB_NAME}.")
    else:
        print("No new rows appended (deduped or empty).")

    print(f"Detail pages scraped: {detail_used} (cap={DETAIL_CAP})")
    print("Done.")


if __name__ == "__main__":
    main()
