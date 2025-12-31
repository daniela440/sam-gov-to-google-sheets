import os
import json
import time
import hashlib
from datetime import datetime, timezone

import requests
import gspread
from google.oauth2.service_account import Credentials


# -----------------------------
# Config
# -----------------------------
ALLOWED_NAICS = {"238210", "236220", "237310", "238220", "238120"}

# NYC Open Data datasets (Socrata IDs)
# DOB NOW: Build – Job Application Filings
DATASET_JOB_FILINGS = "w9ak-ipjd"
# DOB NOW: Build – Approved Permits
DATASET_APPROVED_PERMITS = "rbx6-tga4"


def utc_now_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def normalize_str(x):
    return (str(x).strip() if x is not None else "")


def first_present(record: dict, candidates: list[str]) -> str:
    """Return the first non-empty field found among candidate keys."""
    for k in candidates:
        v = record.get(k)
        if v is None:
            continue
        v = normalize_str(v)
        if v:
            return v
    return ""


def socrata_get(dataset_id: str, where: str | None = None, limit: int = 2000, offset: int = 0) -> list[dict]:
    """Fetch rows from NYC Open Data Socrata endpoint."""
    base = f"https://data.cityofnewyork.us/resource/{dataset_id}.json"
    headers = {}
    token = os.environ.get("NYC_SOCRATA_APP_TOKEN")
    if token:
        headers["X-App-Token"] = token

    params = {
        "$limit": limit,
        "$offset": offset,
    }
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


def header_map(ws):
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("Row 1 headers are empty. Paste your column headers in row 1.")
    # map header->col_index (1-based)
    return {h: i + 1 for i, h in enumerate(headers)}


def load_existing_award_ids(ws, award_id_header="Award ID") -> set[str]:
    """Read existing Award IDs to avoid duplicates."""
    col_values = ws.col_values(1)  # Column A expected Award ID
    # First row is header; skip blanks
    return {v.strip() for v in col_values[1:] if v and v.strip()}


def infer_naics(job_record: dict, permit_record: dict | None) -> str:
    """
    Conservative NAICS inference.
    You can refine mapping later once you inspect real field distributions.
    """
    # Candidate fields that may hint at discipline/type
    work_type = first_present(job_record, ["work_type", "job_type", "jobtype", "job_type_code"])
    permit_type = first_present(permit_record or {}, ["permit_type", "permit_subtype", "permittypename"]) if permit_record else ""

    text = f"{work_type} {permit_type}".lower()

    # Very simple keyword mapping:
    if "elect" in text:
        return "238210"
    if "plumb" in text or "hvac" in text or "mechanic" in text or "boiler" in text:
        return "238220"
    if "steel" in text or "struct" in text or "precast" in text:
        return "238120"
    if "bridge" in text or "highway" in text or "street" in text or "road" in text:
        return "237310"

    # Default for most DOB building jobs:
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
    Returns: (score 0-100, start_date YYYY-MM-DD or "", rationale)
    Start dates anchored: Q2=2026-04-01, Q3=2026-07-01, Q4=2026-10-01
    Only assigns start_date if score >= 70.
    """
    score = 0
    rationale_parts = []

    # Status/permit signals
    permit_issued = first_present(permit or {}, ["issuance_date", "permit_issued_date", "issued_date", "issue_date"])
    job_status = first_present(job, ["job_status", "status", "jobstatus"])

    if permit_issued:
        score += 25
        rationale_parts.append(f"permit_issued={permit_issued}(+25)")
    if job_status:
        js = job_status.lower()
        if "permit" in js:
            score += 15
            rationale_parts.append(f"job_status={job_status}(+15)")
        elif "approv" in js:
            score += 10
            rationale_parts.append(f"job_status={job_status}(+10)")

    # Timing signals (look at year)
    filed_date = first_present(job, ["filing_date", "filed_date", "application_date", "date_filed"])
    def year_of(d: str) -> int | None:
        try:
            return int(d[:4])
        except Exception:
            return None

    py = year_of(permit_issued) if permit_issued else None
    fy = year_of(filed_date) if filed_date else None

    if py == 2025:
        score += 20
        rationale_parts.append("permit_year=2025(+20)")
    elif fy == 2025 and not permit_issued:
        score += 15
        rationale_parts.append("filed_year=2025(+15)")
    elif fy == 2024 and not permit_issued:
        score += 10
        rationale_parts.append("filed_year=2024(+10)")

    # Job type
    job_type = first_present(job, ["job_type", "jobtype", "work_type"])
    jt = job_type.upper()
    if "NB" in jt or "NEW" in jt:
        score += 15
        rationale_parts.append(f"job_type={job_type}(+15)")
    elif "ALT" in jt or "A1" in jt or "ALT-1" in jt:
        score += 10
        rationale_parts.append(f"job_type={job_type}(+10)")

    # Cost
    est_cost_raw = first_present(job, ["estimated_job_cost", "estimated_cost", "job_cost", "estimatedconstructioncost"])
    est_cost = 0.0
    if est_cost_raw:
        try:
            est_cost = float(est_cost_raw.replace(",", "").replace("$", ""))
        except Exception:
            est_cost = 0.0

    if est_cost >= 50_000_000:
        score += 15
        rationale_parts.append("est_cost>=50M(+15)")
    elif est_cost >= 10_000_000:
        score += 10
        rationale_parts.append("est_cost>=10M(+10)")
    elif est_cost >= 1_000_000:
        score += 5
        rationale_parts.append("est_cost>=1M(+5)")

    # Not started (we only have proxies; keep conservative)
    # If there is a completion/signoff field, use it as exclusion:
    completion = first_present(job, ["completion_date", "signed_off_date", "job_completed_date"])
    if completion:
        # Exclude effectively by forcing score low
        rationale_parts.append(f"completion_date={completion}(exclude)")
        return (0, "", "Excluded: completed/signed off")

    # Assign quarter start date if >=70
    start_date = ""
    if score >= 70:
        # Use permit issued month when available; otherwise default to Q3
        q2 = "2026-04-01"
        q3 = "2026-07-01"
        q4 = "2026-10-01"

        def month_of(d: str) -> int | None:
            try:
                return int(d[5:7])
            except Exception:
                return None

        m = month_of(permit_issued) if permit_issued else None
        if py == 2025 and m is not None:
            if 1 <= m <= 6:
                start_date = q2
                rationale_parts.append("quarter=Q2(permitted Jan-Jun 2025)")
            elif 7 <= m <= 9:
                start_date = q3
                rationale_parts.append("quarter=Q3(permitted Jul-Sep 2025)")
            else:
                start_date = q4
                rationale_parts.append("quarter=Q4(permitted Oct-Dec 2025)")
        else:
            # Conservative default for strong but not permitted-yet jobs
            start_date = q3
            rationale_parts.append("quarter=Q3(default)")

        # Clamp to Q2–Q4 only
        if start_date not in (q2, q3, q4):
            start_date = q3

    rationale = "; ".join(rationale_parts) if rationale_parts else "No rationale"
    return (score, start_date, rationale)


def build_links(award_id: str) -> tuple[str, str]:
    # DOB NOW public portal links differ; we keep a safe search link pattern for now.
    web_search = f"https://www.google.com/search?q={award_id}+site%3Adata.cityofnewyork.us"
    # Placeholder award link; you can refine once you confirm the public portal format you want.
    award_link = f"https://data.cityofnewyork.us/resource/{DATASET_JOB_FILINGS}.json?$where=job__={award_id}"
    return award_link, web_search


def main():
    # Sheet env
    sheet_id = os.environ.get("NYC_SHEET_ID")
    tab_name = os.environ.get("NYC_TAB_NAME")
    if not sheet_id or not tab_name:
        raise RuntimeError("Missing NYC_SHEET_ID or NYC_TAB_NAME")

    batch_size = int(os.environ.get("NYC_BATCH_SIZE", "200"))
    max_pages = int(os.environ.get("NYC_MAX_PAGES", "5"))  # safety cap
    sleep_seconds = float(os.environ.get("NYC_SLEEP_SECONDS", "0.25"))

    # 1) Connect to Google Sheet
    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab_name)
    hmap = header_map(ws)
    existing_ids = load_existing_award_ids(ws)

    # 2) Pull approved permits (recent-ish) and index by job number where possible
    permits_by_job = {}
    for page in range(max_pages):
        rows = socrata_get(DATASET_APPROVED_PERMITS, where=None, limit=batch_size, offset=page * batch_size)
        if not rows:
            break
        for r in rows:
            job_no = first_present(r, ["job__", "job_number", "jobno", "job"])
            if job_no:
                permits_by_job[job_no] = r
        time.sleep(sleep_seconds)

    # 3) Pull job filings and create rows
    rows_to_append = []
    now = utc_now_str()

    for page in range(max_pages):
        jobs = socrata_get(DATASET_JOB_FILINGS, where=None, limit=batch_size, offset=page * batch_size)
        if not jobs:
            break

        for job in jobs:
            award_id = first_present(job, ["job__", "job_number", "jobno", "job"])
            if not award_id:
                continue
            if award_id in existing_ids:
                continue

            permit = permits_by_job.get(award_id)

            # NAICS inference + filter
            naics = infer_naics(job, permit)
            if naics not in ALLOWED_NAICS:
                continue

            score, start_date, rationale = score_and_quarter_start(job, permit)
            # Only keep Q2–Q4 2026 targets with score>=70 and start_date assigned
            if score < 70 or start_date not in {"2026-04-01", "2026-07-01", "2026-10-01"}:
                continue

            company = first_present(job, ["contractor_name", "owner_business_name", "applicant_business_name", "business_name"])
            hq_addr = first_present(job, ["applicant_address", "owner_address", "business_address"])
            pop = first_present(job, ["house__", "house_number", "street_name", "borough", "zip_code", "address"])

            awarding = first_present(job, ["owner_name", "applicant_name", "developer_name"])
            desc = first_present(job, ["job_description", "description", "work_description"])

            est_cost = first_present(job, ["estimated_job_cost", "estimated_cost", "job_cost", "estimatedconstructioncost"])

            award_link, web_search = build_links(award_id)

            # Create recipient_id deterministic
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
                "Awarding Agency": awarding,
                "Place of Performance": pop,
                "Description": desc,
                "Award Link": award_link,
                "Recipient Profile Link": "",
                "Web Search Link": web_search,
                # Enrichment columns left blank for now (we will fill later)
                "Company Website": "",
                "Company Phone": "",
                "Company General Email": "",
                "Responsible Person Name": "",
                "Responsible Person Role": "",
                "Responsible Person Email": "",
                "Responsible Person Phone": "",
                # Tracking/scoring
                "job_type": first_present(job, ["job_type", "jobtype", "work_type"]),
                "job_status": first_present(job, ["job_status", "status", "jobstatus"]),
                "filed_date": first_present(job, ["filing_date", "filed_date", "application_date", "date_filed"]),
                "permit_issued_date": first_present(permit or {}, ["issuance_date", "permit_issued_date", "issued_date", "issue_date"]),
                "confidence_score": str(score),
                "prediction_rationale": rationale,
                "target_flag": "TRUE",
                "recipient_id": recipient_id,
                "data_source": "NYC Open Data (DOB NOW Build)",
                "data_confidence_level": "High" if score >= 85 else "Medium",
                "last_verified_date": now,
                "notes": "",
            }

            # Convert to row in correct header order
            ordered_row = [""] * len(hmap)
            for header, col_index in hmap.items():
                ordered_row[col_index - 1] = values.get(header, "")
            rows_to_append.append(ordered_row)
            existing_ids.add(award_id)

        time.sleep(sleep_seconds)

    # 4) Append rows in one batch (fast + less API calls)
    if rows_to_append:
        ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        print(f"✅ Appended {len(rows_to_append)} rows.")
    else:
        print("No new target rows found (Q2–Q4 2026 + NAICS filter + score>=70).")


if __name__ == "__main__":
    main()
