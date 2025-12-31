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
DATASET_JOB_FILINGS = "w9ak-ipjd"        # DOB NOW: Build – Job Application Filings
DATASET_APPROVED_PERMITS = "rbx6-tga4"   # DOB NOW: Build – Approved Permits


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
    # For both datasets, this exists (per your sample keys)
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
    Uses NYC DOB fields actually present in your sample:
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

    # Status signals
    status_text = f"{filing_status} {permit_status}".lower()
    if "permit" in status_text:
        score += 15
        rationale.append("status_contains_permit(+15)")
    if "approv" in status_text:
        score += 10
        rationale.append("status_contains_approved(+10)")

    # Permit existence proxy
    if first_permit_date:
        score += 25
        rationale.append(f"first_permit_date_present(+25)")

    # 2025 activity signals (proxy for 2026 start planning/execution)
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

    # Job type
    job_type = first_present(job, ["job_type"]).lower()
    if "new" in job_type:
        score += 15
        rationale.append("job_type_new(+15)")
    elif "alter" in job_type:
        score += 10
        rationale.append("job_type_alteration(+10)")

    # Cost
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

    # Start date anchor (only if score is strong enough in strict mode)
    start_date = ""
    if score >= 70:
        q2, q3, q4 = "2026-04-01", "2026-07-01", "2026-10-01"
        anchor = first_permit_date or current_status_date or filing_date

        # If anchor is in 2025, map month -> quarter
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


def build_links(job_filing_number: str) -> tuple[str, str]:
    award_link = (
        f"https://data.cityofnewyork.us/resource/{DATASET_JOB_FILINGS}.json"
        f"?$where=job_filing_number='{job_filing_number}'"
    )
    web_search = f"https://www.google.com/search?q={job_filing_number}+site%3Adata.cityofnewyork.us"
    return award_link, web_search


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

    # Discovery controls
    min_score = int(os.environ.get("NYC_MIN_SCORE", "70"))  # set to 40 for discovery run
    discovery_mode = os.environ.get("NYC_DISCOVERY_MODE", "false").lower() == "true"

    # 2025-only filter (your requirement)
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

    # Connect to Google Sheet
    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab_name)
    hmap = header_map(ws)
    existing_ids = load_existing_award_ids(ws)

    # Pull permits (no date filter here; we join by job_filing_number)
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

    # Pull ONLY 2025-activity job filings
    rows_to_append = []
    now = utc_now_str()

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

            # Discovery mode: force Q3 2026 if not assigned (so you can see rows)
            if discovery_mode and not start_date:
                start_date = "2026-07-01"
                rationale = rationale + "; discovery_mode_forced_Q3_2026"

            if start_date in {"2026-04-01", "2026-07-01", "2026-10-01"}:
                c_assigned_q2026 += 1
            else:
                continue

            # Map fields into your sheet schema (best-effort)
            company = first_present(job, ["owner_s_business_name", "filing_representative_business_name"])
            hq_addr = first_present(job, ["owner_s_street_name", "filing_representative_street_name"])

            pop = f"{first_present(job, ['house_no'])} {first_present(job, ['street_name'])}, {first_present(job, ['borough'])} {first_present(job, ['zip'])}".strip()
            desc = first_present(job, ["job_description"])
            est_cost = first_present(job, ["initial_cost"])

            award_link, web_search = build_links(award_id)
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
                "Place of Performance": pop,
                "Description": desc,
                "Award Link": award_link,
                "Recipient Profile Link": "",
                "Web Search Link": web_search,

                # enrichment placeholders (do not invent)
                "Company Website": "",
                "Company Phone": "",
                "Company General Email": "",
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
                "notes": "",
            }

            # Write row in sheet header order (no assumptions)
            ordered_row = [""] * len(hmap)
            for header, col_index in hmap.items():
                ordered_row[col_index - 1] = values.get(header, "")

            rows_to_append.append(ordered_row)
            existing_ids.add(award_id)

        time.sleep(sleep_seconds)

    # Append + debug output
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


if __name__ == "__main__":
    main()
