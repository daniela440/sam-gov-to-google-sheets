import os
import json
import time
from datetime import datetime, timedelta

import requests
import gspread
from google.oauth2.service_account import Credentials


def sam_get(url, params, retries=5, sleep_seconds=3):
    """
    Wrapper for SAM.gov GET requests with basic rate-limit handling (429).
    Retries with a simple backoff.
    """
    for attempt in range(retries):
        response = requests.get(url, params=params, timeout=60)

        # Rate limited
        if response.status_code == 429:
            # Backoff: 3s, 6s, 12s, 24s...
            wait = sleep_seconds * (2 ** attempt)
            time.sleep(wait)
            continue

        # Other errors
        response.raise_for_status()
        return response

    # If we exhausted retries, raise the last response if present
    response.raise_for_status()
    return response


# =============================
# CONFIG
# =============================
SAM_API_KEY = os.environ["SAM_API_KEY"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
GOOGLE_CREDENTIALS_JSON = os.environ["GOOGLE_CREDENTIALS_JSON"]

# Only these NAICS codes
NAICS_CODES = ["238210", "236220", "237310", "238220", "238120"]

# How many awards per NAICS code to fetch per run
LIMIT_PER_NAICS = 25

# How many days back to look
DAYS_BACK = 30

# Polite delay between entity lookups (helps avoid 429)
ENTITY_LOOKUP_DELAY_SECONDS = 0.6


# =============================
# GOOGLE SHEETS AUTH
# =============================
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

creds_info = json.loads(GOOGLE_CREDENTIALS_JSON)
creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
client = gspread.authorize(creds)

sheet = client.open_by_key(SPREADSHEET_ID).sheet1  # first tab


def ensure_header():
    # If A1 is empty or missing, write header row
    a1 = sheet.acell("A1").value
    if not a1 or str(a1).strip() == "":
        sheet.update(
            "A1:E1",
            [["Company Name", "Website", "Physical Address", "Phone Number", "NAICS Code"]],
        )


def safe_get(d, path, default=""):
    """
    path like: ["award","awardee","name"]
    """
    cur = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur if cur is not None else default


def join_parts(parts):
    parts = [p.strip() for p in parts if isinstance(p, str) and p.strip()]
    return ", ".join(parts)


def fetch_awards_for_naics(naics_code: str):
    """
    Opportunities API:
      - date range is mandatory
      - awards are ptype=a
      - NAICS filter is ncode
    """
    url = "https://api.sam.gov/opportunities/v2/search"

    today = datetime.utcnow().date()
    posted_to = today.strftime("%m/%d/%Y")
    posted_from = (today - timedelta(days=DAYS_BACK)).strftime("%m/%d/%Y")

    params = {
        "api_key": SAM_API_KEY,
        "ptype": "a",
        "postedFrom": posted_from,
        "postedTo": posted_to,
        "ncode": naics_code,
        "limit": LIMIT_PER_NAICS,
        "offset": 0,
    }

    r = sam_get(url, params)
    data = r.json()

    return data.get("opportunitiesData") or []


def fetch_entity_by_uei(uei: str):
    """
    Entity Information API (public): fetch entity details by UEI.
    """
    if not uei:
        return None

    url = "https://api.sam.gov/entity-information/v2/entities"
    params = {
        "api_key": SAM_API_KEY,
        "ueiSAM": uei,
        "includeSections": "entityRegistration,coreData,pointsOfContact",
    }

    try:
        r = sam_get(url, params)
    except requests.HTTPError:
        return None

    return r.json()


def extract_website_phone_from_entity(entity_json: dict):
    """
    Tries a few common shapes. Returns (website, phone, physical_address_override)
    """
    if not isinstance(entity_json, dict):
        return ("", "", "")

    entity_list = entity_json.get("entityData") or entity_json.get("entities") or []
    if not entity_list:
        return ("", "", "")

    e = entity_list[0] if isinstance(entity_list, list) else entity_list

    # Website
    website = (
        safe_get(e, ["entityRegistration", "url"], "")
        or safe_get(e, ["entityRegistration", "website"], "")
        or safe_get(e, ["coreData", "url"], "")
        or safe_get(e, ["coreData", "website"], "")
        or ""
    )

    url_list = safe_get(e, ["coreData", "urlList"], []) or safe_get(e, ["entityRegistration", "urlList"], [])
    if not website and isinstance(url_list, list):
        for item in url_list:
            if isinstance(item, dict):
                u = item.get("url") or item.get("value")
                if u:
                    website = u
                    break
            elif isinstance(item, str) and item.strip():
                website = item.strip()
                break

    # Phone
    phone = (
        safe_get(e, ["pointsOfContact", "governmentBusinessPOC", "usPhone"], "")
        or safe_get(e, ["pointsOfContact", "electronicBusinessPOC", "usPhone"], "")
        or safe_get(e, ["pointsOfContact", "pastPerformancePOC", "usPhone"], "")
        or safe_get(e, ["entityRegistration", "usPhone"], "")
        or safe_get(e, ["coreData", "usPhone"], "")
        or ""
    )

    # Physical address
    address = join_parts([
        safe_get(e, ["entityRegistration", "physicalAddress", "addressLine1"], ""),
        safe_get(e, ["entityRegistration", "physicalAddress", "addressLine2"], ""),
        safe_get(e, ["entityRegistration", "physicalAddress", "city"], ""),
        safe_get(e, ["entityRegistration", "physicalAddress", "stateOrProvinceCode"], ""),
        safe_get(e, ["entityRegistration", "physicalAddress", "zipCode"], ""),
        safe_get(e, ["entityRegistration", "physicalAddress", "countryCode"], ""),
    ])

    return (website, phone, address)


def main():
    ensure_header()

    rows_to_append = []

    for naics in NAICS_CODES:
        awards = fetch_awards_for_naics(naics)

        for item in awards:
            d = item.get("data", item)

            company = (
                safe_get(d, ["award", "awardee", "name"], "")
                or safe_get(d, ["award", "awardee", "legalBusinessName"], "")
                or ""
            )

            naics_code = item.get("naicsCode") or d.get("naicsCode") or naics

            # Address from award payload (sometimes present)
            loc = safe_get(d, ["award", "awardee", "location"], {}) or {}
            award_address = join_parts([
                loc.get("streetAddress", "") if isinstance(loc, dict) else "",
                loc.get("streetAddress2", "") if isinstance(loc, dict) else "",
                (loc.get("city") or {}).get("name") if isinstance(loc.get("city"), dict) else (loc.get("city") or ""),
                (loc.get("state") or {}).get("name") if isinstance(loc.get("state"), dict) else (loc.get("state") or ""),
                loc.get("zip", "") if isinstance(loc, dict) else "",
                (loc.get("country") or {}).get("name") if isinstance(loc.get("country"), dict) else (loc.get("country") or ""),
            ])

            # Website/phone sometimes present in award payload, but often missing
            website = safe_get(d, ["award", "awardee", "website"], "") or safe_get(d, ["award", "awardee", "url"], "")
            phone = safe_get(d, ["award", "awardee", "phone"], "") or safe_get(d, ["award", "awardee", "telephone"], "")

            # UEI for enrichment
            uei = (
                safe_get(d, ["award", "awardee", "ueiSAM"], "")
                or safe_get(d, ["award", "awardee", "uei"], "")
                or safe_get(d, ["award", "awardee", "uniqueEntityId"], "")
            )

            # Enrich from entity API if needed
            if (not website or not phone or not award_address) and uei:
                entity_json = fetch_entity_by_uei(uei)
                ent_website, ent_phone, ent_address = extract_website_phone_from_entity(entity_json)

                if not website and ent_website:
                    website = ent_website
                if not phone and ent_phone:
                    phone = ent_phone
                if not award_address and ent_address:
                    award_address = ent_address

                # Be polite to the API
                time.sleep(ENTITY_LOOKUP_DELAY_SECONDS)

            if company:
                rows_to_append.append([company, website, award_address, phone, naics_code])

    if rows_to_append:
        sheet.append_rows(rows_to_append, value_input_option="RAW")

    print(f"Done. Added {len(rows_to_append)} rows.")


if __name__ == "__main__":
    main()
