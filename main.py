import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta

# =============================
# CONFIG
# =============================
SAM_API_KEY = os.environ["SAM_API_KEY"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]

# Only these NAICS codes
NAICS_CODES = ["238210", "236220", "237310", "238220", "238120"]

# How many awards per NAICS code to fetch per run
LIMIT_PER_NAICS = 25

# How many days back to look
DAYS_BACK = 30


# =============================
# GOOGLE SHEETS AUTH
# =============================
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

creds_info = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
client = gspread.authorize(creds)

sheet = client.open_by_key(SPREADSHEET_ID).sheet1  # first tab


def ensure_header():
    # If A1 is empty, write header row
    if sheet.acell("A1").value.strip() == "":
        sheet.update(
            "A1:E1",
            [["Company Name", "Website", "Physical Address", "Phone Number", "NAICS Code"]],
        )


def safe_get(d, path, default=""):
    """
    path like: ["data","award","awardee","name"]
    """
    cur = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur if cur is not None else default


def join_parts(parts):
    return ", ".join([p for p in parts if p])


def fetch_awards_for_naics(naics_code: str):
    # Opportunities API: date is mandatory and NAICS filter is `ncode`
    # Award type is `ptype=a`
    url = "https://api.sam.gov/opportunities/v2/search"

    today = datetime.utcnow().date()
    posted_to = today.strftime("%m/%d/%Y")
    posted_from = (today - timedelta(days=DAYS_BACK)).strftime("%m/%d/%Y")

    params = {
        "api_key": SAM_API_KEY,
        "ptype": "a",
        "postedFrom": posted_from,
        "postedTo": posted_to,
        "ncode": naics_code,           # NAICS filter
        "limit": LIMIT_PER_NAICS,
        "offset": 0,
    }

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()

    # Most common key is "opportunitiesData"
    return data.get("opportunitiesData") or []


def fetch_entity_by_uei(uei: str):
    """
    Entity Management API (public): fetch entity details by UEI.
    We request only the sections we care about.
    """
    if not uei:
        return None

    url = "https://api.sam.gov/entity-information/v2/entities"
    params = {
        "api_key": SAM_API_KEY,
        "ueiSAM": uei,
        "includeSections": "entityRegistration,coreData,pointsOfContact",
    }

    r = requests.get(url, params=params, timeout=60)
    # If entity not found or no access, just skip enrichment
    if r.status_code != 200:
        return None
    return r.json()


def extract_website_phone_from_entity(entity_json: dict):
    """
    Tries a few common shapes. Returns (website, phone, physical_address_override)
    """
    if not isinstance(entity_json, dict):
        return ("", "", "")

    # The API often returns a list under "entityData" (varies by version/response)
    entity_list = entity_json.get("entityData") or entity_json.get("entities") or []
    if not entity_list:
        return ("", "", "")

    e = entity_list[0] if isinstance(entity_list, list) else entity_list

    website = ""
    phone = ""
    address = ""

    # Try common spots
    website = (
        safe_get(e, ["entityRegistration", "url"], "")
        or safe_get(e, ["entityRegistration", "website"], "")
        or safe_get(e, ["coreData", "url"], "")
        or safe_get(e, ["coreData", "website"], "")
        or ""
    )

    # Sometimes there is a url list
    url_list = safe_get(e, ["coreData", "urlList"], []) or safe_get(e, ["entityRegistration", "urlList"], [])
    if not website and isinstance(url_list, list) and url_list:
        # pick first item that looks like a URL
        for item in url_list:
            if isinstance(item, dict):
                u = item.get("url") or item.get("value")
                if u:
                    website = u
                    break
            elif isinstance(item, str):
                website = item
                break

    # Phones can appear in pointsOfContact (public often limited), but we try anyway
    phone = (
        safe_get(e, ["pointsOfContact", "governmentBusinessPOC", "usPhone"], "")
        or safe_get(e, ["pointsOfContact", "electronicBusinessPOC", "usPhone"], "")
        or safe_get(e, ["pointsOfContact", "pastPerformancePOC", "usPhone"], "")
        or safe_get(e, ["entityRegistration", "usPhone"], "")
        or safe_get(e, ["coreData", "usPhone"], "")
        or ""
    )

    # Physical address (if we can get it cleanly from entity)
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

            # Awardee basic info
            company = safe_get(d, ["award", "awardee", "name"], "") or safe_get(d, ["award", "awardee", "legalBusinessName"], "")
            naics_code = item.get("naicsCode") or d.get("naicsCode") or naics

            # Address from award payload (sometimes present)
            loc = safe_get(d, ["award", "awardee", "location"], {}) or {}
            award_address = join_parts([
                loc.get("streetAddress") if isinstance(loc, dict) else "",
                loc.get("streetAddress2") if isinstance(loc, dict) else "",
                (loc.get("city") or {}).get("name") if isinstance(loc.get("city"), dict) else loc.get("city"),
                (loc.get("state") or {}).get("name") if isinstance(loc.get("state"), dict) else loc.get("state"),
                loc.get("zip") if isinstance(loc, dict) else "",
                (loc.get("country") or {}).get("name") if isinstance(loc.get("country"), dict) else loc.get("country"),
            ])

            # These are often missing in awards payload, but try anyway
            website = safe_get(d, ["award", "awardee", "website"], "") or safe_get(d, ["award", "awardee", "url"], "")
            phone = safe_get(d, ["award", "awardee", "phone"], "") or safe_get(d, ["award", "awardee", "telephone"], "")

            # UEI (if present) lets us enrich via Entity API
            uei = (
                safe_get(d, ["award", "awardee", "ueiSAM"], "")
                or safe_get(d, ["award", "awardee", "uei"], "")
                or safe_get(d, ["award", "awardee", "uniqueEntityId"], "")
            )

            # Enrich from Entity API only if we’re missing fields
            if (not website or not phone or not award_address) and uei:
                entity_json = fetch_entity_by_uei(uei)
                ent_website, ent_phone, ent_address = extract_website_phone_from_entity(entity_json)

                if not website and ent_website:
                    website = ent_website
                if not phone and ent_phone:
                    phone = ent_phone
                if not award_address and ent_address:
                    award_address = ent_address

            # Append row
            if company:
                rows_to_append.append([company, website, award_address, phone, naics_code])

    if rows_to_append:
        sheet.append_rows(rows_to_append, value_input_option="RAW")

    print(f"Done. Added {len(rows_to_append)} rows.")


if __name__ == "__main__":
    main()
