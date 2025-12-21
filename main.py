import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials


# =============================
# CONFIG (FROM GITHUB SECRETS)
# =============================
SAM_API_KEY = os.environ["SAM_API_KEY"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
GOOGLE_CREDENTIALS_JSON = os.environ["GOOGLE_CREDENTIALS_JSON"]

LIMIT = 25  # number of awards per run


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
sheet = client.open_by_key(SPREADSHEET_ID).sheet1


# =============================
# ENSURE HEADER ROW
# =============================
header = [
    "Company Name",
    "Website",
    "Physical Address",
    "Phone Number",
    "NAICS Code",
]

existing_header = sheet.row_values(1)
if existing_header != header:
    sheet.clear()
    sheet.append_row(header)


# =============================
# SAM.GOV REQUEST
# =============================
url = "https://api.sam.gov/prod/opportunities/v2/search"
params = {
    "api_key": SAM_API_KEY,
    "notice_type": "award",
    "limit": LIMIT,
}

response = requests.get(url, params=params, timeout=60)
response.raise_for_status()
data = response.json()

items = data.get("opportunitiesData", []) or []


def build_address(addr: dict) -> str:
    parts = [
        addr.get("line1"),
        addr.get("line2"),
        addr.get("city"),
        addr.get("state"),
        addr.get("zip"),
        addr.get("country"),
    ]
    return ", ".join([p for p in parts if p])


rows = []

for item in items:
    award = item.get("award", {}) or {}
    awardee = award.get("awardee", {}) or {}

    company = (
        awardee.get("name")
        or item.get("awardeeName")
        or item.get("organizationName")
        or ""
    )

    website = awardee.get("website", "") or ""
    phone = awardee.get("phone", "") or ""

    address_data = awardee.get("address", {}) or {}
    address = build_address(address_data)

    classification = item.get("classification", {}) or {}
    naics = ""

    if isinstance(classification.get("naics"), str):
        naics = classification.get("naics")
    elif isinstance(classification.get("naics"), list) and classification.get("naics"):
        first = classification["naics"][0]
        if isinstance(first, dict):
            naics = first.get("code", "") or first.get("naicsCode", "")
        elif isinstance(first, str):
            naics = first

    if any([company, website, address, phone, naics]):
        rows.append([company, website, address, phone, naics])


if rows:
    sheet.append_rows(rows, value_input_option="RAW")

print(f"Done. Added {len(rows)} rows.")
