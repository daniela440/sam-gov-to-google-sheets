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
# SAM.GOV REQUEST (v2 requires postedFrom/postedTo)
# =============================
from datetime import datetime, timedelta

# Use the documented production endpoint
url = "https://api.sam.gov/opportunities/v2/search"

# Pull awards posted in the last 30 days (you can change 30 to 7, 14, 60, etc.)
today = datetime.utcnow().date()
posted_to = today.strftime("%m/%d/%Y")
posted_from = (today - timedelta(days=30)).strftime("%m/%d/%Y")

params = {
    "api_key": SAM_API_KEY,
    "ptype": "a",              # a = Award Notice
    "postedFrom": posted_from, # REQUIRED
    "postedTo": posted_to,     # REQUIRED
    "limit": LIMIT,
    "offset": 0,
}

response = requests.get(url, params=params, timeout=60)
response.raise_for_status()
data = response.json()

# Different responses have used different top-level keys over time; handle both.
items = data.get("opportunitiesData") or data.get("data") or []


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

rows = []

def join_parts(parts):
    return ", ".join([p for p in parts if p])

for item in items:
    # Many responses wrap details under "data"
    d = item.get("data", item)

    award = d.get("award", {}) or {}
    awardee = award.get("awardee", {}) or {}

    company = awardee.get("name") or ""
    website = awardee.get("website") or ""
    phone = awardee.get("phone") or ""

    loc = awardee.get("location", {}) or {}
    address = join_parts([
        loc.get("streetAddress"),
        loc.get("streetAddress2"),
        loc.get("city", {}).get("name") if isinstance(loc.get("city"), dict) else loc.get("city"),
        loc.get("state", {}).get("name") if isinstance(loc.get("state"), dict) else loc.get("state"),
        loc.get("zip"),
        loc.get("country", {}).get("name") if isinstance(loc.get("country"), dict) else loc.get("country"),
    ])

    naics = item.get("naicsCode") or d.get("naicsCode") or ""

    if any([company, website, address, phone, naics]):
        rows.append([company, website, address, phone, naics])

if rows:
    sheet.append_rows(rows, value_input_option="RAW")

print(f"Done. Added {len(rows)} rows.")
