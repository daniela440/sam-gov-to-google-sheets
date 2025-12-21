import requests
import gspread
from google.oauth2.service_account import Credentials
import os
import json

# ===== CONFIG =====
SAM_API_KEY = "SAM-7f9ae9a8-1f25-42bd-a52c-295ffff7934b"
SPREADSHEET_NAME = "SAM – Awarded Bids – Auto Import"

# ===== GOOGLE AUTH =====
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds_info = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])

creds = Credentials.from_service_account_info(
    creds_info,
    scopes=scopes
)

client = gspread.authorize(creds)
sheet = client.open(SPREADSHEET_NAME).sheet1

# ===== SAM.GOV REQUEST =====
url = "https://api.sam.gov/prod/opportunities/v2/search"
params = {
    "api_key": SAM_API_KEY,
    "notice_type": "award",
    "limit": 10
}

response = requests.get(url, params=params)
data = response.json()

# ===== PARSE + WRITE =====
for item in data.get("opportunitiesData", []):
    company = item.get("award", {}).get("awardee", {}).get("name", "")
    address = item.get("award", {}).get("awardee", {}).get("address", {}).get("line1", "")
    phone = item.get("award", {}).get("awardee", {}).get("phone", "")
    naics = item.get("classification", {}).get("naics", "")
    website = ""

    sheet.append_row([
        company,
        website,
        address,
        phone,
        naics
    ])
