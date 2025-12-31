import os
import json
import gspread
from google.oauth2.service_account import Credentials

def main():
    creds_json = os.environ.get("NYC_GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("Missing NYC_GOOGLE_CREDENTIALS_JSON secret")

    creds_dict = json.loads(creds_json)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(credentials)

    sheet_id = os.environ.get("NYC_SHEET_ID")
    tab_name = os.environ.get("NYC_TAB_NAME")
    if not sheet_id or not tab_name:
        raise RuntimeError("Missing NYC_SHEET_ID or NYC_TAB_NAME")

    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab_name)

    headers = ws.row_values(1)
    print("✅ Connected to sheet successfully")
    print("Headers found:")
    for h in headers:
        print("-", h)

if __name__ == "__main__":
    main()
