import os
import json
import gspread
from google.oauth2.service_account import Credentials


def main():
    creds_json = os.environ.get("LA_GOOGLE_CREDENTIALS_JSON")
    sheet_id = os.environ.get("LA_SHEET_ID")
    tab_name = os.environ.get("LA_TAB_NAME")

    if not creds_json:
        raise RuntimeError("Missing LA_GOOGLE_CREDENTIALS_JSON")
    if not sheet_id or not tab_name:
        raise RuntimeError("Missing LA_SHEET_ID or LA_TAB_NAME")

    creds_dict = json.loads(creds_json)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(credentials)

    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab_name)

    headers = ws.row_values(1)

    print("✅ Connected to Google Sheet")
    print(f"Sheet ID: {sheet_id}")
    print(f"Tab name: {tab_name}")
    print(f"Header count: {len(headers)}")
    print("Headers:")
    for h in headers:
        print(f" - {h}")


if __name__ == "__main__":
    main()
