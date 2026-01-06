import os
import json
import time
import hashlib
import re
import io
import zipfile
import pandas as pd
import requests
import gspread
from datetime import datetime, timezone
from google.oauth2.service_account import Credentials

# =============================
# CONFIG
# =============================
# CSLB Master List Download Page (Statewide)
CSLB_MASTER_DOWNLOAD_URL = "https://www.cslb.ca.gov/onlineservices/dataportal/ContractorList"

# Target Criteria
TARGET_COUNTY = "LOS ANGELES" # CSLB data is usually uppercase
TARGET_CLASSIFICATIONS = ["A", "B", "C-10", "C-20", "C-36", "C-4", "C-32", "C-51", "C-50"]

# NAICS Mapping (Matches your existing logic)
CLASS_TO_NAICS = {
    "C-10": "238210", "C-20": "238220", "C-36": "238220", "C-4": "238220",
    "C-51": "238120", "C-50": "238120", "A": "237310", "C-32": "237310", "B": "236220"
}
NAICS_DESC = {
    "238210": "Electrical Contractors",
    "236220": "Commercial/Institutional Building",
    "237310": "Highway/Street/Bridge",
    "238220": "Plumbing/HVAC",
    "238120": "Structural Steel"
}

# =============================
# Helper Functions
# =============================
def get_gspread_client():
    creds_json = os.environ.get("LA_GOOGLE_CREDENTIALS_JSON")
    if not creds_json: raise RuntimeError("Missing LA_GOOGLE_CREDENTIALS_JSON secret")
    creds_dict = json.loads(creds_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    return gspread.authorize(Credentials.from_service_account_info(creds_dict, scopes=scopes))

def fetch_cslb_master_data():
    """
    Downloads the full License Master file from CSLB.
    Note: CSLB often requires a direct download from their specific backend endpoint.
    If the CSV download link changes, we find it here.
    """
    # For automation, we typically use the direct CSV or Excel link if available.
    # Replace this with the specific direct download URL from the 'Master List' portal
    # Typical pattern: https://www.cslb.ca.gov/OnlineServices/DataPortal/Download.aspx?t=LicenseMaster
    download_url = "https://www.cslb.ca.gov/OnlineServices/DataPortal/Download.aspx?t=LicenseMaster"
    
    print(f"Connecting to CSLB Master List...")
    response = requests.get(download_url, timeout=120)
    response.raise_for_status()
    
    # Load into Pandas for efficient filtering
    # The CSLB files are often tab-separated or standard CSV
    try:
        df = pd.read_csv(io.BytesIO(response.content), low_memory=False)
    except:
        # Fallback for Excel format if they provide .xls
        df = pd.read_excel(io.BytesIO(response.content))
    
    return df

def main():
    # Load Env Vars
    sheet_id = os.environ.get("LA_SHEET_ID")
    tab_name = os.environ.get("LA_TAB_NAME")
    
    # Connect to Sheet
    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab_name)
    
    # Load Existing IDs to avoid duplicates
    existing_ids = set(ws.col_values(1)[1:]) 
    
    # Get and Filter Data
    df = fetch_cslb_master_data()
    print(f"Total statewide records: {len(df)}")
    
    # Filter by County and Class
    # Column names may vary (e.g., 'COUNTY', 'CLASS_CODE') - check CSLB documentation
    df.columns = [c.strip().upper() for c in df.columns]
    
    filtered_df = df[
        (df['COUNTY'].str.contains(TARGET_COUNTY, na=False, case=False)) &
        (df['LICENSE_STATUS'].str.contains('ACTIVE', na=False, case=False))
    ]
    
    new_rows = []
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    for _, row in filtered_df.iterrows():
        lic_no = str(row.get('LICENSE_NUMBER', ''))
        if lic_no in existing_ids or not lic_no:
            continue
            
        # Extract classification to find NAICS
        classes = str(row.get('CLASSIFICATIONS', ''))
        primary_class = classes.split(',')[0].strip() # Get the first one
        naics_code = CLASS_TO_NAICS.get(primary_class, "")
        
        if not naics_code: continue # Skip if doesn't match your target list

        # Format row for Google Sheets (Matching your columns)
        new_rows.append([
            lic_no,                                  # Award ID
            row.get('BUSINESS_NAME', ''),            # Recipient
            "", "", "",                              # UEI / DUNS
            row.get('ADDRESS', ''),                  # HQ Address
            "2026-07-01",                            # Start Date
            "", now, "",                             # Dates / Amount
            naics_code,
            NAICS_DESC.get(naics_code, ""),
            "CSLB Master List",
            "Los Angeles County, CA",
            f"Active License. Class: {classes}",
            "",                                      # Award Link
            "https://www.cslb.ca.gov/OnlineServices/CheckLicenseII/CheckLicense.aspx",
            f"https://www.google.com/search?q={row.get('BUSINESS_NAME','')}"
        ])

    # Append to Sheets
    if new_rows:
        ws.append_rows(new_rows, value_input_option="USER_ENTERED")
        print(f"✅ Successfully added {len(new_rows)} new contractors to {tab_name}.")
    else:
        print("No new matching contractors found.")

if __name__ == "__main__":
    main()
