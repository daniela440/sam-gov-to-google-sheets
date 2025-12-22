import os
import json
import time
import re
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials


# ====== CONFIG (from GitHub Secrets) ======
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
SHEET_TAB_NAME = os.environ.get("SHEET_TAB_NAME", "USASpending Construction")

# How many rows to process per run (safe batch size)
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))

# Polite pacing between searches (seconds)
SLEEP_SECONDS = float(os.environ.get("SLEEP_SECONDS", "2.5"))

GOOGLE_CREDENTIALS_JSON = os.environ["GOOGLE_CREDENTIALS_JSON"]


# Domains we do NOT want as "official websites"
BLOCKED_DOMAINS = {
    "facebook.com", "www.facebook.com",
    "linkedin.com", "www.linkedin.com",
    "yelp.com", "www.yelp.com",
    "bbb.org", "www.bbb.org",
    "mapquest.com", "www.mapquest.com",
    "opencorporates.com", "www.opencorporates.com",
    "dnb.com", "www.dnb.com",
    "bloomberg.com", "www.bloomberg.com",
    "crunchbase.com", "www.crunchbase.com",
    "instagram.com", "www.instagram.com",
    "x.com", "www.x.com",
    "twitter.com", "www.twitter.com",
    "chamberofcommerce.com", "www.chamberofcommerce.com",
    "yellowpages.com", "www.yellowpages.com",
    "angi.com", "www.angi.com",
    "homeadvisor.com", "www.homeadvisor.com",
}


def normalize_domain(url: str) -> str:
    try:
        p = urlparse(url)
        return (p.netloc or "").lower()
    except Exception:
        return ""


def extract_official_site_from_ddg(company: str, address: str) -> str:
    """
    Very simple approach:
    - search DuckDuckGo HTML endpoint
    - take the first non-directory/non-social domain
    - return the base domain URL (https://domain)
    """
    query = f"{company} {address}"
    ddg_url = "https://html.duckduckgo.com/html/"
    r = requests.post(ddg_url, data={"q": query}, timeout=30, headers={
        "User-Agent": "Mozilla/5.0 (compatible; CompanyWebsiteFinder/1.0)"
    })
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "lxml")
    links = soup.select("a.result__a")

    for a in links[:10]:
        href = a.get("href", "").strip()
        if not href:
            continue

        domain = normalize_domain(href)
        if not domain:
            continue

        # Skip blocked / directory style results
        if domain in BLOCKED_DOMAINS:
            continue

        # Skip obvious PDFs or odd files
        if re.search(r"\.(pdf|doc|docx|xls|xlsx)$", href, re.IGNORECASE):
            continue

        # If we got here, accept
        return f"https://{domain}"

    return ""


def main():
    # Google auth
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_info = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    client = gspread.authorize(creds)

    ws = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_TAB_NAME)

    # Read all values for columns A..R (we need A, P, R)
    # This grabs a rectangular range which is simplest for beginners.
    values = ws.get_values("A:R")

    if not values or len(values) < 2:
        print("No data found.")
        return

    # Identify rows to process:
    # Row 1 is header; data starts at row 2.
    rows_to_update = []
    processed = 0

    for i in range(1, len(values)):
        row_num = i + 1  # because sheet rows are 1-indexed

        row = values[i]
        # Ensure row has at least 18 columns (R is 18th)
        while len(row) < 18:
            row.append("")

        company = (row[0] or "").strip()       # Column A
        address = (row[15] or "").strip()      # Column P (A=0, P=15)
        existing_site = (row[17] or "").strip()  # Column R (A=0, R=17)

        # Skip if no company or already has website
        if not company:
            continue
        if existing_site:
            continue

        # Skip if no address (optional; you can relax this later)
        if not address:
            continue

        website = extract_official_site_from_ddg(company, address)

        if website:
            rows_to_update.append((row_num, website))
        else:
            # Leave blank if not found (you can change this to "REVIEW" later)
            rows_to_update.append((row_num, ""))

        processed += 1
        time.sleep(SLEEP_SECONDS)

        if processed >= BATCH_SIZE:
            break

    if not rows_to_update:
        print("Nothing to update (no blank websites found in this batch).")
        return

    # Batch update Column R
    data = []
    for row_num, website in rows_to_update:
        data.append({
            "range": f"R{row_num}",
            "values": [[website]]
        })

    ws.batch_update(data)
    print(f"Done. Processed {processed} rows; updated {len(rows_to_update)} cells in column R.")


if __name__ == "__main__":
    main()
