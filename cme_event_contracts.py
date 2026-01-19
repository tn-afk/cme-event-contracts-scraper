#!/usr/bin/env python3
"""
CME Event Contracts Volume Scraper
Downloads daily CME Event Contracts PDFs and extracts volume data to Google Sheets.
"""

import os
import sys
import re
import json
import requests
import pdfplumber
from datetime import datetime
from pathlib import Path

# Google Sheets API
import google.auth
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Configuration
CME_BASE_URL = "https://www.cmegroup.com"
SECTION73_URL = f"{CME_BASE_URL}/daily_bulletin/current/Section73_Event_Contracts.pdf"
SWAPS_URL = f"{CME_BASE_URL}/daily_bulletin/preliminary_voi/Event_Contracts_Swap_based.pdf"
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

# Google Sheets configuration
TOKENS_FILE = os.path.expanduser("~/.google_tokens.json")
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


def download_pdf(url: str, output_path: str) -> bool:
    """Download PDF from URL."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()
        with open(output_path, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded: {output_path} ({len(response.content)} bytes)")
        return True
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False


def extract_section73_volume(pdf_path: str) -> int:
    """Extract total volume from Section 73 Event Contracts PDF."""
    total_volume = 0
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    # Find all TOTAL lines and extract the first number (volume)
                    for line in text.split('\n'):
                        if line.strip().startswith('TOTAL'):
                            # TOTAL <volume> <open_interest>
                            parts = line.split()
                            if len(parts) >= 2:
                                try:
                                    vol = int(parts[1].replace(',', ''))
                                    total_volume += vol
                                except ValueError:
                                    pass
        print(f"Section 73 total volume: {total_volume:,}")
        return total_volume
    except Exception as e:
        print(f"Error parsing Section 73 PDF: {e}")
        return 0


def extract_swaps_volume(pdf_path: str) -> int:
    """Extract total volume from Event Contracts Swaps PDF.

    The PDF has CALLS and PUTS sections, each with a 'Totals X Y' summary line.
    We extract the volume (first number) from these summary lines.
    """
    total_volume = 0
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue

                for line in text.split('\n'):
                    # Look for summary "Totals" lines (e.g., "Totals 735,540 1,829,470")
                    # These appear at the end of CALLS and PUTS sections
                    if line.strip().startswith('Totals') and 'by Products' not in line:
                        parts = line.split()
                        if len(parts) >= 2:
                            # Second element should be the volume
                            try:
                                vol = int(parts[1].replace(',', ''))
                                total_volume += vol
                                print(f"  Found subtotal: {vol:,}")
                            except (ValueError, IndexError):
                                pass

        print(f"Swaps total volume: {total_volume:,}")
        return total_volume
    except Exception as e:
        print(f"Error parsing Swaps PDF: {e}")
        return 0


def get_google_credentials():
    """Load Google credentials from environment variables or tokens file."""
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request

    # Try environment variables first (for Render deployment)
    refresh_token = os.environ.get('GOOGLE_REFRESH_TOKEN')
    client_id = os.environ.get('GOOGLE_CLIENT_ID')
    client_secret = os.environ.get('GOOGLE_CLIENT_SECRET')

    if refresh_token and client_id and client_secret:
        print("Using credentials from environment variables")
        token_data = {
            'refresh_token': refresh_token,
            'token_uri': 'https://oauth2.googleapis.com/token',
            'client_id': client_id,
            'client_secret': client_secret,
            'scopes': SCOPES,
        }
        creds = Credentials.from_authorized_user_info(token_data, SCOPES)
    elif os.path.exists(TOKENS_FILE):
        # Fallback to tokens file (for local development)
        print("Using credentials from tokens file")
        with open(TOKENS_FILE, 'r') as f:
            token_data = json.load(f)
        creds = Credentials.from_authorized_user_info(token_data, SCOPES)
    else:
        print("Error: No Google credentials found.")
        print("Set GOOGLE_REFRESH_TOKEN, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET env vars")
        print("Or create ~/.google_tokens.json")
        sys.exit(1)

    # Refresh if expired
    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            print("Refreshed Google credentials")
        except Exception as e:
            print(f"Error refreshing credentials: {e}")
            sys.exit(1)

    return creds


def write_to_google_sheet(spreadsheet_id: str, section73_volume: int, swaps_volume: int, date_str: str):
    """Write volume data to Google Sheet."""
    try:
        creds = get_google_credentials()
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        # Check if header exists, if not add it
        result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range='A1:C1'
        ).execute()

        existing_values = result.get('values', [])
        if not existing_values or existing_values[0] != ['Date', 'Event Contracts (PG 73)', 'Event Contracts (Swaps)']:
            # Add header
            sheet.values().update(
                spreadsheetId=spreadsheet_id,
                range='A1:C1',
                valueInputOption='RAW',
                body={'values': [['Date', 'Event Contracts (PG 73)', 'Event Contracts (Swaps)']]}
            ).execute()
            print("Added header row")

        # Get all existing dates to check for duplicates
        result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range='A:A'
        ).execute()
        existing_dates = [row[0] if row else '' for row in result.get('values', [])]

        if date_str in existing_dates:
            # Update existing row
            row_idx = existing_dates.index(date_str) + 1
            sheet.values().update(
                spreadsheetId=spreadsheet_id,
                range=f'A{row_idx}:C{row_idx}',
                valueInputOption='RAW',
                body={'values': [[date_str, section73_volume, swaps_volume]]}
            ).execute()
            print(f"Updated existing row {row_idx} for {date_str}")
        else:
            # Append new row
            sheet.values().append(
                spreadsheetId=spreadsheet_id,
                range='A:C',
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body={'values': [[date_str, section73_volume, swaps_volume]]}
            ).execute()
            print(f"Appended new row for {date_str}")

        print(f"Successfully wrote to Google Sheet: {date_str}, {section73_volume:,}, {swaps_volume:,}")
        return True

    except HttpError as e:
        print(f"Google Sheets API error: {e}")
        return False
    except Exception as e:
        print(f"Error writing to Google Sheet: {e}")
        return False


def main():
    """Main entry point."""
    # Get spreadsheet ID from environment or command line
    spreadsheet_id = os.environ.get('CME_SPREADSHEET_ID')
    if len(sys.argv) > 1:
        spreadsheet_id = sys.argv[1]

    if not spreadsheet_id:
        print("Error: No spreadsheet ID provided.")
        print("Usage: python3 cme_event_contracts.py <spreadsheet_id>")
        print("Or set CME_SPREADSHEET_ID environment variable")
        sys.exit(1)

    # Create temp directory for PDFs
    tmp_dir = Path('/tmp/cme_pdfs')
    tmp_dir.mkdir(exist_ok=True)

    section73_path = tmp_dir / 'section73.pdf'
    swaps_path = tmp_dir / 'swaps.pdf'

    # Download PDFs
    print("Downloading CME Event Contracts PDFs...")
    if not download_pdf(SECTION73_URL, str(section73_path)):
        sys.exit(1)
    if not download_pdf(SWAPS_URL, str(swaps_path)):
        sys.exit(1)

    # Extract volumes
    print("\nExtracting volume data...")
    section73_volume = extract_section73_volume(str(section73_path))
    swaps_volume = extract_swaps_volume(str(swaps_path))

    # Extract date from Section 73 PDF
    report_date = None
    try:
        with pdfplumber.open(str(section73_path)) as pdf:
            text = pdf.pages[0].extract_text()
            # Look for date pattern like "Fri, Jan 16, 2026"
            import re
            match = re.search(r'(Mon|Tue|Wed|Thu|Fri),\s+(\w+)\s+(\d+),\s+(\d{4})', text)
            if match:
                month_map = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
                            'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                            'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
                month = month_map.get(match.group(2), '01')
                day = match.group(3).zfill(2)
                year = match.group(4)
                report_date = f"{year}-{month}-{day}"
                print(f"Report date from PDF: {report_date}")
    except Exception as e:
        print(f"Could not extract date from PDF: {e}")

    # Fallback to today if extraction failed
    if not report_date:
        report_date = datetime.now().strftime('%Y-%m-%d')
        print(f"Using today's date: {report_date}")

    # Write to Google Sheet
    print(f"\nWriting to Google Sheet...")
    if write_to_google_sheet(spreadsheet_id, section73_volume, swaps_volume, report_date):
        print("\nDone!")
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
