#!/usr/bin/env python3
"""
Debug script to test SEC EDGAR downloads and diagnose issues
"""

import os
import sys
from pathlib import Path
from sec_edgar_downloader import Downloader
import logging
import requests

import json

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def test_sec_connection():
    """Test basic connection to SEC EDGAR"""
    print("\n1. Testing SEC EDGAR connection...")

    headers = {
        'User-Agent': f"{os.environ.get('SEC_COMPANY', 'Test Company')} {os.environ.get('SEC_EMAIL', 'test@example.com')}"
    }

    try:
        # Test connection to SEC
        response = requests.get("https://www.sec.gov/", headers=headers, timeout=10)
        print(f"   ✓ SEC website accessible (status: {response.status_code})")

        # Test EDGAR API
        response = requests.get("https://data.sec.gov/api/xbrl/companyconcept/CIK0000789019/us-gaap/Assets.json",
                                headers=headers, timeout=10)
        print(f"   ✓ SEC EDGAR API accessible (status: {response.status_code})")

        return True
    except Exception as e:
        print(f"   ✗ Connection error: {str(e)}")
        return False


def test_cik_info(cik):
    """Get information about a specific CIK"""
    print(f"\n2. Checking CIK {cik} information...")

    cik = str(cik).zfill(10)
    headers = {
        'User-Agent': f"{os.environ.get('SEC_COMPANY', 'Test Company')} {os.environ.get('SEC_EMAIL', 'test@example.com')}"
    }

    try:
        # Get company information
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code == 200:
            data = response.json()
            print(f"   ✓ Company found: {data.get('name', 'Unknown')}")
            print(f"   ✓ Tickers: {', '.join(data.get('tickers', [])) or 'None'}")
            print(f"   ✓ SIC: {data.get('sic', 'Unknown')} - {data.get('sicDescription', '')}")

            # Check recent filings
            if 'filings' in data and 'recent' in data['filings']:
                recent = data['filings']['recent']
                form_types = recent.get('form', [])
                filing_dates = recent.get('filingDate', [])

                # Count 10-K filings
                ten_k_count = sum(1 for ft in form_types if ft == '10-K')
                print(f"   ✓ Total 10-K filings: {ten_k_count}")

                # Show recent 10-Ks
                if ten_k_count > 0:
                    print("   ✓ Recent 10-K filings:")
                    for i, (form, date) in enumerate(zip(form_types[:50], filing_dates[:50])):
                        if form == '10-K':
                            print(f"      - {date}")
                            if i > 5:  # Show max 5 recent ones
                                break

            return True
        else:
            print(f"   ✗ CIK not found (status: {response.status_code})")
            return False

    except Exception as e:
        print(f"   ✗ Error getting CIK info: {str(e)}")
        return False


def test_download(cik, start_date="2020-01-01", end_date="2024-12-31"):
    """Test downloading filings for a specific CIK"""
    print(f"\n3. Testing download for CIK {cik}...")

    cik = str(cik).zfill(10)
    download_dir = os.environ.get('EDGAR_DOWNLOAD_DIR', 'test_downloads')

    try:
        # Create downloader
        dl = Downloader(
            company_name=os.environ.get('SEC_COMPANY', 'Test Company'),
            email_address=os.environ.get('SEC_EMAIL', 'test@example.com')
        )

        # Set download directory
        dl.dir = Path(download_dir)
        print(f"   Download directory: {download_dir}")

        # Try to download
        print(f"   Attempting to download 10-K filings from {start_date} to {end_date}...")
        dl.get("10-K", cik, after=start_date, before=end_date)

        # Check what was downloaded
        cik_dir = Path(download_dir) / "sec-edgar-filings" / cik / "10-K"

        if cik_dir.exists():
            filing_folders = list(cik_dir.iterdir())
            print(f"   ✓ Downloaded {len(filing_folders)} filings")

            # Show what was downloaded
            for folder in filing_folders[:5]:  # Show first 5
                files = list(folder.iterdir())
                print(f"      - {folder.name}: {len(files)} files")
                for file in files[:3]:  # Show first 3 files
                    print(f"         • {file.name} ({file.stat().st_size:,} bytes)")

            return True
        else:
            print(f"   ✗ No filings downloaded (directory doesn't exist)")
            return False

    except Exception as e:
        print(f"   ✗ Download error: {str(e)}")
        print(f"   Error type: {type(e).__name__}")
        return False


def test_multiple_ciks(cik_list):
    """Test multiple CIKs to see patterns"""
    print(f"\n4. Testing multiple CIKs...")

    success_count = 0
    for cik in cik_list:
        try:
            cik = str(cik).zfill(10)
            headers = {
                'User-Agent': f"{os.environ.get('SEC_COMPANY', 'Test Company')} {os.environ.get('SEC_EMAIL', 'test@example.com')}"
            }

            # Quick check if CIK has 10-K filings
            url = f"https://data.sec.gov/submissions/CIK{cik}.json"
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                data = response.json()
                name = data.get('name', 'Unknown')

                # Count 10-Ks
                ten_k_count = 0
                if 'filings' in data and 'recent' in data['filings']:
                    form_types = data['filings']['recent'].get('form', [])
                    ten_k_count = sum(1 for ft in form_types if ft == '10-K')

                status = "✓" if ten_k_count > 0 else "✗"
                print(f"   {status} CIK {cik}: {name[:40]:<40} - {ten_k_count} 10-Ks")

                if ten_k_count > 0:
                    success_count += 1
            else:
                print(f"   ✗ CIK {cik}: Not found")

        except Exception as e:
            print(f"   ✗ CIK {cik}: Error - {str(e)}")

    print(f"\nSummary: {success_count}/{len(cik_list)} CIKs have 10-K filings")


def main():
    """Run diagnostic tests"""
    print("SEC EDGAR Download Diagnostic Tool")
    print("=" * 50)

    # Check environment
    print("\nEnvironment Configuration:")
    print(f"SEC_EMAIL: {os.environ.get('SEC_EMAIL', 'NOT SET')}")
    print(f"SEC_COMPANY: {os.environ.get('SEC_COMPANY', 'NOT SET')}")
    print(f"EDGAR_DOWNLOAD_DIR: {os.environ.get('EDGAR_DOWNLOAD_DIR', 'NOT SET')}")

    # Test connection
    if not test_sec_connection():
        print("\nConnection failed. Check your internet connection and SEC access.")
        return

    # Test specific CIK (use a known good one like Microsoft)
    test_cik = "0000789019"  # Microsoft
    if test_cik_info(test_cik):
        test_download(test_cik, "2022-01-01", "2024-12-31")

    # Test some bank CIKs (SIC 6021)
    print("\n" + "=" * 50)
    print("Testing sample bank CIKs (SIC 6021):")
    print("=" * 50)

    # Sample bank CIKs
    bank_ciks = [
        "0000036104",  # Bank of America
        "0000019617",  # JPMorgan Chase
        "0000831001",  # Wells Fargo
        "0000903419",  # Alerus Financial
        "0001576336",  # Some smaller bank
    ]

    test_multiple_ciks(bank_ciks)

    # If you have a specific CIK that's failing, test it
    if len(sys.argv) > 1:
        test_cik = sys.argv[1]
        print(f"\n{'=' * 50}")
        print(f"Testing specific CIK: {test_cik}")
        print("=" * 50)

        if test_cik_info(test_cik):
            test_download(test_cik)


if __name__ == "__main__":
    # Set your credentials here for testing
    os.environ['SEC_EMAIL'] = 'danielburke0920@gmail.com'
    os.environ['SEC_COMPANY'] = 'Personal Research'
    os.environ['EDGAR_DOWNLOAD_DIR'] = 'D:/10-k forms/TEST SIC'

    main()