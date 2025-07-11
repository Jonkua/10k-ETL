#!/usr/bin/env python3
"""
SEC EDGAR ETL by SIC Code
Main script to extract 10-K filings and MDA sections for all companies within specified SIC codes
"""

import json
import logging
import os
import sys
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from utils.get_10k_filing import get_ticker_10k_filing as get_cik_10k_filings
from utils.get_companies_by_sic import  get_companies_by_sic_code, get_all_sic_codes, get_companies_by_sic_list, validate_csv_format
from utils.processing.process_single_ticker import process_single_ticker
from utils.helpers.initialize_status_file import initialize_status_file
from utils.helpers.update_status_file import update_status_file
from utils.helpers.write_to_master_file import write_to_master_file
from utils.helpers.delete_processed_folder import delete_processed_folder

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sic_processing.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Suppress pyrate_limiter logs
logging.getLogger('pyrate_limiter').setLevel(logging.WARNING)


class SECEdgarSICProcessor:
    """Process SEC EDGAR filings by SIC code"""

    def __init__(self, sic_codes, start_date=None, end_date=None, max_workers=5,
                 csv_file="company_database.csv", require_ticker=False, use_cik=True):
        """
        Initialize the processor

        Args:
            sic_codes (list): List of SIC codes to process
            start_date (str): Start date for filings (YYYY-MM-DD)
            end_date (str): End date for filings (YYYY-MM-DD)
            max_workers (int): Maximum number of concurrent workers
            csv_file (str): Path to CSV file containing company data
            require_ticker (bool): If True, only process companies with tickers
            use_cik (bool): If True, use CIK for downloads instead of ticker
        """
        self.sic_codes = sic_codes if isinstance(sic_codes, list) else [sic_codes]
        self.start_date = start_date or "2020-01-01"
        self.end_date = end_date or datetime.now().strftime("%Y-%m-%d")
        self.max_workers = max_workers
        self.csv_file = csv_file
        self.require_ticker = require_ticker
        self.use_cik = use_cik

        self.data_dir = Path("data")
        self.sic_data_dir = Path("sic_data")

        self.data_dir.mkdir(exist_ok=True)
        self.sic_data_dir.mkdir(exist_ok=True)

        os.environ['EDGAR_START_DATE'] = self.start_date
        os.environ['EDGAR_END_DATE'] = self.end_date

        self.data_dir = Path(os.environ.get('EDGAR_DOWNLOAD_DIR', 'data'))

        logging.info(f"Initialized processor for SIC codes: {self.sic_codes}")
        logging.info(f"Date range: {self.start_date} to {self.end_date}")
        logging.info(f"Using CSV file: {self.csv_file}")
        logging.info(f"Download directory: {self.data_dir}")
        logging.info(f"Require ticker: {self.require_ticker}")
        logging.info(f"Use CIK for downloads: {self.use_cik}")

    def get_all_companies_for_sics(self):
        if not validate_csv_format(self.csv_file):
            logging.error(f"Invalid CSV file format: {self.csv_file}")
            return {}

        if not self.sic_codes or self.sic_codes == ['ALL']:
            logging.info("No specific SIC codes provided, processing all SIC codes in CSV")
            self.sic_codes = get_all_sic_codes(self.csv_file)
            logging.info(f"Found {len(self.sic_codes)} unique SIC codes to process")

        all_companies = get_companies_by_sic_list(self.sic_codes, self.csv_file)

        for sic_code, companies in all_companies.items():
            if companies:
                sic_file = self.sic_data_dir / f"sic_{sic_code}_companies.json"
                with open(sic_file, 'w') as f:
                    json.dump(companies, f, indent=2)
                logging.info(f"Found {len(companies)} companies for SIC {sic_code}")
            else:
                logging.warning(f"No companies found for SIC code: {sic_code}")

        return all_companies

    def process_single_company(self, company_info, sic_code):
        """Process a single company with rate limiting"""
        cik = company_info['cik']
        ticker = company_info.get('ticker', '')
        company_name = company_info.get('company_name', 'Unknown')

        # Use CIK as identifier if no ticker or if use_cik is True
        identifier = cik if (self.use_cik or not ticker) else ticker
        identifier_type = "CIK" if (self.use_cik or not ticker) else "ticker"

        result = {
            'cik': cik,
            'ticker': ticker,
            'company_name': company_name,
            'sic_code': sic_code,
            'status': 'pending'
        }

        try:
            logging.info(f"Processing {company_name} ({identifier_type}: {identifier}) from SIC {sic_code}")

            # Add delay before each download to avoid rate limiting
            time.sleep(0.5)  # Half second delay between requests

            # Download using CIK
            download_success = get_cik_10k_filings(cik)

            if download_success:
                # Add small delay before processing
                time.sleep(0.1)

                # Process using CIK directory structure
                process_result = process_single_ticker(cik)

                if process_result:
                    result['status'] = 'success'
                    update_status_file(identifier, 'completed')
                    delete_processed_folder(cik)
                else:
                    result['status'] = 'processing_failed'
                    update_status_file(identifier, 'failed')
            else:
                result['status'] = 'download_failed'
                update_status_file(identifier, 'download_failed')

        except Exception as e:
            logging.error(f"Error processing {company_name} (CIK: {cik}): {str(e)}")
            result['status'] = 'error'
            result['error'] = str(e)

            # Handle specific error for update_status_file
            try:
                update_status_file(identifier, 'error', str(e))
            except:
                # If update_status_file fails, just log it
                logging.error(f"Failed to update status file for {identifier}")

        return result

    def process_all_sic_codes(self):
        all_companies = self.get_all_companies_for_sics()

        if not all_companies:
            logging.error("No companies found for any SIC code")
            return

        # Filter companies based on ticker requirement
        filtered_companies = {}
        total_before_filter = 0
        total_after_filter = 0

        for sic_code, companies in all_companies.items():
            total_before_filter += len(companies)

            if self.require_ticker:
                # Only include companies with tickers
                companies_filtered = [c for c in companies if c.get('ticker')]
            else:
                # Include all companies
                companies_filtered = companies

            total_after_filter += len(companies_filtered)

            if companies_filtered:
                filtered_companies[sic_code] = companies_filtered
                logging.info(f"SIC {sic_code}: {len(companies_filtered)} companies to process")

        logging.info(f"Total companies before filtering: {total_before_filter}")
        logging.info(f"Total companies after filtering: {total_after_filter}")

        if not filtered_companies:
            logging.error("No companies to process after filtering")
            return

        # Initialize status file with identifiers
        all_identifiers = []
        for sic_code, companies in filtered_companies.items():
            for company in companies:
                if self.use_cik:
                    all_identifiers.append(company['cik'])
                else:
                    identifier = company.get('ticker') or company['cik']
                    all_identifiers.append(identifier)

        initialize_status_file(all_identifiers)

        all_results = []

        # Process sequentially to avoid rate limiting
        for sic_code, companies in filtered_companies.items():
            logging.info(f"\n{'=' * 50}")
            logging.info(f"Processing SIC code: {sic_code}")
            logging.info(f"Total companies: {len(companies)}")
            logging.info(f"{'=' * 50}\n")

            # Process companies one by one with delays
            for i, company in enumerate(companies):
                logging.info(f"Processing company {i + 1}/{len(companies)} for SIC {sic_code}")
                result = self.process_single_company(company, sic_code)
                all_results.append(result)

                # Longer delay every 10 companies
                if (i + 1) % 10 == 0:
                    logging.info("Pausing for 10 seconds to avoid rate limiting...")
                    time.sleep(10)

            # Delay between SIC codes
            if sic_code != list(filtered_companies.keys())[-1]:  # Not the last SIC
                logging.info(f"Completed SIC {sic_code}, pausing before next SIC code...")
                time.sleep(5)

        self.write_summary_results(all_results)

        logging.info("Creating master file with all MDA data...")

        # Call write_to_master_file
        write_to_master_file(master_df)

        logging.info("\n" + "=" * 50)
        logging.info("Processing complete!")
        logging.info(f"Total companies processed: {len(all_results)}")
        logging.info(f"Successful: {sum(1 for r in all_results if r['status'] == 'success')}")
        logging.info(f"Failed: {sum(1 for r in all_results if r['status'] != 'success')}")
        logging.info("=" * 50)

    def write_summary_results(self, results):
        summary_file = self.sic_data_dir / f"sic_processing_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        with open(summary_file, 'w') as f:
            json.dump({
                'sic_codes': self.sic_codes,
                'date_range': {
                    'start': self.start_date,
                    'end': self.end_date
                },
                'total_companies': len(results),
                'successful': sum(1 for r in results if r['status'] == 'success'),
                'failed': sum(1 for r in results if r['status'] != 'success'),
                'require_ticker': self.require_ticker,
                'use_cik': self.use_cik,
                'results': results
            }, f, indent=2)

        logging.info(f"Summary results saved to: {summary_file}")


def main():
    # SEC credentials
    os.environ['SEC_EMAIL'] = 'danielburke0920@gmail.com'
    os.environ['SEC_COMPANY'] = 'Personal Research'
    os.environ['EDGAR_DOWNLOAD_DIR'] = 'D:/10-k forms/TEST SIC'

    # Configuration
    csv_file = "cik_sic_history.csv"

    # SIC codes to process
    sic_codes = ['6021']

    # Date range
    start_date = "1994-01-01"
    end_date = "2024-12-31"

    # Processing options
    REQUIRE_TICKER = False  # Set to False to process ALL companies (even without tickers)
    USE_CIK = True  # Set to True to use CIK for downloads (more reliable)

    if not Path(csv_file).exists():
        logging.error(f"CSV file not found: {csv_file}")
        print(f"\nERROR: Could not find CSV file: {csv_file}")
        print("Expected format: cik,company_name,sic,industry,years_active")
        return

    # Create processor
    processor = SECEdgarSICProcessor(
        sic_codes=sic_codes,
        start_date=start_date,
        end_date=end_date,
        max_workers=2,  # Keep at 1 to avoid parallel rate limiting
        csv_file=csv_file,
        require_ticker=REQUIRE_TICKER,  # Process all companies or only those with tickers
        use_cik=USE_CIK  # Use CIK instead of ticker for downloads
    )

    try:
        processor.process_all_sic_codes()
    except KeyboardInterrupt:
        logging.info("\nProcessing interrupted by user")
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()