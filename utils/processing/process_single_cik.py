# utils/processing/process_single_cik.py
"""
Process all 10-K filings for a single CIK
"""
import logging
from pathlib import Path
import os

logger = logging.getLogger(__name__)


def process_single_cik(cik):
    """
    Process all 10-K filings for a single CIK

    Args:
        cik (str): Central Index Key

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Ensure CIK is properly formatted
        cik = str(cik).strip().zfill(10)
        logger.info(f"Starting processing for CIK: {cik}")

        # Get download directory
        download_dir = Path(os.environ.get('EDGAR_DOWNLOAD_DIR', 'data'))

        # Files are saved under: download_dir/sec-edgar-filings/CIK/10-K/
        cik_dir = download_dir / "sec-edgar-filings" / cik / "10-K"

        if not cik_dir.exists():
            logger.warning(f"No 10-K directory found for CIK {cik} at {cik_dir}")
            return False

        # Find all 10-K filing folders
        filing_folders = [f for f in cik_dir.iterdir() if f.is_dir()]

        if not filing_folders:
            logger.warning(f"No 10-K filing folders found for CIK {cik}")
            return False

        logger.info(f"Found {len(filing_folders)} 10-K filings for CIK {cik}")

        processed_count = 0
        for filing_folder in filing_folders:
            try:
                # Each filing folder contains the actual filing files
                # Look for HTML or TXT files
                filing_files = list(filing_folder.glob("*.html")) + \
                               list(filing_folder.glob("*.htm")) + \
                               list(filing_folder.glob("*.txt"))

                if filing_files:
                    logger.debug(f"Processing filing in {filing_folder.name}")
                    # Here you would call your MDA extraction logic
                    # For now, just mark as processed
                    processed_count += 1
                else:
                    logger.warning(f"No filing files found in {filing_folder}")

            except Exception as e:
                logger.error(f"Error processing filing folder {filing_folder}: {str(e)}")
                continue

        if processed_count > 0:
            logger.info(f"Successfully processed {processed_count} filings for CIK {cik}")
            return True
        else:
            logger.warning(f"No filings were successfully processed for CIK {cik}")
            return False

    except Exception as e:
        logger.error(f"Error processing CIK {cik}: {str(e)}")
        return False


# For backward compatibility
process_single_ticker = process_single_cik