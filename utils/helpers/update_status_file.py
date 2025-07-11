# utils/helpers/update_status_file.py
"""
Update the processing status for a ticker
"""
import csv
from datetime import datetime
from pathlib import Path
import filelock
import logging

logger = logging.getLogger(__name__)


def update_status_file(ticker, status='completed', error_msg=None):
    """
    Update the status for a specific ticker

    Args:
        ticker (str): Ticker symbol
        status (str, optional): New status (pending, processing, completed, failed, error)
        error_msg (str, optional): Error message if status is error
    """
    status_file = Path("processing_status.csv")
    lock_file = Path("processing_status.lock")

    try:
        with filelock.FileLock(lock_file, timeout=10):
            # Read all rows
            rows = []
            if status_file.exists():
                with open(status_file, 'r') as f:
                    reader = csv.reader(f)
                    rows = list(reader)

            # Update the specific ticker
            updated = False
            for i, row in enumerate(rows):
                if i > 0 and len(row) > 0 and row[0] == ticker:
                    # Ensure row has enough columns
                    while len(row) < 4:
                        row.append('')

                    row[1] = status
                    row[2] = datetime.now().isoformat()
                    row[3] = error_msg or ''
                    updated = True
                    break

            # If ticker not found, add it
            if not updated and len(rows) > 0:
                rows.append([ticker, status, datetime.now().isoformat(), error_msg or ''])

            # Write back
            with open(status_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(rows)

            logger.debug(f"Updated status for {ticker}: {status}")

    except filelock.Timeout:
        logger.error(f"Timeout waiting for status file lock for {ticker}")
    except Exception as e:
        logger.error(f"Error updating status file for {ticker}: {str(e)}")