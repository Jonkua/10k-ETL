"""
Utility to fetch companies by SIC code from a CSV file
Replaces the hardcoded mappings with CSV-based lookup
"""

import csv
import logging
from pathlib import Path
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


def get_companies_by_sic_code(sic_code: str, csv_file: str = "cik_sic_history.csv") -> List[Dict]:
    """
    Get all companies with a specific SIC code from CSV file

    Args:
        sic_code (str): The SIC code to search for
        csv_file (str): Path to the CSV file containing company data

    Returns:
        List[Dict]: List of companies with ticker, CIK, and company name
    """
    # Convert to string and ensure it's 4 digits
    sic_code = str(sic_code).zfill(4)
    companies = []

    csv_path = Path(csv_file)
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_file}")
        return companies

    logger.info(f"Reading companies from {csv_file} for SIC code: {sic_code}")

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            # Map expected columns to actual columns (case-insensitive)
            column_mapping = _get_column_mapping(reader.fieldnames)

            if not column_mapping:
                return companies

            for row in reader:
                # Compare SIC codes
                row_sic = str(row.get(column_mapping['sic'], '')).strip()

                # Handle SIC codes that might not be 4 digits
                if row_sic:
                    if len(row_sic) < 4 and row_sic.isdigit():
                        row_sic = row_sic.zfill(4)

                    if row_sic == sic_code:
                        # Extract CIK and ensure it's 10 digits
                        cik = str(row.get(column_mapping['cik'], '')).strip()
                        if cik and cik.isdigit():
                            cik = cik.zfill(10)

                        company_name = row.get(column_mapping['company_name'], '').strip()
                        years_active = row.get(column_mapping.get('years_active', ''), '').strip()
                        industry = row.get(column_mapping.get('industry', ''), '').strip()

                        # Try to extract ticker from company name if it's in format "TICKER - Company Name"
                        ticker = ''
                        if ' - ' in company_name:
                            parts = company_name.split(' - ', 1)
                            potential_ticker = parts[0].strip()
                            # Check if it looks like a ticker (1-5 uppercase letters)
                            if potential_ticker.isupper() and len(potential_ticker) <= 5:
                                ticker = potential_ticker
                                company_name = parts[1].strip()

                        # If no ticker found, try to use first word if it's all caps
                        if not ticker:
                            first_word = company_name.split()[0] if company_name else ''
                            if first_word.isupper() and len(first_word) <= 5:
                                ticker = first_word

                        companies.append({
                            'cik': cik,
                            'ticker': ticker,
                            'company_name': company_name,
                            'years_active': years_active,
                            'sic': row_sic,
                            'industry': industry
                        })

        logger.info(f"Found {len(companies)} companies for SIC {sic_code}")

    except Exception as e:
        logger.error(f"Error reading CSV file: {str(e)}")

    return companies


def get_all_sic_codes(csv_file: str = "cik_sic_history.csv") -> List[str]:
    """
    Get all unique SIC codes from the CSV file

    Args:
        csv_file (str): Path to the CSV file

    Returns:
        List[str]: List of unique SIC codes
    """
    sic_codes = set()

    csv_path = Path(csv_file)
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_file}")
        return []

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            # Map columns
            column_mapping = _get_column_mapping(reader.fieldnames)
            if not column_mapping:
                return []

            sic_column = column_mapping['sic']

            for row in reader:
                sic = str(row.get(sic_column, '')).strip()
                if sic and sic.isdigit():
                    sic_codes.add(sic.zfill(4))

        logger.info(f"Found {len(sic_codes)} unique SIC codes in {csv_file}")

    except Exception as e:
        logger.error(f"Error reading CSV file: {str(e)}")

    return sorted(list(sic_codes))


def get_companies_by_sic_list(sic_codes: List[str], csv_file: str = "cik_sic_history.csv") -> Dict[str, List[Dict]]:
    """
    Get companies for multiple SIC codes at once (more efficient)

    Args:
        sic_codes (List[str]): List of SIC codes to search for
        csv_file (str): Path to the CSV file

    Returns:
        Dict[str, List[Dict]]: Dictionary mapping SIC codes to lists of companies
    """
    # Normalize SIC codes
    sic_codes_set = {str(code).zfill(4) for code in sic_codes}
    results = {sic: [] for sic in sic_codes_set}

    csv_path = Path(csv_file)
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_file}")
        return results

    logger.info(f"Reading companies from {csv_file} for {len(sic_codes)} SIC codes")

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            # Map columns
            column_mapping = _get_column_mapping(reader.fieldnames)
            if not column_mapping:
                return results

            for row in reader:
                row_sic = str(row.get(column_mapping['sic'], '')).strip()

                # Handle SIC codes that might not be 4 digits
                if row_sic and row_sic.isdigit():
                    row_sic = row_sic.zfill(4)

                if row_sic in sic_codes_set:
                    cik = str(row.get(column_mapping['cik'], '')).strip()
                    if cik and cik.isdigit():
                        cik = cik.zfill(10)

                    company_name = row.get(column_mapping['company_name'], '').strip()
                    years_active = row.get(column_mapping.get('years_active', ''), '').strip()
                    industry = row.get(column_mapping.get('industry', ''), '').strip()

                    # Extract ticker logic (same as above)
                    ticker = ''
                    if ' - ' in company_name:
                        parts = company_name.split(' - ', 1)
                        potential_ticker = parts[0].strip()
                        if potential_ticker.isupper() and len(potential_ticker) <= 5:
                            ticker = potential_ticker
                            company_name = parts[1].strip()

                    if not ticker:
                        first_word = company_name.split()[0] if company_name else ''
                        if first_word.isupper() and len(first_word) <= 5:
                            ticker = first_word

                    results[row_sic].append({
                        'cik': cik,
                        'ticker': ticker,
                        'company_name': company_name,
                        'years_active': years_active,
                        'sic': row_sic,
                        'industry': industry
                    })

        # Log summary
        total_companies = sum(len(companies) for companies in results.values())
        logger.info(f"Found {total_companies} total companies across {len(sic_codes)} SIC codes")

    except Exception as e:
        logger.error(f"Error reading CSV file: {str(e)}")

    return results


def validate_csv_format(csv_file: str = "cik_sic_history.csv") -> bool:
    """
    Validate that the CSV file has the expected format

    Args:
        csv_file (str): Path to the CSV file

    Returns:
        bool: True if valid, False otherwise
    """
    csv_path = Path(csv_file)
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_file}")
        return False

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            # Check if we can map the columns
            column_mapping = _get_column_mapping(reader.fieldnames)
            if not column_mapping:
                return False

            # Check if there's at least one row of data
            try:
                next(reader)
                return True
            except StopIteration:
                logger.error("CSV file is empty (no data rows)")
                return False

    except Exception as e:
        logger.error(f"Error validating CSV file: {str(e)}")
        return False


def _get_column_mapping(fieldnames: List[str]) -> Optional[Dict[str, str]]:
    """
    Create a mapping from expected column names to actual column names in the CSV
    Handles case-insensitive matching and common variations

    Args:
        fieldnames: List of actual column names from the CSV

    Returns:
        Dict mapping expected names to actual names, or None if required columns missing
    """
    if not fieldnames:
        logger.error("No column headers found in CSV")
        return None

    # Create lowercase version of fieldnames for matching
    fieldnames_lower = {name.lower(): name for name in fieldnames}

    # Define required columns and their variations
    column_variations = {
        'cik': ['cik', 'c.i.k.', 'c_i_k', 'central_index_key'],
        'company_name': ['company_name', 'co_name', 'name', 'company', 'companyname', 'co name'],
        'sic': ['sic', 's.i.c.', 's_i_c', 'sic_code', 'siccode', 'standard_industrial_classification'],
        'years_active': ['years_active', 'yearsactive', 'years', 'active_years', 'year_active'],
        'industry': ['industry', 'industry_name', 'sic_description', 'sector']
    }

    # Find matches
    mapping = {}
    missing_required = []

    for expected, variations in column_variations.items():
        found = False
        for variation in variations:
            if variation.lower() in fieldnames_lower:
                mapping[expected] = fieldnames_lower[variation.lower()]
                found = True
                break

        # Only CIK, company_name, and SIC are truly required
        if not found and expected in ['cik', 'company_name', 'sic']:
            missing_required.append(expected)

    if missing_required:
        logger.error(f"CSV missing required columns: {missing_required}")
        logger.error(f"Available columns: {fieldnames}")
        logger.info("Acceptable column names:")
        for expected, variations in column_variations.items():
            if expected in ['cik', 'company_name', 'sic']:
                logger.info(f"  {expected}: {variations}")
        return None

    logger.debug(f"Column mapping: {mapping}")
    return mapping


# For backward compatibility with the original interface
get_companies_by_sic_code_fast = get_companies_by_sic_code