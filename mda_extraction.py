#!/usr/bin/env python3
"""
MD&A Section Extractor for SEC 10-K Filings

This program navigates a structured directory of previously downloaded 10-K filings,
extracts the Management's Discussion & Analysis (MD&A) section from HTML or TXT sources,
and outputs the extracted content as PDF or text files in a separate directory.

Requirements:
    pip install beautifulsoup4 pdfkit lxml
    and install wkhtmltopdf (https://wkhtmltopdf.org/downloads.html)
"""
import os
import re
import logging
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup, Tag, NavigableString
from lxml import html as lxml_html
import pdfkit

# ==== CONFIGURATION ====
INPUT_ROOT = Path("D:/10-k forms/TEST SIC/0000832847_0000832847")
OUTPUT_ROOT = Path("D:/10-k forms/MDNA_EXTRACTED")
LOG_FILE = "mdna_extractor.log"
WKHTMLTOPDF_PATH = r"C:\\Program Files\\wkhtmltopdf\\bin\\wkhtmltopdf.exe"
pdfkit_config = pdfkit.configuration(wkhtmltopdf=WKHTMLTOPDF_PATH)
pdfkit_options = {
    'no-images': '',
    'disable-javascript': '',
    'load-error-handling': 'ignore',
    'load-media-error-handling': 'ignore',
    'quiet': '',
    'page-size': 'A4',
    'margin-top': '0.75in',
    'margin-right': '0.75in',
    'margin-bottom': '0.75in',
    'margin-left': '0.75in',
    'encoding': "UTF-8",
    'no-outline': None
}

# ==== LOGGING SETUP ====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8')
    ]
)

# ==== REGEX PATTERNS ====
START_PATTERNS = [
    re.compile(r"Item\s*7\.?", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[-–—:]", re.IGNORECASE),
    re.compile(r"Item\s*7\s*\.", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[:\-–—]+\s*Management", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[:\-–—]+\s*Management'?s?\s+Discussion", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[:\-–—]+\s*Management'?s?\s+Discussion\s+and\s+Analysis", re.IGNORECASE),
    re.compile(r"Item\s*7\s+Management'?s?\s+Discussion\s+and\s+Analysis", re.IGNORECASE),
    re.compile(r"Item\s*7\s*Management'?s?\s+Discussion", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[:\-–—]*\s*MD&A", re.IGNORECASE),
    re.compile(r"Item\s+7\s+MD&A", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[-–—]*\s*Management'?s\s+Discussion\s+and\s+Analysis\s+of\s+Financial\s+Condition",
               re.IGNORECASE),
    re.compile(r"Item\s*Seven", re.IGNORECASE),
    re.compile(r"Item\s*Seven\s*[-–—:]?", re.IGNORECASE),
    re.compile(r"Item\s*Seven\s*Management'?s?\s+Discussion", re.IGNORECASE),
    re.compile(r"Item\s*7\s*Management'?s?\s+Discussion\s+and\s+Analysis\s+of\s+Financial\s+Condition", re.IGNORECASE),
    re.compile(r"Item\s*7\s*Management'?s?\s+Discussion\s+and\s+Results\s+of\s+Operations", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[:\-–—]?\s*Discussion\s+and\s+Analysis", re.IGNORECASE),
    re.compile(r"Item\s*7\s*-\s*MDA", re.IGNORECASE),
    re.compile(r"Item\s*7\s*\n*\s*Management", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[\.\-–—:]?\s*<[^>]+>\s*Management'?s?\s+Discussion", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[\.\-–—:]?\s*(?:\s|\xa0|&#160;)*Management'?s?\s+Discussion", re.IGNORECASE),
    re.compile(r"Item\s*7\s*(of\s+Form\s+10-K)?", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[:\-–—]*\s*Overview", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[:\-–—]*\s*Operating\s+Results", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[:\-–—]*\s*Results\s+of\s+Operations", re.IGNORECASE),
    re.compile(r"Item\s*7\s+–\s+Management", re.IGNORECASE),
    re.compile(r"Item\s+7\s+–\s+Discussion", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[:\-–—]*\s*Discussion\s+and\s+Financial", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[:\-–—]*\s*Business\s+Overview", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[-–—]*\s*Review\s+of\s+Operations", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[-–—]*\s*Analysis\s+of\s+Financial\s+Condition", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[-–—]*\s*Operating\s+and\s+Financial\s+Review", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[:\-–—]*\s*Liquidity\s+and\s+Capital\s+Resources", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[:\-–—]*\s*Trends\s+and\s+Uncertainties", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[:\-–—]*\s*Forward[-–—]Looking\s+Statements", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[:\-–—]*\s*Critical\s+Accounting\s+Policies", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[:\-–—]*\s*Summary\s+of\s+Results", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[:\-–—]*\s*Segment\s+Information", re.IGNORECASE),
    re.compile(r"Item\s*7\s*[:\-–—]*\s*Company\s+Performance", re.IGNORECASE),
]

END_PATTERNS = [
    re.compile(r"Item\s*7A\.?", re.IGNORECASE),
    re.compile(r"Item\s*7\s*A\.?", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[-–—:]", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*\.", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[:\-–—]+\s*Quantitative", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[:\-–—]+\s*Quantitative\s+and\s+Qualitative", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*Quantitative\s+and\s+Qualitative\s+Disclosures", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*Quantitative\s+and\s+Qualitative\s+Disclosures\s+About\s+Market\s+Risk", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*Disclosures\s+About\s+Market\s+Risk", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[-–—]*\s*Market\s+Risk", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[-–—]*\s*Market\s+Risk\s+Disclosures", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[-–—]*\s*Qualitative\s+and\s+Quantitative", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[-–—]*\s*Risk\s+Disclosures", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[:\-–—]*\s*Risk\s+Factors", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[:\-–—]*\s*Sensitivity\s+Analysis", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[:\-–—]*\s*Interest\s+Rate\s+Risk", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[:\-–—]*\s*Foreign\s+Currency\s+Risk", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[:\-–—]*\s*Market\s+Volatility", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[:\-–—]*\s*Value\s+at\s+Risk", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[:\-–—]*\s*Quantitative\s+Market\s+Risk", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[:\-–—]*\s*Financial\s+Risk", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[\.\-–—:]?\s*(?:\s|\xa0|&#160;)*Quantitative\s+and\s+Qualitative", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[\.\-–—:]?\s*(?:\s|\xa0|&#160;)*Disclosures\s+About\s+Market\s+Risk", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[\.\-–—:]?\s*(?:<[^>]+>\s*)*Quantitative\s+and\s+Qualitative", re.IGNORECASE),
    re.compile(r"Item\s*7\s*A\s*Quantitative\s+and\s+Qualitative", re.IGNORECASE),
    re.compile(r"Item\s*7\s*A\s*Disclosures\s+About\s+Market\s+Risk", re.IGNORECASE),
    re.compile(r"Item\s*Seven\s*A", re.IGNORECASE),
    re.compile(r"Item\s*Seven\s*A\s*Quantitative\s+and\s+Qualitative", re.IGNORECASE),
    re.compile(r"Item\s*7A\s+–\s+Quantitative\s+and\s+Qualitative", re.IGNORECASE),
    re.compile(r"Item\s*7A\s+–\s+Market\s+Risk", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[:\-–—]*\s*Overview\s+of\s+Market\s+Risks", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[:\-–—]*\s*Exposure\s+to\s+Market\s+Risks", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[:\-–—]*\s*Price\s+Risk\s+Analysis", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[:\-–—]*\s*Interest\s+Rate\s+Sensitivity", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[:\-–—]*\s*Currency\s+Exchange\s+Rate\s+Risk", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[:\-–—]*\s*Risk\s+Analysis", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[:\-–—]*\s*Sensitivity\s+to\s+Market\s+Changes", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[:\-–—]*\s*Financial\s+Instrument\s+Risks", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[:\-–—]*\s*Hedging\s+Strategies", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[:\-–—]*\s*Commodities\s+Exposure", re.IGNORECASE),
    re.compile(r"Item\s*7A\s*[:\-–—]*\s*Sensitivity\s+Tables", re.IGNORECASE),
    re.compile(r"Item\s*8\.?", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[-–—:]", re.IGNORECASE),
    re.compile(r"Item\s*8\s*\.", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[:\-–—]+\s*Financial", re.IGNORECASE),
    re.compile(r"Item\s*8\s*Financial\s+Statements", re.IGNORECASE),
    re.compile(r"Item\s*8\s*Financial\s+Statements\s+and\s+Supplementary\s+Data", re.IGNORECASE),
    re.compile(r"Item\s*8\s*Financial\s+Statements\s+and\s+Data", re.IGNORECASE),
    re.compile(r"Item\s*8\s*Financial\s+Information", re.IGNORECASE),
    re.compile(r"Item\s*8\s*Consolidated\s+Financial\s+Statements", re.IGNORECASE),
    re.compile(r"Item\s*8\s+Audited\s+Financial\s+Statements", re.IGNORECASE),
    re.compile(r"Item\s*8\s*Financial\s+Statements\s+and\s+Notes", re.IGNORECASE),
    re.compile(r"Item\s*8\s*–\s*Financial", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[:\-–—]*\s*Statements\s+and\s+Data", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[:\-–—]*\s*Statement\s+of\s+Operations", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[:\-–—]*\s*Balance\s+Sheet", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[:\-–—]*\s*Cash\s+Flow\s+Statement", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[:\-–—]*\s*Income\s+Statement", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[:\-–—]*\s*Results\s+of\s+Operations", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[:\-–—]*\s*Quarterly\s+Data", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[:\-–—]*\s*Notes\s+to\s+Financial\s+Statements", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[:\-–—]*\s*Report\s+of\s+Independent", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[:\-–—]*\s*Supplementary\s+Data", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[:\-–—]*\s*Accounting\s+Policies", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[:\-–—]*\s*Significant\s+Accounting\s+Policies", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[:\-–—]*\s*Auditor", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[:\-–—]*\s*Independent\s+Audit", re.IGNORECASE),
    re.compile(r"Item\s*8\s+–\s+Audited\s+Statements", re.IGNORECASE),
    re.compile(r"Item\s*Eight", re.IGNORECASE),
    re.compile(r"Item\s*Eight\s*[-–—:]?", re.IGNORECASE),
    re.compile(r"Item\s*8\s+–\s+Consolidated\s+Statements", re.IGNORECASE),
    re.compile(r"Item\s*8\s+–\s+Audited\s+Consolidated\s+Financial\s+Statements", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[\.\-–—:]?\s*<[^>]+>\s*Financial\s+Statements", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[\.\-–—:]?\s*(?:\s|\xa0|&#160;)*Financial\s+Statements", re.IGNORECASE),
    re.compile(r"Item\s*8\s*Financial\s+Statements\s+and\s+Footnotes", re.IGNORECASE),
    re.compile(r"Item\s*8\s*Statements\s+of\s+Financial\s+Position", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[\.\-–—:]?\s*Annual\s+Financial\s+Statements", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[\.\-–—:]?\s*Financial\s+Statements\s+\(Audited\)", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[\.\-–—:]?\s*Required\s+Financial\s+Statements", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[:\-–—]*\s*Summary\s+Financial\s+Data", re.IGNORECASE),
    re.compile(r"Item\s*8\s*[:\-–—]*\s*Audited\s+Statements\s+and\s+Reports", re.IGNORECASE),
]

FORM_TYPE_PATTERN = re.compile(r"\bFORM\b\s*(10[-\s]*K(?:\s*/A)?)", re.IGNORECASE)
CIK_PATTERN = re.compile(r"cik_([0-9]{10})")


# ==== UTILITY FUNCTIONS ====
def extract_form_type(text: str) -> str:
    """Extract form type from document text"""
    m = FORM_TYPE_PATTERN.search(text)
    return m.group(1).replace(' ', '') if m else '10-K'


def clean_html_content(html_content: str) -> str:
    """Clean and normalize HTML content before parsing"""
    html_content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', html_content)
    html_content = html_content.replace('&nbsp;', ' ')
    html_content = html_content.replace('&#160;', ' ')
    html_content = html_content.replace('&#xa0;', ' ')
    html_content = html_content.replace('\xa0', ' ')
    html_content = html_content.replace('&amp;', '&')
    html_content = html_content.replace('&lt;', '<')
    html_content = html_content.replace('&gt;', '>')
    html_content = html_content.replace('&quot;', '"')
    html_content = html_content.replace('&#39;', "'")
    html_content = re.sub(r'[ \t]+', ' ', html_content)
    return html_content


def normalize_text_spacing(text: str) -> str:
    """Normalize spacing in extracted text while preserving structure"""
    text = re.sub(r'[ \t\xa0\u00a0\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]+', ' ', text)
    text = re.sub(r'\n\s*\n', '\n\n', text)
    text = re.sub(r'(?<!\n)\n(?!\n)', ' ', text)
    lines = [line.strip() for line in text.split('\n')]
    text = '\n'.join(lines)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def detect_tabular_content(element) -> bool:
    """Detect if an element contains tabular financial data"""
    text = element.get_text(strip=True)

    financial_indicators = [
        r'\$[\d,]+(?:\.\d+)?',
        r'\(\$?[\d,]+(?:\.\d+)?\)',
        r'\b\d{1,3}(?:,\d{3})*(?:\.\d+)?\b',
        r'\b\d+\.\d+%',
    ]

    indicator_count = sum(len(re.findall(pattern, text)) for pattern in financial_indicators)

    structure_indicators = [
        len(re.findall(r'\n\s*\n', text)) >= 2,
        len(re.findall(r'\s{5,}', text)) >= 3,
        any(word in text.lower() for word in ['december', 'year ended', 'three months', 'quarter']),
        '(' in text and ')' in text and indicator_count >= 3,
    ]

    return indicator_count >= 5 and any(structure_indicators)


def extract_tabular_data_from_text(text: str) -> list:
    """Extract tabular data from text content, detecting various formats"""
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    if len(lines) < 3:
        return []

    table_rows = []
    current_table = []

    for line in lines:
        is_table_row = (
                len(re.findall(r'\$[\d,]+(?:\.\d+)?', line)) >= 1 or
                len(re.findall(r'\b\d+(?:,\d{3})*(?:\.\d+)?\b', line)) >= 2 or
                len(re.findall(r'\s{3,}', line)) >= 2
        )

        if is_table_row or any(
                word in line.lower() for word in ['total', 'assets', 'liabilities', 'december', 'year ended']):
            current_table.append(line)
        else:
            if len(current_table) >= 3:
                table_rows.extend(current_table)
                table_rows.append('')
            current_table = []

    if len(current_table) >= 3:
        table_rows.extend(current_table)

    return table_rows


def extract_and_format_tables(soup_element) -> list:
    """Extract tables from HTML and format them for proper display - Enhanced version"""
    formatted_tables = []

    # Method 1: Traditional HTML tables
    tables = soup_element.find_all('table')
    for i, table in enumerate(tables):
        try:
            rows = []
            for tr in table.find_all('tr'):
                row = []
                for cell in tr.find_all(['td', 'th']):
                    cell_text = cell.get_text(strip=True)
                    cell_text = normalize_text_spacing(cell_text)

                    colspan = int(cell.get('colspan', 1))
                    rowspan = int(cell.get('rowspan', 1))

                    row.append({
                        'text': cell_text,
                        'colspan': colspan,
                        'rowspan': rowspan,
                        'is_header': cell.name == 'th'
                    })

                if row:
                    rows.append(row)

            if rows:
                formatted_tables.append({
                    'id': f'html_table_{i + 1}',
                    'rows': rows,
                    'original_html': str(table),
                    'type': 'html_table'
                })

        except Exception as e:
            logging.warning(f"Error processing HTML table {i + 1}: {e}")
            continue

    # Method 2: Detect tabular content in divs, paragraphs, or other elements
    potential_table_elements = soup_element.find_all(['div', 'p', 'pre', 'span'],
                                                     string=lambda text: text and
                                                                         (
                                                                                     '$' in text or 'Assets' in text or 'Liabilities' in text))

    for i, element in enumerate(potential_table_elements):
        try:
            check_element = element.parent if element.parent else element

            if detect_tabular_content(check_element):
                element_text = check_element.get_text()
                tabular_lines = extract_tabular_data_from_text(element_text)

                if tabular_lines:
                    table_rows = []
                    for line in tabular_lines:
                        if not line.strip():
                            continue

                        columns = re.split(r'\s{2,}|\t+', line.strip())
                        if len(columns) >= 2:
                            row = []
                            for j, col in enumerate(columns):
                                is_header = (
                                        j == 0 and not re.search(r'[\d$]', col) or
                                        any(word in col.lower() for word in ['total', 'assets', 'liabilities', 'december'])
                                )

                                row.append({
                                    'text': col.strip(),
                                    'colspan': 1,
                                    'rowspan': 1,
                                    'is_header': is_header
                                })

                            if row:
                                table_rows.append(row)

                    if table_rows:
                        formatted_tables.append({
                            'id': f'detected_table_{i + 1}',
                            'rows': table_rows,
                            'original_html': str(check_element),
                            'type': 'detected_table'
                        })

        except Exception as e:
            logging.warning(f"Error processing potential table element {i + 1}: {e}")
            continue

    # Method 3: Look for preformatted text that might be tables
    pre_elements = soup_element.find_all(['pre', 'code'])
    for i, pre in enumerate(pre_elements):
        try:
            pre_text = pre.get_text()
            if detect_tabular_content(pre):
                tabular_lines = extract_tabular_data_from_text(pre_text)

                if tabular_lines:
                    table_rows = []
                    for line in tabular_lines:
                        if not line.strip():
                            continue

                        columns = re.split(r'\s{3,}', line.strip())
                        if len(columns) >= 2:
                            row = []
                            for col in columns:
                                row.append({
                                    'text': col.strip(),
                                    'colspan': 1,
                                    'rowspan': 1,
                                    'is_header': not re.search(r'[\d$]', col)
                                })
                            table_rows.append(row)

                    if table_rows:
                        formatted_tables.append({
                            'id': f'pre_table_{i + 1}',
                            'rows': table_rows,
                            'original_html': str(pre),
                            'type': 'preformatted_table'
                        })

        except Exception as e:
            logging.warning(f"Error processing preformatted element {i + 1}: {e}")
            continue

    return formatted_tables


def format_plain_text_table(table_lines: list) -> list:
    """Format a detected table from plain text with better alignment"""
    if not table_lines:
        return []

    formatted = [
        '',
        '=' * 80,
        'TABLE:',
        '-' * 80
    ]

    for line in table_lines:
        parts = re.split(r'\s{2,}', line.strip())
        if len(parts) > 1:
            formatted_line = parts[0].ljust(30)
            for part in parts[1:]:
                formatted_line += part.rjust(15)
            formatted.append(formatted_line)
        else:
            formatted.append(line)

    formatted.extend([
        '-' * 80,
        ''
    ])

    return formatted


def create_html_table(table_data: dict) -> str:
    """Create properly formatted HTML table from extracted data"""
    table_type = table_data.get('type', 'html_table')
    html_parts = [f'<table class="financial-table {table_type}">']

    if table_type != 'html_table':
        html_parts.append('<caption>Financial Data Table</caption>')

    for row_idx, row in enumerate(table_data['rows']):
        is_header_row = any(cell['is_header'] for cell in row)
        tag = 'th' if is_header_row else 'td'

        html_parts.append('<tr>')

        for cell in row:
            attrs = []
            if cell['colspan'] > 1:
                attrs.append(f'colspan="{cell["colspan"]}"')
            if cell['rowspan'] > 1:
                attrs.append(f'rowspan="{cell["rowspan"]}"')

            attr_str = ' ' + ' '.join(attrs) if attrs else ''
            cell_text = cell['text']

            if re.match(r'^\$[\d,]+\.?\d*$', cell_text.replace('(', '').replace(')', '')):
                html_parts.append(f'<{tag} class="currency"{attr_str}>{cell_text}</{tag}>')
            elif re.match(r'^\([\$\d,]+\.?\d*\)$', cell_text):
                html_parts.append(f'<{tag} class="negative"{attr_str}>{cell_text}</{tag}>')
            elif re.match(r'^\d+\.?\d*%$', cell_text):
                html_parts.append(f'<{tag} class="percentage"{attr_str}>{cell_text}</{tag}>')
            elif re.match(r'^\d{1,3}(?:,\d{3})*(?:\.\d+)?$', cell_text):
                html_parts.append(f'<{tag} class="numeric"{attr_str}>{cell_text}</{tag}>')
            elif cell['is_header'] or is_header_row:
                html_parts.append(f'<{tag} class="header"{attr_str}>{cell_text}</{tag}>')
            else:
                html_parts.append(f'<{tag}{attr_str}>{cell_text}</{tag}>')

        html_parts.append('</tr>')

    html_parts.append('</table>')
    return '\n'.join(html_parts)


def extract_mdna_with_tables(soup, text_content: str) -> tuple:
    """Extract MD&A content while preserving ALL table structure"""
    item_7_matches = []
    for pattern in START_PATTERNS:
        matches = list(pattern.finditer(text_content))
        item_7_matches.extend([(match.start(), match.end(), match.group()) for match in matches])

    if not item_7_matches:
        return None, []

    item_7_matches.sort(key=lambda x: x[0])
    if len(item_7_matches) < 2:
        start_pos = item_7_matches[0][0]
    else:
        start_pos = item_7_matches[1][0]  # Skip TOC

    end_pos = None
    for pattern in END_PATTERNS:
        match = pattern.search(text_content, start_pos)
        if match:
            end_pos = match.start()
            break

    if end_pos is None:
        end_pos = len(text_content)

    mdna_text = text_content[start_pos:end_pos]
    mdna_text = normalize_text_spacing(mdna_text)

    if len(mdna_text.strip()) < 100:
        return None, []

    # Find ALL elements within the MD&A section
    all_text = soup.get_text()
    mdna_start_in_full_text = all_text.find(mdna_text[:200])
    mdna_end_in_full_text = mdna_start_in_full_text + len(mdna_text) if mdna_start_in_full_text != -1 else len(all_text)

    mdna_soup = None
    if mdna_start_in_full_text != -1:
        mdna_elements = []
        current_pos = 0

        for element in soup.find_all(['p', 'div', 'table', 'pre', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'span']):
            element_text = element.get_text()
            element_start = current_pos
            element_end = current_pos + len(element_text)

            if (element_start <= mdna_end_in_full_text and element_end >= mdna_start_in_full_text):
                mdna_elements.append(element)

            current_pos = element_end

        mdna_html = '<div class="mdna-section">' + ''.join(str(elem) for elem in mdna_elements) + '</div>'
        mdna_soup = BeautifulSoup(mdna_html, 'html.parser')

    # Extract ALL tables from the MD&A section
    tables = []
    if mdna_soup:
        tables = extract_and_format_tables(mdna_soup)
    else:
        # Fallback approach
        all_elements = soup.find_all(['p', 'div', 'table', 'pre', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'span'])
        mdna_elements = []
        item_7_found = False
        item_end_found = False

        for element in all_elements:
            if item_end_found:
                break

            element_text = element.get_text(strip=True)

            if not item_7_found:
                for pattern in START_PATTERNS:
                    if pattern.search(element_text):
                        item_7_found = True
                        break

            if item_7_found:
                for pattern in END_PATTERNS:
                    if pattern.search(element_text):
                        item_end_found = True
                        break

                if not item_end_found:
                    mdna_elements.append(element)

        for element in mdna_elements:
            if element.name == 'table':
                table_data = extract_and_format_tables(BeautifulSoup(str(element), 'html.parser'))
                tables.extend(table_data)
            else:
                nested_tables = extract_and_format_tables(element)
                tables.extend(nested_tables)

    logging.info(f"Found {len(tables)} tables in MD&A section (all preserved)")
    return mdna_text, tables


def extract_mdna_from_html(html_content: str) -> str:
    """Extract MD&A section from HTML content with enhanced table preservation"""
    try:
        html_content = clean_html_content(html_content)
        soup = BeautifulSoup(html_content, 'html.parser')

        text_content = soup.get_text(separator=' ', strip=True)
        text_content = normalize_text_spacing(text_content)

        mdna_text, tables = extract_mdna_with_tables(soup, text_content)

        if not mdna_text:
            logging.warning("No MD&A content found in HTML")
            return ''

        paragraphs = mdna_text.split('\n\n')
        formatted_content = []
        table_counter = 0

        for para in paragraphs:
            para = para.strip()
            if not para:
                continue

            table_indicators = [
                'table', 'following table', 'shows the following', 'summarized below',
                'as follows:', 'the following', 'breakdown', 'summary'
            ]

            might_precede_table = any(indicator in para.lower() for indicator in table_indicators)

            if (len(para) < 100 and
                    (para.isupper() or
                     para.startswith(('Item ', 'ITEM ')) or
                     re.match(r'^[A-Z][^.]*$', para))):
                formatted_content.append(f'<h3>{para}</h3>')
            else:
                formatted_content.append(f'<p>{para}</p>')

            if might_precede_table and table_counter < len(tables):
                table_html = create_html_table(tables[table_counter])
                formatted_content.append(table_html)
                table_counter += 1

        while table_counter < len(tables):
            table_html = create_html_table(tables[table_counter])
            formatted_content.append(table_html)
            table_counter += 1

        enhanced_css = """
        <style>
            body { 
                font-family: Arial, sans-serif; 
                margin: 20px; 
                line-height: 1.6;
                color: #333;
            }
            h1, h2, h3 { 
                color: #2c3e50; 
                margin-top: 25px;
                margin-bottom: 15px;
            }
            h1 { font-size: 24px; }
            h2 { font-size: 20px; }
            h3 { font-size: 16px; }
            p { 
                margin-bottom: 12px; 
                text-align: justify;
                font-size: 11px;
            }
            .header {
                border-bottom: 2px solid #2c3e50;
                padding-bottom: 10px;
                margin-bottom: 20px;
            }

            .financial-table { 
                border-collapse: collapse; 
                width: 100%; 
                margin: 15px 0;
                font-size: 9px;
                background-color: white;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            .financial-table td, .financial-table th { 
                border: 1px solid #ccc; 
                padding: 6px; 
                text-align: left;
                vertical-align: top;
            }
            .financial-table th, .financial-table td.header { 
                background-color: #f8f9fa;
                font-weight: bold;
                text-align: center;
                color: #2c3e50;
            }
            .financial-table td.numeric, .financial-table td.currency {
                text-align: right;
                font-family: 'Courier New', monospace;
                white-space: nowrap;
            }
            .financial-table td.negative {
                text-align: right;
                font-family: 'Courier New', monospace;
                color: #d9534f;
                white-space: nowrap;
            }
            .financial-table td.percentage {
                text-align: right;
                font-family: 'Courier New', monospace;
                white-space: nowrap;
            }
            .financial-table tr:nth-child(even) {
                background-color: #f9f9f9;
            }
            .financial-table tr:hover {
                background-color: #f1f1f1;
            }
            .financial-table caption {
                caption-side: top;
                font-weight: bold;
                margin-bottom: 8px;
                color: #2c3e50;
            }

            .financial-table.detected_table {
                border: 2px solid #3498db;
            }
            .financial-table.preformatted_table {
                font-family: 'Courier New', monospace;
                font-size: 8px;
            }

            .financial-table {
                page-break-inside: avoid;
            }
            .financial-table thead {
                display: table-header-group;
            }

            @media print {
                .financial-table {
                    font-size: 7px;
                }
                .financial-table td, .financial-table th {
                    padding: 3px;
                }
            }
        </style>
        """

        html_output = f"""
        <html>
        <head>
            <title>MD&A Section</title>
            <meta charset="UTF-8">
            {enhanced_css}
        </head>
        <body>
            <div class="header">
                <h1>Management's Discussion and Analysis of Financial Condition and Results of Operations</h1>
            </div>
            <div class="mdna-content">
                {''.join(formatted_content)}
            </div>
        </body>
        </html>
        """

        return html_output

    except Exception as e:
        logging.error(f"Error in HTML parsing: {e}")
        return ''


def format_text_tables(text: str) -> str:
    """Attempt to identify and format tables in plain text"""
    lines = text.split('\n')
    formatted_lines = []
    in_table = False
    table_lines = []

    for i, line in enumerate(lines):
        line = line.strip()

        is_table_row = (
                len(re.findall(r'\$[\d,]+(?:\.\d+)?', line)) >= 2 or
                len(re.findall(r'\b\d+(?:,\d{3})*(?:\.\d+)?\b', line)) >= 3 or
                len(re.findall(r'\s{3,}', line)) >= 2
        )

        is_table_header = (
                any(word in line.lower() for word in
                    ['year ended', 'three months', 'quarter', 'december', 'march', 'june', 'september']) and
                len(re.findall(r'\b\d{4}\b', line)) >= 2
        )

        if is_table_row or is_table_header:
            if not in_table:
                in_table = True
                table_lines = []
            table_lines.append(line)
        else:
            if in_table:
                if len(table_lines) >= 2:
                    formatted_table = format_plain_text_table(table_lines)
                    formatted_lines.extend(formatted_table)
                else:
                    formatted_lines.extend(table_lines)
                in_table = False
                table_lines = []

            formatted_lines.append(line)

    if in_table and len(table_lines) >= 2:
        formatted_table = format_plain_text_table(table_lines)
        formatted_lines.extend(formatted_table)
    elif in_table:
        formatted_lines.extend(table_lines)

    return '\n'.join(formatted_lines)


def extract_mdna_from_text(content: str) -> str:
    """Extract MD&A section from plain text content with table preservation"""
    matches = []
    for pattern in START_PATTERNS:
        for match in pattern.finditer(content):
            matches.append(match)

    if len(matches) < 2:
        if len(matches) == 1:
            logging.warning("Only found one Item 7 match in text, using it")
            start = matches[0]
        else:
            logging.warning("No Item 7 matches found in text")
            return ''
    else:
        start = matches[1]  # Skip TOC occurrence

    end = None
    for pattern in END_PATTERNS:
        end = pattern.search(content, start.end())
        if end:
            break

    extracted = content[start.start(): end.start() if end else None]

    if len(extracted.strip()) < 100:
        logging.warning("Extracted text MD&A is too short")
        return ''

    extracted = format_text_tables(extracted)
    return extracted


def extract_filing_date(path: Path) -> str:
    """Extract filing date from path or use file modification time"""
    match = re.search(r"(\d{4})[-_](\d{2})[-_](\d{2})", str(path))
    if match:
        return "_".join(match.groups())
    dt = datetime.fromtimestamp(path.stat().st_mtime)
    return dt.strftime("%Y_%m_%d")


def extract_cik_from_path(path: Path) -> str:
    """Extract CIK from file path"""
    match = CIK_PATTERN.search(path.as_posix())
    if match:
        return match.group(1)
    digits = re.findall(r"\b\d{10}\b", path.as_posix())
    return digits[0] if digits else "unknown"


def process_filing(filing_path: Path, cik: str, output_dir: Path):
    """Process a single filing and extract MD&A"""
    try:
        logging.info(f"→ Processing: {filing_path.name}")

        snippet = filing_path.read_text(encoding='utf-8', errors='ignore')[:3000]
        form_type = extract_form_type(snippet)
        date_str = extract_filing_date(filing_path)
        out_prefix = f"{cik}_{form_type}_{date_str}_mdna"

        existing_pdf = output_dir / f"{out_prefix}.pdf"
        existing_html = output_dir / f"{out_prefix}.html"

        if existing_pdf.exists():
            logging.info(f"⚠ PDF already exists, skipping: {existing_pdf.name}")
            return

        try:
            content = filing_path.read_text(encoding='utf-8', errors='ignore')
        except UnicodeDecodeError:
            for encoding in ['latin-1', 'cp1252', 'iso-8859-1']:
                try:
                    content = filing_path.read_text(encoding=encoding, errors='ignore')
                    break
                except UnicodeDecodeError:
                    continue
            else:
                logging.error(f"Could not read file with any encoding: {filing_path.name}")
                return

        if not content.strip():
            logging.warning(f"Empty file: {filing_path.name}")
            return

        if filing_path.suffix.lower() in ['.html', '.htm']:
            mdna_html = extract_mdna_from_html(content)
            if mdna_html:
                try:
                    out_file = output_dir / f"{out_prefix}.pdf"
                    pdfkit.from_string(mdna_html, str(out_file),
                                       configuration=pdfkit_config,
                                       options=pdfkit_options)
                    logging.info(f"✓ Extracted PDF MD&A → {out_file.name}")
                except Exception as pdf_error:
                    logging.error(f"PDF generation failed for {filing_path.name}: {pdf_error}")
                    out_file_html = output_dir / f"{out_prefix}.html"
                    out_file_html.write_text(mdna_html, encoding='utf-8')
                    logging.info(f"✓ Saved as HTML instead → {out_file_html.name}")
            else:
                logging.warning(f"✗ No MD&A found in HTML: {filing_path.name}")

        elif filing_path.suffix.lower() == '.txt':
            if existing_html.exists():
                logging.info(f"⚠ HTML version already exists, skipping TXT: {filing_path.name}")
                return

            mdna_txt = extract_mdna_from_text(content)
            if mdna_txt:
                out_file = output_dir / f"{out_prefix}.txt"
                out_file.write_text(mdna_txt, encoding='utf-8')
                logging.info(f"✓ Extracted TXT MD&A → {out_file.name}")
            else:
                logging.warning(f"✗ No MD&A found in TXT: {filing_path.name}")
        else:
            logging.warning(f"Unsupported file type: {filing_path.suffix}")

    except Exception as e:
        logging.error(f"‼ Error processing {filing_path.name}: {e}")


def smart_traverse_and_process(root: Path, output_dir: Path):
    """Traverse directory structure and process all valid filings"""
    output_dir.mkdir(parents=True, exist_ok=True)

    processed_count = 0
    error_count = 0
    skipped_count = 0

    for folder in root.rglob("*"):
        if not folder.is_dir():
            continue

        html_files = list(folder.glob("*.html"))
        htm_files = list(folder.glob("*.htm"))
        txt_files = list(folder.glob("*_RAW.txt"))
        other_txt_files = list(folder.glob("*.txt"))

        all_html_files = html_files + htm_files
        all_txt_files = txt_files + [f for f in other_txt_files if f not in txt_files]

        all_html_files = list(dict.fromkeys(all_html_files))
        all_txt_files = list(dict.fromkeys(all_txt_files))

        if not all_html_files and not all_txt_files:
            continue

        cik = extract_cik_from_path(folder)
        logging.info(f"Processing folder: {folder.name} (CIK: {cik})")

        for filing_path in all_html_files:
            try:
                process_filing(filing_path, cik, output_dir)
                processed_count += 1
            except Exception as e:
                logging.error(f"Failed to process {filing_path.name}: {e}")
                error_count += 1

        for filing_path in all_txt_files:
            try:
                snippet = filing_path.read_text(encoding='utf-8', errors='ignore')[:3000]
                form_type = extract_form_type(snippet)
                date_str = extract_filing_date(filing_path)
                out_prefix = f"{cik}_{form_type}_{date_str}_mdna"

                existing_outputs = [
                    output_dir / f"{out_prefix}.pdf",
                    output_dir / f"{out_prefix}.html"
                ]

                if any(f.exists() for f in existing_outputs):
                    logging.info(f"⚠ Skipping TXT file, HTML version already processed: {filing_path.name}")
                    skipped_count += 1
                    continue

                process_filing(filing_path, cik, output_dir)
                processed_count += 1
            except Exception as e:
                logging.error(f"Failed to process {filing_path.name}: {e}")
                error_count += 1

    logging.info(
        f"Processing complete. Files processed: {processed_count}, Errors: {error_count}, Skipped: {skipped_count}")


def validate_dependencies():
    """Validate that required dependencies are available"""
    try:
        import pdfkit
        if os.path.exists(WKHTMLTOPDF_PATH):
            logging.info("✓ wkhtmltopdf found")
        else:
            logging.warning(f"⚠ wkhtmltopdf not found at {WKHTMLTOPDF_PATH}")
            logging.warning("PDF generation may fail. Consider installing wkhtmltopdf or updating the path.")
    except ImportError:
        logging.error("pdfkit not installed. Run: pip install pdfkit")
        return False

    try:
        from bs4 import BeautifulSoup
        logging.info("✓ BeautifulSoup4 available")
    except ImportError:
        logging.error("BeautifulSoup4 not installed. Run: pip install beautifulsoup4")
        return False

    try:
        from lxml import html as lxml_html
        logging.info("✓ lxml available")
    except ImportError:
        logging.error("lxml not installed. Run: pip install lxml")
        return False

    return True


def create_sample_output_structure():
    """Create a sample directory structure for output organization"""
    sample_structure = OUTPUT_ROOT / "README.txt"
    if not sample_structure.exists():
        readme_content = """
MD&A Extraction Output Directory
===============================

This directory contains extracted Management's Discussion & Analysis (MD&A) sections 
from SEC 10-K filings.

File Naming Convention:
{CIK}_{FORM_TYPE}_{DATE}_mdna.{ext}

Where:
- CIK: Company's Central Index Key (10 digits)
- FORM_TYPE: Usually "10-K" or "10-K/A" 
- DATE: Filing date in YYYY_MM_DD format
- ext: pdf (from HTML) or txt (from text files)

Examples:
- 0000123456_10-K_2023_03_15_mdna.pdf
- 0000789012_10-K_2022_12_31_mdna.txt

The extracted content focuses specifically on Item 7 - Management's Discussion 
and Analysis of Financial Condition and Results of Operations.

Tables and financial data are preserved with proper formatting and alignment.
ALL tables found within Item 7 boundaries are included without content filtering.
"""
        sample_structure.write_text(readme_content, encoding='utf-8')


def main():
    """Main execution function"""
    print("MD&A Section Extractor for SEC 10-K Filings")
    print("=" * 50)

    if not validate_dependencies():
        logging.error("Missing required dependencies. Please install them and try again.")
        return

    if not INPUT_ROOT.exists():
        logging.error(f"Input directory does not exist: {INPUT_ROOT}")
        return

    create_sample_output_structure()

    logging.info(f"Input directory: {INPUT_ROOT}")
    logging.info(f"Output directory: {OUTPUT_ROOT}")
    logging.info(f"Log file: {LOG_FILE}")
    logging.info("=" * 50)

    logging.info(f"Scanning filings under: {INPUT_ROOT}")
    smart_traverse_and_process(INPUT_ROOT, OUTPUT_ROOT)
    logging.info("✓ Extraction complete.")

    print(f"\nProcessing complete! Check {LOG_FILE} for detailed logs.")
    print(f"Extracted MD&A files are in: {OUTPUT_ROOT}")


if __name__ == '__main__':
    main()