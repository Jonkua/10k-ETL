#!/usr/bin/env python3
"""
get_10k_filing.py

Robust SEC EDGAR 10-K and related filings downloader utility designed for integration
with a larger SIC batch processing pipeline.

Reads config from environment variables by default:
- SEC_EMAIL (required)
- SEC_COMPANY (required)
- EDGAR_DOWNLOAD_DIR (default: "data")
- EDGAR_START_DATE (default: "1994-01-01")
- EDGAR_END_DATE (default: today)

Usage:
    from get_10k_filing import download_10k_for_cik

    success = download_10k_for_cik(cik="0000320193", company_name="Apple Inc.")
"""
import os
import time
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List
import subprocess
import sys

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class SEC10KDownloader:
    def __init__(
            self,
            download_dir: Optional[Path] = None,
            start_date: str = "1994-01-01",
            end_date: Optional[str] = None,
            email: Optional[str] = None,
            company_name: Optional[str] = None,
            rate_limit_sec: float = 0.15,
            max_retries: int = 3,
            retry_backoff_sec: float = 2.0,
    ):
        self.download_dir = download_dir or Path(os.environ.get("EDGAR_DOWNLOAD_DIR", "data"))
        self.download_dir.mkdir(parents=True, exist_ok=True)

        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(
            end_date or datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d"
        )

        self.email = email or os.getenv("SEC_EMAIL")
        self.company_name = company_name or os.getenv("SEC_COMPANY")
        if not self.email or not self.company_name:
            raise ValueError("SEC_EMAIL and SEC_COMPANY must be set.")

        self.rate_limit_sec = rate_limit_sec
        self.max_retries = max_retries
        self.retry_backoff_sec = retry_backoff_sec

        user_agent = f"{self.company_name} ({self.email})"
        # Session for data.sec.gov
        self.session_data = requests.Session()
        self.session_data.headers.update({
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Host": "data.sec.gov",
            "Accept": "application/json, text/plain, */*",
        })
        # Session for www.sec.gov archives
        self.session_archives = requests.Session()
        self.session_archives.headers.update({
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

        self.DATA_SEC_URL = "https://data.sec.gov"
        self.ARCHIVES_BASE_URL = f"https://www.sec.gov/Archives/edgar/data"

    def _rate_limit(self):
        time.sleep(self.rate_limit_sec)

    def _retry_get(self, session: requests.Session, url: str, **kwargs) -> Optional[requests.Response]:
        for attempt in range(1, self.max_retries + 1):
            try:
                self._rate_limit()
                resp = session.get(url, timeout=30, **kwargs)
                if resp.status_code == 200:
                    return resp
                elif resp.status_code == 404:
                    logger.debug(f"URL not found (404): {url}")
                    return None
                else:
                    logger.warning(f"Unexpected status {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Request attempt {attempt} error for {url}: {e}")
            time.sleep(self.retry_backoff_sec * attempt)
        logger.error(f"Failed to retrieve URL after {self.max_retries} attempts: {url}")
        return None

    def get_submissions_json(self, cik: str) -> Optional[Dict]:
        cik_padded = str(cik).zfill(10)
        url = f"{self.DATA_SEC_URL}/submissions/CIK{cik_padded}.json"
        resp = self._retry_get(self.session_data, url)
        if resp:
            try:
                return resp.json()
            except Exception as e:
                logger.error(f"Error parsing submissions JSON for {cik}: {e}")
        return None

    def get_filing_index_json(self, cik: str, accession: str) -> Optional[Dict]:
        cik_no_pad = str(int(cik))  # strip leading zeros
        accession_no_dash = accession.replace("-", "")
        url = f"{self.ARCHIVES_BASE_URL}/{cik_no_pad}/{accession_no_dash}/index.json"
        resp = self._retry_get(self.session_archives, url)
        if resp:
            try:
                return resp.json()
            except Exception as e:
                logger.error(f"Error parsing index.json for {cik} {accession}: {e}")
        return None

    def extract_html_from_txt_filing(self, content: str) -> Optional[str]:
        """
        Extract HTML content from a full submission text file.
        For pre-2003 filings that contain embedded HTML.
        """
        # Look for DOCUMENT markers
        doc_pattern = r'<DOCUMENT>(.*?)</DOCUMENT>'

        for match in re.finditer(doc_pattern, content, re.DOTALL | re.IGNORECASE):
            doc_content = match.group(1)

            # Check if this document contains 10-K content
            if any(marker in doc_content.upper() for marker in ['10-K', 'FORM 10-K', 'ANNUAL REPORT']):
                # Look for HTML content within the document
                html_start = doc_content.find('<HTML>')
                if html_start == -1:
                    html_start = doc_content.find('<html>')

                if html_start != -1:
                    html_end = doc_content.find('</HTML>', html_start)
                    if html_end == -1:
                        html_end = doc_content.find('</html>', html_start)

                    if html_end != -1:
                        return doc_content[html_start:html_end + 7]

        # If no DOCUMENT tags, try direct HTML extraction
        html_start = content.find('<HTML>')
        if html_start == -1:
            html_start = content.find('<html>')

        if html_start != -1:
            html_end = content.find('</HTML>', html_start)
            if html_end == -1:
                html_end = content.find('</html>', html_start)

            if html_end != -1:
                return content[html_start:html_end + 7]

        return None

    def extract_text_from_html(self, html_content: str) -> str:
        """
        Extract clean text from HTML content only.
        """
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            for script in soup(["script", "style"]):
                script.decompose()
            text = soup.get_text(separator="\n")
            lines = (ln.strip() for ln in text.splitlines())
            chunks = (phrase.strip() for ln in lines for phrase in ln.split("  "))
            return "\n".join(ch for ch in chunks if ch)
        except Exception as e:
            logger.error(f"Error extracting text from HTML: {e}")
            return ""

    def extract_text_content(self, content: str) -> str:
        """
        Extract clean text from either HTML or raw text content.
        """
        # First check if it's HTML
        if '<html' in content.lower() and '</html' in content.lower():
            return self.extract_text_from_html(content)

        # If not HTML, clean up raw text
        # Remove excessive whitespace and clean up formatting
        lines = content.split('\n')
        cleaned_lines = []

        for line in lines:
            # Skip lines that are mostly special characters or formatting
            if line.strip() and not all(c in '-=_*' for c in line.strip()):
                cleaned_lines.append(line.strip())

        return '\n'.join(cleaned_lines)

    def html_to_pdf(self, html_path: Path, pdf_path: Path) -> bool:
        """
        Convert HTML to PDF using wkhtmltopdf if available.
        Falls back to weasyprint or returns False if neither available.
        """
        # Try wkhtmltopdf first
        try:
            result = subprocess.run(
                ['wkhtmltopdf', '--quiet', str(html_path), str(pdf_path)],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                logger.info(f"Successfully converted to PDF using wkhtmltopdf: {pdf_path.name}")
                return True
        except FileNotFoundError:
            logger.debug("wkhtmltopdf not found")

        # Try weasyprint as fallback
        try:
            import weasyprint
            weasyprint.HTML(filename=str(html_path)).write_pdf(str(pdf_path))
            logger.info(f"Successfully converted to PDF using weasyprint: {pdf_path.name}")
            return True
        except ImportError:
            logger.debug("weasyprint not installed")
        except Exception as e:
            logger.warning(f"weasyprint conversion failed: {e}")
        return False

    def select_primary_filing(self, items: List[Dict]) -> Optional[str]:
        """
        Select the primary 10-K document from the filing items.
        Prioritizes HTML/HTM files, then text files.
        """

        def get_size(item):
            try:
                return int(item.get("size", 0))
            except (TypeError, ValueError):
                return 0

        # Filter files larger than 2KB
        candidates = [it for it in items if get_size(it) > 2000]

        # First, look for HTML files with 10-K in the name
        for kw in ("10-k", "10k", "form10-k", "form10k"):
            for it in candidates:
                nm = it.get("name", "").lower()
                if kw in nm and nm.endswith((".htm", ".html")):
                    return it["name"]

        # Then look for text files with 10-K in the name (for older filings)
        for kw in ("10-k", "10k", "form10-k", "form10k"):
            for it in candidates:
                nm = it.get("name", "").lower()
                if kw in nm and nm.endswith(".txt"):
                    return it["name"]

        # Fallback: largest HTML file
        htmls = [it for it in candidates if it.get("name", "").lower().endswith((".htm", ".html"))]
        if htmls:
            return max(htmls, key=get_size)["name"]

        # Final fallback: largest text file
        txts = [it for it in candidates if it.get("name", "").lower().endswith(".txt")]
        if txts:
            return max(txts, key=get_size)["name"]

        return None

    def save_filing_files(
            self,
            cik: str,
            company_name: Optional[str],
            filing_date: str,
            accession: str,
            filing_items: List[Dict],
    ) -> Dict[str, str]:
        """
        Download and save HTML, text, and PDF for a single filing.
        Handles both modern HTML filings and older text-only filings.
        Returns a dict mapping file types to paths, or empty dict on failure.
        """
        try:
            cik_pad = str(int(cik))
            acc_no_dash = accession.replace("-", "")
            safe_name = re.sub(r'[<>:"/\\|?*]', "_", (company_name.strip() if company_name else "") or cik)
            out_dir = self.download_dir / f"{cik}_{safe_name}" / f"10K_{filing_date}_{acc_no_dash}"
            out_dir.mkdir(parents=True, exist_ok=True)

            downloaded = {}

            # Standard filename base for consistency
            base_filename = f"10K_{filing_date}"

            # Try to download the primary filing document
            primary = self.select_primary_filing(filing_items)

            if primary:
                url = f"{self.ARCHIVES_BASE_URL}/{cik_pad}/{acc_no_dash}/{primary}"
                resp = self._retry_get(self.session_archives, url)
                if resp:
                    content = resp.text
                    is_html = primary.lower().endswith(('.htm', '.html'))

                    if is_html:
                        # Save HTML file with consistent naming
                        html_path = out_dir / f"{base_filename}.html"
                        html_path.write_text(content, encoding="utf-8")
                        downloaded["html"] = str(html_path)

                        # Extract and save text with consistent naming
                        txt = self.extract_text_content(content)
                        if txt:
                            txt_path = out_dir / f"{base_filename}.txt"
                            txt_path.write_text(txt, encoding="utf-8")
                            downloaded["txt"] = str(txt_path)

                        # Convert to PDF if possible
                        pdf_path = out_dir / f"{base_filename}.pdf"
                        if self.html_to_pdf(html_path, pdf_path):
                            downloaded["pdf"] = str(pdf_path)
                    else:
                        # Handle text filing
                        # First save the raw text
                        txt_raw_path = out_dir / f"{base_filename}_raw.txt"
                        txt_raw_path.write_text(content, encoding="utf-8")
                        downloaded["txt_raw"] = str(txt_raw_path)

                        # Try to extract HTML from text filing
                        html_content = self.extract_html_from_txt_filing(content)
                        if html_content:
                            html_path = out_dir / f"{base_filename}.html"
                            html_path.write_text(html_content, encoding="utf-8")
                            downloaded["html"] = str(html_path)

                            # Convert extracted HTML to PDF
                            pdf_path = out_dir / f"{base_filename}.pdf"
                            if self.html_to_pdf(html_path, pdf_path):
                                downloaded["pdf"] = str(pdf_path)

                        # Extract clean text with consistent naming
                        clean_txt = self.extract_text_content(content)
                        if clean_txt:
                            txt_path = out_dir / f"{base_filename}.txt"
                            txt_path.write_text(clean_txt, encoding="utf-8")
                            downloaded["txt"] = str(txt_path)

            # If no primary filing found, try the full submission text file
            if not downloaded:
                # Try accession.txt (full submission)
                full_sub_url = f"{self.ARCHIVES_BASE_URL}/{cik_pad}/{acc_no_dash}/{accession}.txt"
                resp = self._retry_get(self.session_archives, full_sub_url)
                if resp:
                    content = resp.text

                    # Save raw submission
                    raw_path = out_dir / f"{base_filename}_raw.txt"
                    raw_path.write_text(content, encoding="utf-8")
                    downloaded["txt_raw"] = str(raw_path)

                    # Try to extract HTML
                    html_content = self.extract_html_from_txt_filing(content)
                    if html_content:
                        html_path = out_dir / f"{base_filename}.html"
                        html_path.write_text(html_content, encoding="utf-8")
                        downloaded["html"] = str(html_path)

                        # Convert to PDF
                        pdf_path = out_dir / f"{base_filename}.pdf"
                        if self.html_to_pdf(html_path, pdf_path):
                            downloaded["pdf"] = str(pdf_path)

                    # Extract clean text with consistent naming
                    clean_txt = self.extract_text_content(content)
                    if clean_txt:
                        txt_path = out_dir / f"{base_filename}.txt"
                        txt_path.write_text(clean_txt, encoding="utf-8")
                        downloaded["txt"] = str(txt_path)

            # Also check for existing PDF files in the filing
            for it in filing_items:
                nm = it.get("name", "").lower()
                if nm.endswith(".pdf") and int(it.get("size", 0)) > 10000 and "pdf" not in downloaded:
                    pdf_url = f"{self.ARCHIVES_BASE_URL}/{cik_pad}/{acc_no_dash}/{it['name']}"
                    r2 = self._retry_get(self.session_archives, pdf_url)
                    if r2 and r2.headers.get('content-type', '').lower().startswith('application/pdf'):
                        # Save original PDF with consistent naming
                        ppath = out_dir / f"{base_filename}_original.pdf"
                        ppath.write_bytes(r2.content)
                        downloaded["pdf_original"] = str(ppath)
                        logger.info(f"Downloaded original PDF")
                    break

            # Write metadata
            meta = {
                "cik": cik,
                "company_name": company_name,
                "filing_date": filing_date,
                "accession": accession,
                "downloaded_files": downloaded,
                "timestamp": datetime.now().isoformat(),
            }
            mpath = out_dir / "filing_metadata.json"
            mpath.write_text(json.dumps(meta, indent=2))

            return downloaded

        except Exception as e:
            logger.error(f"Error saving files for {cik} {accession}: {e}")
            return {}

    def download_10k_filings(
            self, cik: str, company_name: Optional[str] = None, verbose: bool = True
    ) -> bool:
        """
        Orchestrate retrieval of filings JSON, index, and saving files.
        Returns True if at least one filing succeeded.
        """
        submissions = self.get_submissions_json(cik)
        if not submissions:
            logger.error(f"No submissions JSON for {cik}")
            return False

        recent = submissions.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        valid = {"10-K", "10-K/A", "10-K405", "10-K405/A", "10-KT", "10-KT/A"}

        count = 0
        for i, form in enumerate(forms):
            if form not in valid:
                continue
            try:
                fdate = datetime.strptime(dates[i], "%Y-%m-%d")
            except Exception:
                logger.warning(f"Skipping invalid date: {dates[i]}")
                continue
            if not (self.start_date <= fdate <= self.end_date):
                continue

            acc = accessions[i]

            # Try to get index.json first
            idx = self.get_filing_index_json(cik, acc)
            items = idx.get("directory", {}).get("item", []) if idx else []

            # If no index.json (common for older filings), create a minimal items list
            if not items:
                # Add the main accession text file as an item
                items = [{
                    "name": f"{acc}.txt",
                    "size": "1000000"  # Assume 1MB as we don't know actual size
                }]
                logger.debug(f"No index.json for {acc}, using fallback")

            dl = self.save_filing_files(cik, company_name, dates[i], acc, items)
            if dl:
                count += 1
                if verbose:
                    logger.info(f"Downloaded {form} for {cik} on {dates[i]} - {len(dl)} files")

        logger.info(f"Successfully downloaded {count} 10-K filings for CIK {cik}")
        return count > 0


def get_ticker_10k_filing(
        cik: str,
        company_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        download_dir: Optional[Path] = None,
        email: Optional[str] = None,
        org_name: Optional[str] = None,
) -> bool:
    """
    Wrapper to initialize SEC10KDownloader from env vars or args.
    """
    sd = SEC10KDownloader(
        download_dir=download_dir,
        start_date=start_date or os.getenv("EDGAR_START_DATE", "1994-01-01"),
        end_date=end_date or os.getenv("EDGAR_END_DATE"),
        email=email or os.getenv("SEC_EMAIL"),
        company_name=org_name or os.getenv("SEC_COMPANY"),
    )
    return sd.download_10k_filings(cik, company_name)


# Alias for compatibility
download_10k_for_cik = get_ticker_10k_filing

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Download SEC EDGAR 10-K filings")
    parser.add_argument("cik", help="CIK number to download filings for")
    parser.add_argument("--company_name", default=None)
    parser.add_argument("--start_date", default=os.getenv("EDGAR_START_DATE", "1994-01-01"))
    parser.add_argument(
        "--end_date", default=os.getenv("EDGAR_END_DATE", datetime.now().strftime("%Y-%m-%d"))
    )
    parser.add_argument(
        "--download_dir", default=os.getenv("EDGAR_DOWNLOAD_DIR", "data")
    )
    parser.add_argument("--email", default=os.getenv("SEC_EMAIL"))
    parser.add_argument("--org_name", default=os.getenv("SEC_COMPANY"))
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.INFO)

    success = download_10k_for_cik(
        cik=args.cik,
        company_name=args.company_name,
        start_date=args.start_date,
        end_date=args.end_date,
        download_dir=Path(args.download_dir),
        email=args.email,
        org_name=args.org_name,
    )
    print(f"Download success: {success}")