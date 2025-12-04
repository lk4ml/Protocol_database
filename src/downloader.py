"""
Protocol Downloader - Downloads and manages clinical trial protocols.
"""
import os
import re
import time
import logging
import requests
from pathlib import Path
from typing import Optional, List, Dict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import PROTOCOLS_DIR, REQUEST_DELAY
from .database import ProtocolDatabase
from .api_client import ClinicalTrialsAPI

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProtocolDownloader:
    """Downloads and manages clinical trial protocol PDFs."""

    def __init__(self, db: Optional[ProtocolDatabase] = None):
        self.db = db or ProtocolDatabase()
        self.api = ClinicalTrialsAPI()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ProtocolDatabasePipeline/1.0'
        })

    def _sanitize_folder_name(self, name: str) -> str:
        """Create a safe folder name from indication."""
        # Replace spaces with underscores, remove special chars
        safe_name = re.sub(r'[^\w\s-]', '', name.lower())
        safe_name = re.sub(r'[\s]+', '_', safe_name)
        return safe_name

    def _get_indication_folder(self, indication: str) -> Path:
        """Get or create the folder for an indication."""
        folder_name = self._sanitize_folder_name(indication)
        folder_path = PROTOCOLS_DIR / folder_name
        folder_path.mkdir(parents=True, exist_ok=True)
        return folder_path

    def download_pdf(self, url: str, save_path: Path, retries: int = 3) -> bool:
        """Download a PDF file with retry logic."""
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=60, stream=True)
                response.raise_for_status()

                # Check if it's actually a PDF
                content_type = response.headers.get('Content-Type', '')
                if 'pdf' not in content_type.lower() and not url.endswith('.pdf'):
                    logger.warning(f"URL does not appear to be a PDF: {url}")

                with open(save_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

                logger.info(f"Downloaded: {save_path.name}")
                return True

            except requests.exceptions.RequestException as e:
                logger.warning(f"Download failed (attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)

        return False

    def process_indication(
        self,
        indication: str,
        download_pdfs: bool = True,
        max_studies: Optional[int] = None
    ) -> Dict:
        """
        Process all studies for an indication.
        Fetches metadata and optionally downloads protocol PDFs.
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing indication: {indication}")
        logger.info(f"{'='*60}")

        # Get folder for this indication
        indication_folder = self._get_indication_folder(indication)
        logger.info(f"Protocols will be saved to: {indication_folder}")

        # Statistics
        stats = {
            'indication': indication,
            'studies_found': 0,
            'new_studies': 0,
            'updated_studies': 0,
            'protocols_downloaded': 0,
            'download_errors': 0,
            'start_time': datetime.now()
        }

        # Fetch studies from API
        studies_processed = 0
        for study_data in self.api.search_studies(indication):
            if max_studies and studies_processed >= max_studies:
                logger.info(f"Reached max studies limit: {max_studies}")
                break

            stats['studies_found'] += 1
            nct_id = study_data['nct_id']

            # Upsert to database
            is_new = self.db.upsert_protocol(study_data)
            if is_new:
                stats['new_studies'] += 1
            else:
                stats['updated_studies'] += 1

            # Download protocol PDF if available
            if download_pdfs and study_data.get('protocol_url'):
                pdf_filename = f"{nct_id}_protocol.pdf"
                pdf_path = indication_folder / pdf_filename

                # Skip if already downloaded
                if pdf_path.exists():
                    logger.debug(f"PDF already exists: {pdf_filename}")
                else:
                    if self.download_pdf(study_data['protocol_url'], pdf_path):
                        stats['protocols_downloaded'] += 1
                        self.db.update_pdf_path(nct_id, str(pdf_path))
                    else:
                        stats['download_errors'] += 1

                    # Rate limiting for downloads
                    time.sleep(REQUEST_DELAY)

            studies_processed += 1
            if studies_processed % 100 == 0:
                logger.info(f"Processed {studies_processed} studies...")

        stats['end_time'] = datetime.now()
        stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()

        # Log to database
        self.db.log_download(
            indication=indication,
            studies_found=stats['studies_found'],
            protocols_downloaded=stats['protocols_downloaded'],
            new_studies=stats['new_studies'],
            updated_studies=stats['updated_studies'],
            status='completed'
        )

        # Print summary
        self._print_summary(stats)
        return stats

    def download_missing_pdfs(self, indication: Optional[str] = None) -> Dict:
        """Download PDFs for protocols that have URLs but no downloaded file."""
        logger.info("Checking for missing protocol PDFs...")

        protocols = self.db.get_protocols_without_pdf(indication)
        logger.info(f"Found {len(protocols)} protocols without downloaded PDFs")

        stats = {
            'total': len(protocols),
            'downloaded': 0,
            'errors': 0
        }

        for protocol in protocols:
            nct_id = protocol['nct_id']
            url = protocol['protocol_url']
            ind = protocol['indication']

            if not url:
                continue

            indication_folder = self._get_indication_folder(ind)
            pdf_filename = f"{nct_id}_protocol.pdf"
            pdf_path = indication_folder / pdf_filename

            if self.download_pdf(url, pdf_path):
                stats['downloaded'] += 1
                self.db.update_pdf_path(nct_id, str(pdf_path))
            else:
                stats['errors'] += 1

            time.sleep(REQUEST_DELAY)

        logger.info(f"Downloaded {stats['downloaded']} PDFs, {stats['errors']} errors")
        return stats

    def _print_summary(self, stats: Dict):
        """Print processing summary."""
        logger.info(f"\n{'='*60}")
        logger.info(f"Summary for: {stats['indication']}")
        logger.info(f"{'='*60}")
        logger.info(f"Studies found:        {stats['studies_found']}")
        logger.info(f"New studies:          {stats['new_studies']}")
        logger.info(f"Updated studies:      {stats['updated_studies']}")
        logger.info(f"Protocols downloaded: {stats['protocols_downloaded']}")
        logger.info(f"Download errors:      {stats['download_errors']}")
        logger.info(f"Duration:             {stats['duration']:.1f} seconds")
        logger.info(f"{'='*60}\n")


class PipelineRunner:
    """Orchestrates the full pipeline for multiple indications."""

    def __init__(self):
        self.db = ProtocolDatabase()
        self.downloader = ProtocolDownloader(self.db)

    def run(
        self,
        indications: List[str],
        download_pdfs: bool = True,
        max_studies_per_indication: Optional[int] = None
    ) -> Dict:
        """Run the pipeline for multiple indications."""
        logger.info("\n" + "="*70)
        logger.info("CLINICAL TRIALS PROTOCOL DATABASE PIPELINE")
        logger.info("="*70)
        logger.info(f"Indications to process: {indications}")
        logger.info(f"Download PDFs: {download_pdfs}")
        logger.info("="*70 + "\n")

        all_stats = []
        start_time = datetime.now()

        for indication in indications:
            try:
                stats = self.downloader.process_indication(
                    indication=indication,
                    download_pdfs=download_pdfs,
                    max_studies=max_studies_per_indication
                )
                all_stats.append(stats)
            except Exception as e:
                logger.error(f"Error processing {indication}: {e}")
                all_stats.append({
                    'indication': indication,
                    'error': str(e)
                })

        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()

        # Overall summary
        self._print_overall_summary(all_stats, total_duration)

        return {
            'indications': all_stats,
            'total_duration': total_duration
        }

    def _print_overall_summary(self, all_stats: List[Dict], total_duration: float):
        """Print overall pipeline summary."""
        logger.info("\n" + "="*70)
        logger.info("PIPELINE COMPLETE - OVERALL SUMMARY")
        logger.info("="*70)

        total_studies = sum(s.get('studies_found', 0) for s in all_stats)
        total_new = sum(s.get('new_studies', 0) for s in all_stats)
        total_protocols = sum(s.get('protocols_downloaded', 0) for s in all_stats)

        for stats in all_stats:
            if 'error' in stats:
                logger.info(f"  {stats['indication']}: ERROR - {stats['error']}")
            else:
                logger.info(
                    f"  {stats['indication']}: "
                    f"{stats['studies_found']} studies, "
                    f"{stats['new_studies']} new, "
                    f"{stats['protocols_downloaded']} protocols"
                )

        logger.info("-"*70)
        logger.info(f"Total studies:        {total_studies}")
        logger.info(f"Total new studies:    {total_new}")
        logger.info(f"Total protocols:      {total_protocols}")
        logger.info(f"Total duration:       {total_duration:.1f} seconds")
        logger.info("="*70 + "\n")

    def get_database_stats(self) -> Dict:
        """Get current database statistics."""
        overall = self.db.get_statistics()
        by_indication = {}

        for indication in self.db.get_all_indications():
            by_indication[indication] = self.db.get_statistics(indication)

        return {
            'overall': overall,
            'by_indication': by_indication
        }

    def export_to_csv(self, output_path: str, indication: Optional[str] = None):
        """Export protocol data to CSV."""
        import csv

        if indication:
            protocols = self.db.get_protocols_by_indication(indication)
        else:
            protocols = []
            for ind in self.db.get_all_indications():
                protocols.extend(self.db.get_protocols_by_indication(ind))

        if not protocols:
            logger.warning("No protocols to export")
            return

        fieldnames = [
            'nct_id', 'official_title', 'brief_title', 'sponsor', 'sponsor_class',
            'year', 'start_date', 'completion_date', 'indication', 'conditions',
            'phase', 'study_type', 'overall_status', 'enrollment', 'interventions',
            'protocol_url', 'protocol_pdf_path', 'has_protocol_doc'
        ]

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(protocols)

        logger.info(f"Exported {len(protocols)} protocols to {output_path}")
