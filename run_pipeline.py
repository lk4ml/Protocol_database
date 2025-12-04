#!/usr/bin/env python3
"""
Clinical Trials Protocol Database Pipeline

Main entry point for downloading and managing clinical trial protocols
from ClinicalTrials.gov.

Usage:
    # Run for default indications (obesity, prostate cancer, lung cancer)
    python run_pipeline.py

    # Run for specific indications
    python run_pipeline.py --indications "lung cancer" "breast cancer"

    # Run for a new indication
    python run_pipeline.py --indications "diabetes"

    # Show database statistics
    python run_pipeline.py --stats

    # Export to CSV
    python run_pipeline.py --export output.csv

    # Download missing PDFs
    python run_pipeline.py --download-missing

    # Limit studies per indication (for testing)
    python run_pipeline.py --max-studies 100

    # Skip PDF downloads (metadata only)
    python run_pipeline.py --no-pdfs
"""
import argparse
import sys
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.config import DEFAULT_INDICATIONS, PROTOCOLS_DIR, DATABASE_PATH
from src.downloader import PipelineRunner
from src.database import ProtocolDatabase

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Clinical Trials Protocol Database Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--indications', '-i',
        nargs='+',
        default=None,
        help='List of indications to process (default: obesity, prostate cancer, lung cancer)'
    )

    parser.add_argument(
        '--max-studies', '-m',
        type=int,
        default=None,
        help='Maximum number of studies per indication (for testing)'
    )

    parser.add_argument(
        '--no-pdfs',
        action='store_true',
        help='Skip downloading PDF files (metadata only)'
    )

    parser.add_argument(
        '--download-missing',
        action='store_true',
        help='Download PDFs for protocols that have URLs but no downloaded file'
    )

    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show database statistics'
    )

    parser.add_argument(
        '--export',
        type=str,
        metavar='FILE',
        help='Export protocols to CSV file'
    )

    parser.add_argument(
        '--export-indication',
        type=str,
        help='Specific indication to export (use with --export)'
    )

    parser.add_argument(
        '--history',
        action='store_true',
        help='Show download history'
    )

    parser.add_argument(
        '--list-indications',
        action='store_true',
        help='List all indications in database'
    )

    parser.add_argument(
        '--search',
        type=str,
        help='Search protocols by keyword'
    )

    args = parser.parse_args()

    # Initialize
    runner = PipelineRunner()
    db = ProtocolDatabase()

    # Handle different commands
    if args.stats:
        show_stats(runner)
        return

    if args.history:
        show_history(db)
        return

    if args.list_indications:
        list_indications(db)
        return

    if args.export:
        runner.export_to_csv(args.export, args.export_indication)
        return

    if args.download_missing:
        runner.downloader.download_missing_pdfs(args.export_indication)
        return

    if args.search:
        search_protocols(db, args.search)
        return

    # Run main pipeline
    indications = args.indications or DEFAULT_INDICATIONS
    download_pdfs = not args.no_pdfs

    print(f"\nProtocols directory: {PROTOCOLS_DIR}")
    print(f"Database path: {DATABASE_PATH}\n")

    runner.run(
        indications=indications,
        download_pdfs=download_pdfs,
        max_studies_per_indication=args.max_studies
    )


def show_stats(runner: PipelineRunner):
    """Display database statistics."""
    stats = runner.get_database_stats()

    print("\n" + "="*60)
    print("DATABASE STATISTICS")
    print("="*60)

    overall = stats['overall']
    print(f"\nOverall:")
    print(f"  Total studies:     {overall.get('total_studies', 0)}")
    print(f"  With protocols:    {overall.get('with_protocols', 0)}")
    print(f"  Indications:       {overall.get('indication_count', 0)}")
    print(f"  Year range:        {overall.get('earliest_year', 'N/A')} - {overall.get('latest_year', 'N/A')}")

    print(f"\nBy Indication:")
    for indication, ind_stats in stats['by_indication'].items():
        print(f"\n  {indication}:")
        print(f"    Studies:         {ind_stats.get('total_studies', 0)}")
        print(f"    With protocols:  {ind_stats.get('with_protocols', 0)}")
        print(f"    Years:           {ind_stats.get('earliest_year', 'N/A')} - {ind_stats.get('latest_year', 'N/A')}")

    print("="*60 + "\n")


def show_history(db: ProtocolDatabase):
    """Display download history."""
    history = db.get_download_history(limit=20)

    print("\n" + "="*70)
    print("DOWNLOAD HISTORY (Last 20)")
    print("="*70)

    for entry in history:
        print(f"\n  {entry['download_date']}")
        print(f"    Indication:      {entry['indication']}")
        print(f"    Studies found:   {entry['studies_found']}")
        print(f"    New studies:     {entry['new_studies']}")
        print(f"    PDFs downloaded: {entry['protocols_downloaded']}")
        print(f"    Status:          {entry['status']}")

    print("="*70 + "\n")


def list_indications(db: ProtocolDatabase):
    """List all indications in database."""
    indications = db.get_all_indications()

    print("\n" + "="*40)
    print("INDICATIONS IN DATABASE")
    print("="*40)

    if not indications:
        print("  No indications found")
    else:
        for indication in indications:
            stats = db.get_statistics(indication)
            print(f"  {indication}: {stats.get('total_studies', 0)} studies")

    print("="*40 + "\n")


def search_protocols(db: ProtocolDatabase, query: str):
    """Search protocols by keyword."""
    results = db.search_protocols(query)

    print(f"\n" + "="*70)
    print(f"SEARCH RESULTS FOR: {query}")
    print("="*70)

    if not results:
        print("  No results found")
    else:
        print(f"  Found {len(results)} protocols:\n")
        for p in results[:20]:  # Limit display
            print(f"  {p['nct_id']} ({p['year'] or 'N/A'})")
            print(f"    {p['brief_title'][:60]}..." if len(p['brief_title'] or '') > 60 else f"    {p['brief_title']}")
            print(f"    Phase: {p['phase']} | Sponsor: {p['sponsor'][:30]}...")
            print()

        if len(results) > 20:
            print(f"  ... and {len(results) - 20} more")

    print("="*70 + "\n")


if __name__ == '__main__':
    main()
