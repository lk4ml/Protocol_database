#!/usr/bin/env python3
"""
Weekly Scheduler for Clinical Trials Protocol Pipeline

This script can be used to schedule weekly runs of the pipeline.
It can be run in the background or set up as a cron job.

Usage:
    # Run as a daemon (keeps running and schedules weekly)
    python schedule_weekly.py --daemon

    # Or set up as a cron job (add to crontab):
    # 0 0 * * 0 /path/to/python /path/to/run_pipeline.py >> /path/to/logs/weekly.log 2>&1

    # Run once immediately then exit
    python schedule_weekly.py --once
"""
import argparse
import time
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.config import DEFAULT_INDICATIONS
from src.downloader import PipelineRunner

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Path(__file__).parent / 'data' / 'pipeline.log')
    ]
)
logger = logging.getLogger(__name__)


def run_pipeline(indications=None):
    """Execute the pipeline."""
    indications = indications or DEFAULT_INDICATIONS
    logger.info(f"Starting scheduled pipeline run at {datetime.now()}")

    try:
        runner = PipelineRunner()
        result = runner.run(
            indications=indications,
            download_pdfs=True
        )
        logger.info(f"Pipeline completed. Duration: {result['total_duration']:.1f}s")
        return True
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Weekly Protocol Pipeline Scheduler')

    parser.add_argument(
        '--daemon',
        action='store_true',
        help='Run as daemon with weekly schedule'
    )

    parser.add_argument(
        '--once',
        action='store_true',
        help='Run once immediately and exit'
    )

    parser.add_argument(
        '--indications', '-i',
        nargs='+',
        default=None,
        help='Indications to process'
    )

    parser.add_argument(
        '--day',
        type=str,
        default='sunday',
        help='Day of week to run (for daemon mode)'
    )

    parser.add_argument(
        '--hour',
        type=int,
        default=0,
        help='Hour to run (0-23, for daemon mode)'
    )

    args = parser.parse_args()

    if args.once:
        run_pipeline(args.indications)
        return

    if args.daemon:
        try:
            import schedule
        except ImportError:
            logger.error("Schedule library not installed. Run: pip install schedule")
            sys.exit(1)

        # Schedule weekly run
        day_method = getattr(schedule.every(), args.day)
        day_method.at(f"{args.hour:02d}:00").do(run_pipeline, args.indications)

        logger.info(f"Scheduler started. Pipeline will run every {args.day} at {args.hour:02d}:00")
        logger.info("Press Ctrl+C to stop")

        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    else:
        # Default: run once
        run_pipeline(args.indications)


if __name__ == '__main__':
    main()
