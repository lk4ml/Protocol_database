# Clinical Trials Protocol Database Pipeline

A pipeline to download clinical trial protocols from ClinicalTrials.gov and build a searchable database with metadata.

## Features

- Downloads protocols for any indication from the last 20 years
- Creates SQLite database with rich metadata (NCT ID, sponsor, year, phase, etc.)
- Organizes PDF protocols by indication in separate folders
- Supports weekly scheduled runs for updates
- Tracks download history and statistics
- Export to CSV for analysis

## Quick Start

```bash
# Install dependencies
pip install requests python-dateutil tqdm

# Run for default indications (obesity, prostate cancer, lung cancer)
python run_pipeline.py

# Run for specific indications
python run_pipeline.py --indications "lung cancer" "breast cancer" "diabetes"

# Run for a new indication
python run_pipeline.py --indications "alzheimer disease"
```

## Directory Structure

```
Protocol_database/
├── run_pipeline.py          # Main CLI entry point
├── schedule_weekly.py       # Weekly scheduler
├── requirements.txt         # Python dependencies
├── src/
│   ├── __init__.py
│   ├── config.py           # Configuration settings
│   ├── database.py         # SQLite database operations
│   ├── api_client.py       # ClinicalTrials.gov API client
│   └── downloader.py       # Protocol downloader logic
├── data/
│   ├── protocols.db        # SQLite database (created on first run)
│   └── pipeline.log        # Log file
└── protocols/
    ├── obesity/            # PDFs organized by indication
    ├── prostate_cancer/
    └── lung_cancer/
```

## Usage

### Basic Commands

```bash
# Process default indications
python run_pipeline.py

# Process specific indications
python run_pipeline.py --indications "lung cancer" "breast cancer"

# Metadata only (skip PDF downloads)
python run_pipeline.py --no-pdfs

# Limit studies per indication (for testing)
python run_pipeline.py --max-studies 50
```

### Database Operations

```bash
# Show database statistics
python run_pipeline.py --stats

# List all indications in database
python run_pipeline.py --list-indications

# Search protocols by keyword
python run_pipeline.py --search "immunotherapy"

# Show download history
python run_pipeline.py --history

# Export to CSV
python run_pipeline.py --export all_protocols.csv

# Export specific indication
python run_pipeline.py --export lung_cancer.csv --export-indication "lung cancer"
```

### Updating Existing Data

```bash
# Download missing PDFs for protocols that have URLs
python run_pipeline.py --download-missing
```

## Weekly Scheduling

### Option 1: Using the scheduler script

```bash
# Run as daemon (keeps running)
python schedule_weekly.py --daemon --day sunday --hour 2

# Run once and exit
python schedule_weekly.py --once
```

### Option 2: Using cron (recommended for production)

```bash
# Add to crontab (runs every Sunday at 2 AM)
crontab -e

# Add this line:
0 2 * * 0 cd /path/to/Protocol_database && /usr/bin/python3 run_pipeline.py >> data/weekly.log 2>&1
```

## Database Schema

The SQLite database contains the following fields:

| Field | Description |
|-------|-------------|
| `nct_id` | ClinicalTrials.gov NCT identifier |
| `official_title` | Full study title |
| `brief_title` | Short study title |
| `sponsor` | Lead sponsor name |
| `sponsor_class` | Sponsor type (INDUSTRY, NIH, etc.) |
| `year` | Study start year |
| `start_date` | Study start date |
| `completion_date` | Expected completion date |
| `indication` | Search indication (e.g., "lung cancer") |
| `conditions` | All conditions studied |
| `phase` | Trial phase (1, 2, 3, 4, N/A) |
| `study_type` | Interventional, Observational, etc. |
| `overall_status` | Recruiting, Completed, etc. |
| `enrollment` | Number of participants |
| `interventions` | Drugs/treatments being tested |
| `protocol_url` | URL to protocol PDF on CT.gov |
| `protocol_pdf_path` | Local path to downloaded PDF |
| `has_protocol_doc` | Whether protocol PDF exists |

## API Notes

- Uses ClinicalTrials.gov API v2
- Rate limited to ~50 requests/minute
- Pipeline includes automatic retry with exponential backoff
- Date range filter: last 20 years automatically

## Requirements

- Python 3.8+
- requests
- python-dateutil
- tqdm (optional, for progress bars)
- schedule (optional, for daemon mode)
