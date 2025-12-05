"""
Configuration settings for the Clinical Trials Protocol Pipeline.
"""
import os
from pathlib import Path
from datetime import datetime

# Base directories
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
PROTOCOLS_DIR = BASE_DIR / "protocols"

# Database
DATABASE_PATH = DATA_DIR / "protocols.db"

# ClinicalTrials.gov API settings
CT_GOV_API_BASE = "https://clinicaltrials.gov/api/v2"
CT_GOV_STUDIES_ENDPOINT = f"{CT_GOV_API_BASE}/studies"
CT_GOV_STUDY_ENDPOINT = f"{CT_GOV_API_BASE}/studies"

# Search parameters
CURRENT_YEAR = datetime.now().year
START_YEAR = CURRENT_YEAR - 20  # Last 20 years
PAGE_SIZE = 100  # Max allowed by API
REQUEST_DELAY = 1.5  # Seconds between requests (to respect rate limits)

# Default indications to process
DEFAULT_INDICATIONS = [
    "obesity",
    "prostate cancer",
    "lung cancer"
]

# Fields to request from API
API_FIELDS = [
    "NCTId",
    "OfficialTitle",
    "BriefTitle",
    "OverallStatus",
    "Phase",
    "StartDate",
    "CompletionDate",
    "LeadSponsorName",
    "LeadSponsorClass",
    "Condition",
    "InterventionName",
    "InterventionType",
    "EnrollmentCount",
    "StudyType",
    "DocumentSection"
]

# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
PROTOCOLS_DIR.mkdir(parents=True, exist_ok=True)
