"""
ClinicalTrials.gov API v2 Client for fetching study data.
"""
import requests
import time
import logging
from typing import Optional, List, Dict, Any, Generator
from datetime import datetime
from dateutil.parser import parse as parse_date

from .config import (
    CT_GOV_STUDIES_ENDPOINT,
    PAGE_SIZE,
    REQUEST_DELAY,
    START_YEAR,
    CURRENT_YEAR
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClinicalTrialsAPI:
    """Client for ClinicalTrials.gov API v2."""

    def __init__(self):
        self.base_url = CT_GOV_STUDIES_ENDPOINT
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'ProtocolDatabasePipeline/1.0'
        })

    def _make_request(self, params: Dict[str, Any], retries: int = 3) -> Optional[Dict]:
        """Make API request with retry logic."""
        for attempt in range(retries):
            try:
                response = self.session.get(self.base_url, params=params, timeout=30)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"All retries failed: {e}")
                    return None
        return None

    def search_studies(
        self,
        condition: str,
        start_year: int = START_YEAR,
        end_year: int = CURRENT_YEAR,
        page_size: int = PAGE_SIZE
    ) -> Generator[Dict, None, None]:
        """
        Search for studies by condition within a date range.
        Yields study data one at a time, handling pagination automatically.
        """
        # Build date filter for the last 20 years
        date_filter = f"AREA[StartDate]RANGE[{start_year}-01-01,{end_year}-12-31]"

        params = {
            'query.cond': condition,
            'filter.advanced': date_filter,
            'pageSize': page_size,
            'countTotal': 'true',
            'format': 'json'
        }

        page_token = None
        total_fetched = 0

        while True:
            if page_token:
                params['pageToken'] = page_token

            logger.info(f"Fetching studies for '{condition}' (fetched so far: {total_fetched})")
            data = self._make_request(params)

            if not data:
                logger.error("Failed to fetch data from API")
                break

            studies = data.get('studies', [])
            total_count = data.get('totalCount', 0)

            if not studies:
                break

            for study in studies:
                yield self._parse_study(study, condition)
                total_fetched += 1

            logger.info(f"Progress: {total_fetched}/{total_count} studies")

            # Check for next page
            page_token = data.get('nextPageToken')
            if not page_token:
                break

            # Rate limiting
            time.sleep(REQUEST_DELAY)

        logger.info(f"Completed fetching {total_fetched} studies for '{condition}'")

    def _parse_study(self, study_data: Dict, indication: str) -> Dict:
        """Parse raw study data into our protocol format."""
        protocol_section = study_data.get('protocolSection', {})

        # Identification module
        id_module = protocol_section.get('identificationModule', {})
        nct_id = id_module.get('nctId', '')
        official_title = id_module.get('officialTitle', '')
        brief_title = id_module.get('briefTitle', '')

        # Status module
        status_module = protocol_section.get('statusModule', {})
        overall_status = status_module.get('overallStatus', '')
        start_date_struct = status_module.get('startDateStruct', {})
        completion_date_struct = status_module.get('completionDateStruct', {})

        start_date = start_date_struct.get('date', '')
        completion_date = completion_date_struct.get('date', '')

        # Extract year from start date
        year = None
        if start_date:
            try:
                parsed_date = parse_date(start_date)
                year = parsed_date.year
            except (ValueError, TypeError):
                pass

        # Sponsor module
        sponsor_module = protocol_section.get('sponsorCollaboratorsModule', {})
        lead_sponsor = sponsor_module.get('leadSponsor', {})
        sponsor_name = lead_sponsor.get('name', '')
        sponsor_class = lead_sponsor.get('class', '')

        # Design module
        design_module = protocol_section.get('designModule', {})
        study_type = design_module.get('studyType', '')
        phases = design_module.get('phases', [])
        phase = ', '.join(phases) if phases else 'N/A'

        enrollment_info = design_module.get('enrollmentInfo', {})
        enrollment = enrollment_info.get('count', 0)

        # Conditions module
        conditions_module = protocol_section.get('conditionsModule', {})
        conditions = conditions_module.get('conditions', [])
        conditions_str = '; '.join(conditions) if conditions else ''

        # Interventions module
        arms_module = protocol_section.get('armsInterventionsModule', {})
        interventions = arms_module.get('interventions', [])
        intervention_names = [
            f"{i.get('type', '')}: {i.get('name', '')}"
            for i in interventions
        ]
        interventions_str = '; '.join(intervention_names) if intervention_names else ''

        # Documents module - look for protocol documents
        document_section = study_data.get('documentSection', {})
        large_docs = document_section.get('largeDocumentModule', {})
        large_doc_list = large_docs.get('largeDocs', [])

        protocol_url = None
        for doc in large_doc_list:
            # Look for protocol documents
            label = doc.get('label', '').lower()
            if 'protocol' in label or 'sap' in label.lower():
                filename = doc.get('filename', '')
                if filename:
                    protocol_url = f"https://clinicaltrials.gov/ProvidedDocs/{nct_id[-2:]}/{nct_id}/{filename}"
                    break

        # Also check for hasProtocol flag
        has_results = study_data.get('hasResults', False)
        derived_section = study_data.get('derivedSection', {})

        return {
            'nct_id': nct_id,
            'official_title': official_title,
            'brief_title': brief_title,
            'sponsor': sponsor_name,
            'sponsor_class': sponsor_class,
            'year': year,
            'start_date': start_date,
            'completion_date': completion_date,
            'indication': indication,
            'conditions': conditions_str,
            'phase': phase,
            'study_type': study_type,
            'overall_status': overall_status,
            'enrollment': enrollment,
            'interventions': interventions_str,
            'protocol_url': protocol_url,
            'has_protocol_doc': protocol_url is not None
        }

    def get_study_details(self, nct_id: str) -> Optional[Dict]:
        """Get detailed information for a specific study."""
        url = f"{self.base_url}/{nct_id}"
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            study_data = response.json()
            return self._parse_study(study_data, '')
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch study {nct_id}: {e}")
            return None

    def get_total_count(self, condition: str, start_year: int = START_YEAR, end_year: int = CURRENT_YEAR) -> int:
        """Get total count of studies for a condition."""
        date_filter = f"AREA[StartDate]RANGE[{start_year}-01-01,{end_year}-12-31]"

        params = {
            'query.cond': condition,
            'filter.advanced': date_filter,
            'pageSize': 1,
            'countTotal': 'true',
            'format': 'json'
        }

        data = self._make_request(params)
        return data.get('totalCount', 0) if data else 0
