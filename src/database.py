"""
Database module for managing clinical trial protocol metadata.
Uses SQLite for simple, file-based storage.
"""
import sqlite3
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime
from contextlib import contextmanager

from .config import DATABASE_PATH


class ProtocolDatabase:
    """SQLite database for storing clinical trial protocol metadata."""

    def __init__(self, db_path: Path = DATABASE_PATH):
        self.db_path = db_path
        self._init_database()

    @contextmanager
    def _get_connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def _init_database(self):
        """Initialize database schema."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Main protocols table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS protocols (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nct_id TEXT UNIQUE NOT NULL,
                    official_title TEXT,
                    brief_title TEXT,
                    sponsor TEXT,
                    sponsor_class TEXT,
                    year INTEGER,
                    start_date TEXT,
                    completion_date TEXT,
                    indication TEXT,
                    conditions TEXT,
                    phase TEXT,
                    study_type TEXT,
                    overall_status TEXT,
                    enrollment INTEGER,
                    interventions TEXT,
                    protocol_url TEXT,
                    protocol_pdf_path TEXT,
                    has_protocol_doc BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Index for faster queries
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_indication
                ON protocols(indication)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_nct_id
                ON protocols(nct_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_year
                ON protocols(year)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_phase
                ON protocols(phase)
            """)

            # Download tracking table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS download_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    indication TEXT NOT NULL,
                    download_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    studies_found INTEGER,
                    protocols_downloaded INTEGER,
                    new_studies INTEGER,
                    updated_studies INTEGER,
                    status TEXT
                )
            """)

    def upsert_protocol(self, protocol_data: Dict[str, Any]) -> bool:
        """Insert or update a protocol record. Returns True if new, False if updated."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Check if exists
            cursor.execute(
                "SELECT id FROM protocols WHERE nct_id = ?",
                (protocol_data['nct_id'],)
            )
            existing = cursor.fetchone()

            if existing:
                # Update existing record
                cursor.execute("""
                    UPDATE protocols SET
                        official_title = ?,
                        brief_title = ?,
                        sponsor = ?,
                        sponsor_class = ?,
                        year = ?,
                        start_date = ?,
                        completion_date = ?,
                        indication = ?,
                        conditions = ?,
                        phase = ?,
                        study_type = ?,
                        overall_status = ?,
                        enrollment = ?,
                        interventions = ?,
                        protocol_url = ?,
                        protocol_pdf_path = ?,
                        has_protocol_doc = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE nct_id = ?
                """, (
                    protocol_data.get('official_title'),
                    protocol_data.get('brief_title'),
                    protocol_data.get('sponsor'),
                    protocol_data.get('sponsor_class'),
                    protocol_data.get('year'),
                    protocol_data.get('start_date'),
                    protocol_data.get('completion_date'),
                    protocol_data.get('indication'),
                    protocol_data.get('conditions'),
                    protocol_data.get('phase'),
                    protocol_data.get('study_type'),
                    protocol_data.get('overall_status'),
                    protocol_data.get('enrollment'),
                    protocol_data.get('interventions'),
                    protocol_data.get('protocol_url'),
                    protocol_data.get('protocol_pdf_path'),
                    protocol_data.get('has_protocol_doc', False),
                    protocol_data['nct_id']
                ))
                return False
            else:
                # Insert new record
                cursor.execute("""
                    INSERT INTO protocols (
                        nct_id, official_title, brief_title, sponsor, sponsor_class,
                        year, start_date, completion_date, indication, conditions,
                        phase, study_type, overall_status, enrollment, interventions,
                        protocol_url, protocol_pdf_path, has_protocol_doc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    protocol_data['nct_id'],
                    protocol_data.get('official_title'),
                    protocol_data.get('brief_title'),
                    protocol_data.get('sponsor'),
                    protocol_data.get('sponsor_class'),
                    protocol_data.get('year'),
                    protocol_data.get('start_date'),
                    protocol_data.get('completion_date'),
                    protocol_data.get('indication'),
                    protocol_data.get('conditions'),
                    protocol_data.get('phase'),
                    protocol_data.get('study_type'),
                    protocol_data.get('overall_status'),
                    protocol_data.get('enrollment'),
                    protocol_data.get('interventions'),
                    protocol_data.get('protocol_url'),
                    protocol_data.get('protocol_pdf_path'),
                    protocol_data.get('has_protocol_doc', False)
                ))
                return True

    def update_pdf_path(self, nct_id: str, pdf_path: str):
        """Update the PDF path for a protocol."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE protocols
                SET protocol_pdf_path = ?, has_protocol_doc = 1, updated_at = CURRENT_TIMESTAMP
                WHERE nct_id = ?
            """, (pdf_path, nct_id))

    def get_protocol(self, nct_id: str) -> Optional[Dict]:
        """Get a single protocol by NCT ID."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM protocols WHERE nct_id = ?", (nct_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_protocols_by_indication(self, indication: str) -> List[Dict]:
        """Get all protocols for a specific indication."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM protocols WHERE indication = ? ORDER BY year DESC",
                (indication,)
            )
            return [dict(row) for row in cursor.fetchall()]

    def get_protocols_without_pdf(self, indication: Optional[str] = None) -> List[Dict]:
        """Get protocols that have a protocol URL but no downloaded PDF."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if indication:
                cursor.execute("""
                    SELECT * FROM protocols
                    WHERE protocol_url IS NOT NULL
                    AND (protocol_pdf_path IS NULL OR protocol_pdf_path = '')
                    AND indication = ?
                """, (indication,))
            else:
                cursor.execute("""
                    SELECT * FROM protocols
                    WHERE protocol_url IS NOT NULL
                    AND (protocol_pdf_path IS NULL OR protocol_pdf_path = '')
                """)
            return [dict(row) for row in cursor.fetchall()]

    def log_download(self, indication: str, studies_found: int,
                     protocols_downloaded: int, new_studies: int,
                     updated_studies: int, status: str):
        """Log a download run."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO download_history
                (indication, studies_found, protocols_downloaded, new_studies, updated_studies, status)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (indication, studies_found, protocols_downloaded, new_studies, updated_studies, status))

    def get_download_history(self, indication: Optional[str] = None, limit: int = 10) -> List[Dict]:
        """Get download history."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if indication:
                cursor.execute("""
                    SELECT * FROM download_history
                    WHERE indication = ?
                    ORDER BY download_date DESC LIMIT ?
                """, (indication, limit))
            else:
                cursor.execute("""
                    SELECT * FROM download_history
                    ORDER BY download_date DESC LIMIT ?
                """, (limit,))
            return [dict(row) for row in cursor.fetchall()]

    def get_statistics(self, indication: Optional[str] = None) -> Dict:
        """Get summary statistics."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            if indication:
                cursor.execute("""
                    SELECT
                        COUNT(*) as total_studies,
                        SUM(CASE WHEN has_protocol_doc = 1 THEN 1 ELSE 0 END) as with_protocols,
                        COUNT(DISTINCT phase) as phase_count,
                        MIN(year) as earliest_year,
                        MAX(year) as latest_year
                    FROM protocols WHERE indication = ?
                """, (indication,))
            else:
                cursor.execute("""
                    SELECT
                        COUNT(*) as total_studies,
                        SUM(CASE WHEN has_protocol_doc = 1 THEN 1 ELSE 0 END) as with_protocols,
                        COUNT(DISTINCT indication) as indication_count,
                        MIN(year) as earliest_year,
                        MAX(year) as latest_year
                    FROM protocols
                """)

            row = cursor.fetchone()
            return dict(row) if row else {}

    def get_all_indications(self) -> List[str]:
        """Get list of all indications in database."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT indication FROM protocols ORDER BY indication")
            return [row['indication'] for row in cursor.fetchall()]

    def search_protocols(self, query: str, indication: Optional[str] = None) -> List[Dict]:
        """Search protocols by title or NCT ID."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            search_pattern = f"%{query}%"

            if indication:
                cursor.execute("""
                    SELECT * FROM protocols
                    WHERE indication = ? AND (
                        nct_id LIKE ? OR
                        official_title LIKE ? OR
                        brief_title LIKE ? OR
                        conditions LIKE ?
                    )
                    ORDER BY year DESC
                """, (indication, search_pattern, search_pattern, search_pattern, search_pattern))
            else:
                cursor.execute("""
                    SELECT * FROM protocols
                    WHERE nct_id LIKE ? OR
                          official_title LIKE ? OR
                          brief_title LIKE ? OR
                          conditions LIKE ?
                    ORDER BY year DESC
                """, (search_pattern, search_pattern, search_pattern, search_pattern))

            return [dict(row) for row in cursor.fetchall()]
