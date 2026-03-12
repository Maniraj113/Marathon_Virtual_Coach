"""
Database Service - BigQuery interface for athlete data.

This service provides:
1. SQL query execution for athlete profile retrieval.
2. Analysis session persistence (save/retrieve session_id + cached analysis per athlete+activity).
"""

import os
import logging
import json
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class DatabaseService:
    """
    Database service strictly for BigQuery.
    """
    
    def __init__(self):
        """
        Initialize BigQuery service.
        """
        self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        self.dataset_id = "athlete_analysis"
        self.table_name = "activities"
        self.sessions_table = "coach_analysis_sessions"
        self.client = None
        
        self._init_bigquery()
    
    def _init_bigquery(self):
        """
        Initialize BigQuery client.
        """
        if not self.project_id:
            logger.warning("GOOGLE_CLOUD_PROJECT not set - BigQuery disabled")
            return
        
        try:
            from google.cloud import bigquery
            self.client = bigquery.Client(project=self.project_id)
            logger.info(f"BigQuery connected: {self.project_id}")
        except ImportError:
            logger.error("Install: pip install google-cloud-bigquery")
        except Exception as e:
            logger.error(f"BigQuery init failed: {e}")
    
    @property
    def is_connected(self) -> bool:
        """Check if database is connected."""
        return self.client is not None
    
    def get_athlete_profile_by_strava_id(self, strava_id: Any) -> Optional[Dict[str, Any]]:
        """Get athlete profile from BigQuery by Strava ID."""
        sql = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.athletes` WHERE strava_id = {strava_id} LIMIT 1"
        results = self.query(sql)
        return results[0] if results else None

    @property
    def full_table_id(self) -> str:
        """Return the fully qualified table name."""
        return f"{self.project_id}.{self.dataset_id}.{self.table_name}"

    @property
    def sessions_table_id(self) -> str:
        """Return the fully qualified sessions table name."""
        return f"{self.project_id}.{self.dataset_id}.{self.sessions_table}"

    # ── Analysis Session Methods ─────────────────────────────────────────────────

    def get_analysis_session(self, strava_athlete_id: str, strava_activity_id: int) -> Optional[Dict[str, Any]]:
        """
        Retrieve an existing analysis session for a specific athlete + activity.
        Returns: {session_id, cached_analysis, activity_type, created_at} or None.
        """
        sql = f"""
            SELECT session_id, cached_analysis, activity_type, created_at
            FROM `{self.sessions_table_id}`
            WHERE strava_athlete_id = '{strava_athlete_id}'
              AND strava_activity_id = {strava_activity_id}
            ORDER BY created_at DESC
            LIMIT 1
        """
        results = self.query(sql)
        if results:
            row = results[0]
            return {
                "session_id": row.get("session_id"),
                "cached_analysis": row.get("cached_analysis"),
                "activity_type": row.get("activity_type", "training"),
                "created_at": str(row.get("created_at", "")),
            }
        return None

    def save_analysis_session(
        self,
        strava_athlete_id: str,
        strava_activity_id: int,
        session_id: str,
        cached_analysis: str,
        activity_type: str = "training",
    ) -> bool:
        """
        Upsert an analysis session record in BigQuery.
        Strategy: DELETE existing rows for this athlete+activity, then INSERT new one.
        Returns True on success.
        """
        if not self.is_connected:
            logger.error("BigQuery not connected — cannot save session.")
            return False

        try:
            from google.cloud import bigquery

            # Escape strings for BQ DML
            safe_athlete_id  = str(strava_athlete_id).replace("'", "\\'")
            safe_session_id  = str(session_id).replace("'", "\\'")
            safe_analysis    = cached_analysis.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "")
            safe_type        = str(activity_type).replace("'", "\\'")
            now_ts           = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

            # Step 1: delete old rows
            delete_sql = f"""
                DELETE FROM `{self.sessions_table_id}`
                WHERE strava_athlete_id = '{safe_athlete_id}'
                  AND strava_activity_id = {strava_activity_id}
            """
            self.client.query(delete_sql).result()

            # Step 2: insert new row
            insert_sql = f"""
                INSERT INTO `{self.sessions_table_id}`
                  (strava_athlete_id, strava_activity_id, session_id, cached_analysis, activity_type, created_at)
                VALUES
                  ('{safe_athlete_id}', {strava_activity_id}, '{safe_session_id}', '{safe_analysis}', '{safe_type}', TIMESTAMP '{now_ts}')
            """
            self.client.query(insert_sql).result()
            logger.info(f"[Session] Saved session={session_id} for activity={strava_activity_id}")
            return True

        except Exception as e:
            logger.error(f"[Session] Failed to save session: {e}")
            return False

    def list_analysis_sessions(self, strava_athlete_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        List recent analysis sessions for an athlete (for history sidebar).
        Returns list of {session_id, strava_activity_id, activity_type, created_at}.
        """
        sql = f"""
            SELECT session_id, strava_activity_id, activity_type, created_at
            FROM `{self.sessions_table_id}`
            WHERE strava_athlete_id = '{strava_athlete_id}'
            ORDER BY created_at DESC
            LIMIT {limit}
        """
        results = self.query(sql)
        if not results:
            return []
        return [
            {
                "session_id": r.get("session_id"),
                "strava_activity_id": r.get("strava_activity_id"),
                "activity_type": r.get("activity_type", "training"),
                "created_at": str(r.get("created_at", "")),
            }
            for r in results
        ]
    
    def query(self, sql: str) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a SQL query and return results as a list of dictionaries.
        """
        if not self.is_connected:
            logger.error("Database not connected")
            return None
        
        try:
            # Execute query and convert to list of dicts
            query_job = self.client.query(sql)
            rows = query_job.result()
            
            # Convert to list of dictionaries
            data = [dict(row) for row in rows]
            return data
        
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return None

# Singleton instance
_db_service: Optional[DatabaseService] = None


def get_db_service() -> DatabaseService:
    """Get the singleton database service instance."""
    global _db_service
    if _db_service is None:
        _db_service = DatabaseService()
    return _db_service
