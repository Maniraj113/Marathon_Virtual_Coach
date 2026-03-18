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
        self.dataset_id = "athlete_analysis_us"
        self.table_name = "activities"
        self.sessions_table = "coach_analysis_sessions"
        self.chat_table = "coach_chat_history"
        self.memory_table = "athlete_long_term_memory"
        self.telemetry_table = "coach_telemetry"
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
        sql = f"""
            SELECT a.*, p.date_of_birth 
            FROM `{self.project_id}.{self.dataset_id}.athletes` a
            LEFT JOIN `{self.project_id}.{self.dataset_id}.personal_details` p 
              ON a.strava_id = p.athlete_id
            WHERE a.strava_id = {strava_id} 
            LIMIT 1
        """
        results = self.query(sql)
        if not results:
            return None
        profile = results[0]
        if profile.get("date_of_birth"):
            dob = profile["date_of_birth"]
            if isinstance(dob, str):
                try:
                    from datetime import datetime
                    dob = datetime.fromisoformat(dob[:10]).date()
                except ValueError:
                    pass
            from datetime import date
            if isinstance(dob, date) and dob.year != 9999:
                today = date.today()
                age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
                profile["age"] = age
            else:
                profile["age"] = None
        return profile

    @property
    def full_table_id(self) -> str:
        """Return the fully qualified table name."""
        return f"{self.project_id}.{self.dataset_id}.{self.table_name}"

    @property
    def sessions_table_id(self) -> str:
        """Return the fully qualified sessions table name."""
        return f"{self.project_id}.{self.dataset_id}.{self.sessions_table}"
    @property
    def chat_table_id(self) -> str:
        """Return the fully qualified chat table name."""
        return f"{self.project_id}.{self.dataset_id}.{self.chat_table}"

    @property
    def memory_table_id(self) -> str:
        """Return the fully qualified memory table name."""
        return f"{self.project_id}.{self.dataset_id}.{self.memory_table}"
        
    @property
    def telemetry_table_id(self) -> str:
        """Return the fully qualified telemetry table name."""
        return f"{self.project_id}.{self.dataset_id}.{self.telemetry_table}"

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
        activity_name: str = "Activity",
        activity_date: str = None
    ) -> bool:
        """
        Upsert an analysis session record in BigQuery.
        Directly stores name and date to avoid reliance on backup tables.
        """
        if not self.is_connected:
            return False

        try:
            # Escape strings
            safe_athlete_id = str(strava_athlete_id).replace("'", "\\'")
            safe_session_id = str(session_id).replace("'", "\\'")
            safe_analysis   = cached_analysis.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "")
            safe_type       = str(activity_type).replace("'", "\\'")
            safe_name       = str(activity_name).replace("'", "\\'")
            
            # Date handling
            if activity_date:
                # Assuming incoming ISO string or similar
                date_val = f"TIMESTAMP '{activity_date}'"
            else:
                date_val = "NULL"

            now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

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
                    (strava_athlete_id, strava_activity_id, session_id, cached_analysis, activity_type, created_at, activity_name, activity_date)
                VALUES
                    ('{safe_athlete_id}', {strava_activity_id}, '{safe_session_id}', '{safe_analysis}', '{safe_type}', TIMESTAMP '{now_ts}', '{safe_name}', {date_val})
            """
            self.client.query(insert_sql).result()
            logger.info(f"[Session] Saved session={session_id} for {activity_name}")
            return True

        except Exception as e:
            logger.error(f"[Session] Failed to save session: {e}")
            return False

    def list_analysis_sessions(self, strava_athlete_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        List recent analysis sessions for an athlete using internal metadata.
        """
        if not self.is_connected:
            return []

        sql = f"""
            SELECT session_id, strava_activity_id, activity_type, created_at, activity_name, activity_date
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
                "activity_name": r.get("activity_name", f"Activity {r.get('strava_activity_id')}"),
                "activity_date": str(r.get("activity_date", "")),
            }
            for r in results
        ]

    # ── Chat History Methods ──────────────────────────────────────────────────

    def save_chat_message(
        self,
        strava_athlete_id: str,
        session_id: str,
        role: str,
        content: str,
    ) -> bool:
        """
        Save a single chat message to BigQuery.
        """
        if not self.is_connected:
            return False

        try:
            from google.cloud import bigquery
            now_dt = datetime.now(timezone.utc)

            query = f"""
                INSERT INTO `{self.chat_table_id}`
                  (strava_athlete_id, session_id, role, content, created_at)
                VALUES
                  (@athlete_id, @session_id, @role, @content, @created_at)
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("athlete_id", "STRING", str(strava_athlete_id)),
                    bigquery.ScalarQueryParameter("session_id", "STRING", str(session_id)),
                    bigquery.ScalarQueryParameter("role", "STRING", str(role)),
                    bigquery.ScalarQueryParameter("content", "STRING", str(content)),
                    bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", now_dt),
                ]
            )
            
            self.client.query(query, job_config=job_config).result()
            return True
        except Exception as e:
            logger.error(f"[Chat] Failed to save message: {e}")
            return False

    def list_chat_messages(self, session_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Retrieve chat history for a specific session.
        """
        sql = f"""
            SELECT role, content, created_at
            FROM `{self.chat_table_id}`
            WHERE session_id = '{session_id}'
            ORDER BY created_at ASC
            LIMIT {limit}
        """
        results = self.query(sql)
        return results if results else []

    # ── Telemetry Methods ─────────────────────────────────────────────────────
    
    def save_telemetry(
        self,
        strava_athlete_id: str,
        session_id: str,
        model: str,
        prompt_tokens: int,
        candidate_tokens: int,
        total_tokens: int,
        turn_latency_ms: int,
        total_processing_ms: int
    ) -> bool:
        """
        Save conversational token and latency metrics to BigQuery for analytics.
        """
        if not self.is_connected:
            return False
            
        try:
            from google.cloud import bigquery
            now_dt = datetime.now(timezone.utc)
            
            query = f"""
                INSERT INTO `{self.telemetry_table_id}`
                  (strava_athlete_id, session_id, model, prompt_tokens, candidate_tokens, total_tokens, turn_latency_ms, total_processing_ms, created_at)
                VALUES
                  (@athlete_id, @session_id, @model, @prompt, @candidate, @total_tok, @latency, @total_latency, @created_at)
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("athlete_id", "STRING", str(strava_athlete_id)),
                    bigquery.ScalarQueryParameter("session_id", "STRING", str(session_id)),
                    bigquery.ScalarQueryParameter("model", "STRING", str(model)),
                    bigquery.ScalarQueryParameter("prompt", "INT64", prompt_tokens),
                    bigquery.ScalarQueryParameter("candidate", "INT64", candidate_tokens),
                    bigquery.ScalarQueryParameter("total_tok", "INT64", total_tokens),
                    bigquery.ScalarQueryParameter("latency", "INT64", turn_latency_ms),
                    bigquery.ScalarQueryParameter("total_latency", "INT64", total_processing_ms),
                    bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", now_dt),
                ]
            )
            self.client.query(query, job_config=job_config).result()
            return True
        except Exception as e:
            logger.error(f"[Telemetry] Failed to save telemetry: {e}")
            return False

    # ── Memory Bank Methods Removed per user request ──────────────────────────
    
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

    async def update_athlete_profile(self, strava_athlete_id: str, age: int, yearly_goal: str, activity_preference: str) -> bool:
        """
        Update local Postgres profile and trigger BQ sync.
        """
        import asyncpg
        from services.bq_sync import BigQuerySync
        
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise Exception("Database URL not configured")
        
        try:
            conn = await asyncpg.connect(db_url)
            status = await conn.execute(
                "UPDATE athletes SET age = $1, yearly_goal = $2, activity_preference = $3, profile_completed = true WHERE strava_id = $4",
                age, yearly_goal, activity_preference, strava_athlete_id
            )
            await conn.close()
            
            if status == 'UPDATE 0':
                return False
                
            # Sync immediately to BigQuery so the coaching agent sees the newest values
            sync_manager = BigQuerySync()
            sync_manager.sync_table("athletes")
            
            return True
        except Exception as e:
            logger.error(f"Failed to update profile locally: {e}")
            raise

# Singleton instance
_db_service: Optional[DatabaseService] = None


def get_db_service() -> DatabaseService:
    """Get the singleton database service instance."""
    global _db_service
    if _db_service is None:
        _db_service = DatabaseService()
    return _db_service
