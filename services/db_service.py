"""
Database Service - BigQuery interface for athlete data.

This service provides:
1. SQL query execution for athlete profile retrieval.
"""

import os
import logging
from typing import Optional, Dict, Any, List

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
        sql = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.athlete_profile` WHERE strava_id = {strava_id} LIMIT 1"
        results = self.query(sql)
        return results[0] if results else None

    @property
    def full_table_id(self) -> str:
        """Return the fully qualified table name."""
        return f"{self.project_id}.{self.dataset_id}.{self.table_name}"
    
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
