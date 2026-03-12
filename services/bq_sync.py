import os
import logging
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from google.cloud import bigquery
from dotenv import load_dotenv
from typing import List
from datetime import datetime, date, time

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bq_sync")

load_dotenv()

class BigQuerySync:
    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL")
        self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        self.dataset_id = os.getenv("BIGQUERY_DATASET", "athlete_analysis")
        
        if not self.db_url:
            raise ValueError("DATABASE_URL environment variable is not set")
        if not self.project_id:
            raise ValueError("GOOGLE_CLOUD_PROJECT environment variable is not set")

        self.bq_client = bigquery.Client(project=self.project_id)
        
    def get_postgres_connection(self):
        return psycopg2.connect(self.db_url, cursor_factory=RealDictCursor)

    def json_serializer(self, obj):
        """JSON serializer for objects not serializable by default json code"""
        if isinstance(obj, (datetime, date, time)):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    def get_sort_column(self, cur, table_name):
        """Detect the best column to sort by."""
        cur.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            AND column_name IN ('created_at', 'start_date', 'id', 'date')
            ORDER BY CASE column_name 
                WHEN 'created_at' THEN 1 
                WHEN 'start_date' THEN 2 
                WHEN 'date' THEN 3 
                WHEN 'id' THEN 4 
            END ASC 
            LIMIT 1
        """)
        res = cur.fetchone()
        return res['column_name'] if res else None

    def sync_table(self, table_name: str, write_disposition: str = "WRITE_TRUNCATE"):
        """
        Syncs a single table from Postgres to BigQuery without using Pandas.
        Includes automatic sorting and support for TIME types.
        """
        logger.info(f"Starting sync for table: {table_name}")
        
        try:
            # 1. Fetch data from Postgres
            conn = self.get_postgres_connection()
            cur = conn.cursor()
            
            # Find a column to sort by for better sequence in BQ
            sort_col = self.get_sort_column(cur, table_name)
            order_by = f'ORDER BY "{sort_col}" ASC' if sort_col else ""
            
            logger.info(f"Fetching data from {table_name} {order_by}...")
            cur.execute(f'SELECT * FROM "{table_name}" {order_by}')
            rows = cur.fetchall()
            cur.close()
            conn.close()
            
            if not rows:
                logger.warning(f"No data found in Postgres for table {table_name}. Skipping.")
                return

            # Convert rows to list of dicts with JSON-safe types
            data = json.loads(json.dumps(rows, default=self.json_serializer))

            # 2. Define BQ table ID
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name.lower()}"
            
            # 3. Configure Load Job
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                autodetect=True,
            )
            
            # 4. Load into BigQuery
            logger.info(f"Loading {len(data)} rows into BigQuery table {table_id}...")
            job = self.bq_client.load_table_from_json(
                data, table_id, job_config=job_config
            )
            
            job.result()  # Wait for the job to complete
            logger.info(f"Successfully synced {table_name} to {table_id}")
            
        except Exception as e:
            logger.error(f"Failed to sync table {table_name}: {e}")
            raise

    def run_full_sync(self, tables: List[str]):
        """
        Runs sync for a list of tables.
        """
        logger.info(f"Starting full sync for {len(tables)} tables...")
        for table in tables:
            self.sync_table(table, write_disposition="WRITE_TRUNCATE")
        logger.info("All tables synced successfully.")

if __name__ == "__main__":
    TABLES_TO_SYNC = [
        "athletes", 
        "activities", 
        "race_results", 
        "personal_details", 
        "coach_session_logs", 
        "meal_logs", 
        "wellness_logs",
        "challenge_activities", 
        "challenge_participants"
    ]
    
    sync_manager = BigQuerySync()
    sync_manager.run_full_sync(TABLES_TO_SYNC)
