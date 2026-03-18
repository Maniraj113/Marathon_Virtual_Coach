import os
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

def setup_table():
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "athlete-analyzer-479515")
    dataset_id = "athlete_analysis_us"
    table_id = "coach_analysis_sessions"
    
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    schema = [
        bigquery.SchemaField("strava_athlete_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("strava_activity_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("session_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cached_analysis", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("activity_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
    ]
    
    table = bigquery.Table(table_ref, schema=schema)
    table.description = "Stores coach analysis sessions per athlete+activity to avoid re-analysis on reload."
    
    try:
        client.get_table(table_ref)
        print(f"Table {table_id} already exists.")
    except Exception:
        print(f"Creating table {table_id}...")
        client.create_table(table)
        print(f"Table {table_id} created successfully.")

if __name__ == "__main__":
    setup_table()
