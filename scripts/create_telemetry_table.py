from google.cloud import bigquery
from dotenv import load_dotenv
import os

load_dotenv()
client = bigquery.Client(project=os.getenv("GOOGLE_CLOUD_PROJECT"))
schema = [
    bigquery.SchemaField("strava_athlete_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("session_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("model", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("prompt_tokens", "INT64"),
    bigquery.SchemaField("candidate_tokens", "INT64"),
    bigquery.SchemaField("total_tokens", "INT64"),
    bigquery.SchemaField("turn_latency_ms", "INT64"),
    bigquery.SchemaField("total_processing_ms", "INT64"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED")
]
project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
table_id = f"{project_id}.athlete_analysis.coach_telemetry"
table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table, exists_ok=True)
print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
