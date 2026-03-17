-- BigQuery Table Recovery / Creation Script
-- You mentioned deleting tables by mistake. Fortunately, BigQuery has a "Time Travel" feature 
-- that lets you restore tables up to 7 days after deletion.

-- OPTION 1 (RECOMMENDED): RECOVER DELETED TABLES USING TIME TRAVEL
-- Run these commands in your Google Cloud Shell or terminal (assuming gcloud is authenticated):
-- Adjust the timestamp (e.g., -3600000 means 1 hour ago in milliseconds. -86400000 is 24 hours ago).

/*
bq cp "athlete-analyzer-479515:athlete_analysis.coach_analysis_sessions@-3600000" athlete-analyzer-479515:athlete_analysis.coach_analysis_sessions
bq cp "athlete-analyzer-479515:athlete_analysis.personal_details@-3600000" athlete-analyzer-479515:athlete_analysis.personal_details
bq cp "athlete-analyzer-479515:athlete_analysis.activities@-3600000" athlete-analyzer-479515:athlete_analysis.activities
*/


-- OPTION 2: RECREATE TABES MANUALLY
-- If time travel doesn't work, you can recreate the 'coach_analysis_sessions' table manually.
-- Note: 'personal_details' and 'activities' are usually synced from Postgres. If you don't run bq_sync, 
-- you will need to at least create the 'personal_details' table, otherwise the AI profile query will crash 
-- because it tries to join with it to get your Date of Birth.

-- 1. Recreate coach_analysis_sessions
CREATE TABLE IF NOT EXISTS `athlete-analyzer-479515.athlete_analysis.coach_analysis_sessions` (
    strava_athlete_id STRING,
    strava_activity_id INT64,
    session_id STRING,
    cached_analysis STRING,
    activity_type STRING,
    created_at TIMESTAMP,
    activity_name STRING,
    activity_date TIMESTAMP
);

-- 2. Recreate personal_details (Required for joining age!)
CREATE TABLE IF NOT EXISTS `athlete-analyzer-479515.athlete_analysis.personal_details` (
    athlete_id INT64,
    date_of_birth TIMESTAMP,
    gender STRING,
    weight FLOAT64,
    height FLOAT64
);

-- (Optional) If you also need 'activities' manually:
CREATE TABLE IF NOT EXISTS `athlete-analyzer-479515.athlete_analysis.activities` (
    id INT64,
    athlete_id INT64,
    name STRING,
    distance FLOAT64,
    moving_time INT64,
    elapsed_time INT64,
    total_elevation_gain FLOAT64,
    type STRING,
    start_date TIMESTAMP,
    average_speed FLOAT64,
    max_speed FLOAT64,
    average_heartrate FLOAT64,
    max_heartrate FLOAT64
);
