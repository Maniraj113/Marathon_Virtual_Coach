@echo off
echo ===================================================
echo 🏃 Athlete Analyzer - Cloud Run Deployment 🏃
echo ===================================================

SET PROJECT_ID=athlete-analyzer-479515
SET REGION=us-central1
SET SERVICE_NAME=athlete-analyzer-api

echo 🔍 Setting project to %PROJECT_ID%...
call gcloud config set project %PROJECT_ID%

echo 🚀 Deploying to Cloud Run...
cd ..
call gcloud run deploy %SERVICE_NAME% ^
  --source . ^
  --region %REGION% ^
  --allow-unauthenticated ^
  --set-env-vars "GOOGLE_CLOUD_PROJECT=athlete-analyzer-479515,GOOGLE_CLOUD_LOCATION=us-central1,VERTEX_AGENT_ENGINE_ID=4318546322857656320,COACH_AGENT_MODEL=gemini-2.5-flash-lite,ROOT_AGENT_MODEL=gemini-2.5-flash-lite,STRAVA_CLIENT_ID=39122,STRAVA_CLIENT_SECRET=91d480ee95200b2cd3dd1ce2964a6fda80c350a8,BIGQUERY_DATASET=athlete_analysis_us,GOOGLE_GENAI_USE_VERTEXAI=1,FITNESS_BACKEND_URL=https://omrdreamers-backend-968053831621.asia-south2.run.app"

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ✅ Deployment Successful!
) else (
    echo.
    echo ❌ Deployment Failed.
)

pause
