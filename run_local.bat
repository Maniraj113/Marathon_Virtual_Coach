@echo off
setlocal

echo.
echo  Starting Athlete Analyzer Local Server...
echo.

:: Check for .env file
if not exist .env (
    echo  .env file not found! Please create it based on your db details.
    exit /b 1
)


:: Start FastAPI with Uvicorn
echo  Launching API at http://localhost:8000
echo  Documentation available at http://localhost:8000/docs
echo.
uvicorn api:app --reload --host 0.0.0.0 --port 8001

pause
