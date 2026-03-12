@echo off
setlocal

echo.
echo 🔄 Starting Manual Database Sync (Postgres -> BigQuery)...
echo.

:: Run the sync script directly
python services/bq_sync.py

if %ERRORLEVEL% equ 0 (
    echo.
    echo ✅ Sync completed successfully!
) else (
    echo.
    echo ❌ Sync failed. Check the logs above.
)

pause
