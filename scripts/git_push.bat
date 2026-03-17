@echo off
echo =======================================
echo    Git Push Script - Athlete Analyzer
echo =======================================
echo.

cd ..
:: Stage all changes
echo Staging all changes...
git add .

:: Ask for commit message
set /p commit_msg="Enter commit message (default: Update): "
if "%commit_msg%"=="" set commit_msg=Update

:: Commit changes
echo.
echo Committing changes with message: "%commit_msg%"
git commit -m "%commit_msg%"

:: Push to GitHub
echo.
echo Pushing to GitHub (origin main)...
git push -u origin main

echo.
echo =======================================
echo    Process Complete!
echo =======================================
pause
