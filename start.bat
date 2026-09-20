@echo off
title Solana Memecoin Tracker
cd /d "%~dp0"

echo.
echo  ============================================
echo    Solana Memecoin Tracker -- Trial Run
echo  ============================================
echo.

:: Kill any old server on port 8000
echo  Clearing port 8000...
for /f "tokens=5" %%a in ('netstat -ano 2^>nul ^| findstr ":8000" ^| findstr "LISTENING"') do (
    echo  Stopping old server PID %%a
    taskkill /PID %%a /F >nul 2>&1
)
echo  Port cleared.
echo.

:: Install requirements
echo  Installing / checking dependencies...
pip install fastapi uvicorn aiohttp -q --disable-pip-version-check
echo  Done.
echo.

:: Open browser after 3 seconds
start /b cmd /c "timeout /t 3 /nobreak >nul && start http://localhost:8000"

echo  Starting server at http://localhost:8000
echo  Press Ctrl+C to stop.
echo.

python main.py

echo.
echo  ============================================
echo    Server stopped. See any errors above.
echo  ============================================
echo.
pause
