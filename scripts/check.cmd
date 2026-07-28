@echo off
REM ---------------------------------------------------------------------
REM Runs the stage 1 and stage 2 smoke tests against a throwaway database.
REM
REM Usage (from the project root):
REM     scripts\check.cmd
REM
REM ---------------------------------------------------------------------
setlocal enabledelayedexpansion

if not exist "alembic.ini" (
    echo [x] Run this from the project root: scripts\check.cmd
    exit /b 1
)

echo.
echo === 1/6  Checking test dependencies ===
python -c "import httpx, websockets, uvicorn" 2>nul
if errorlevel 1 (
    echo [x] Test dependencies are missing. Install them first:
    echo.
    echo     python -m venv .venv
    echo     .venv\Scripts\activate
    echo     pip install -r requirements-dev.txt
    echo.
    exit /b 1
)
echo     ok

echo.
echo === 2/6  Stopping the app container ===
REM The smoke tests start their own instance. A second dispatcher against the
REM same database would consume events into its own hub, and the stage 2
REM subscriber would never see them.
docker compose stop app >nul 2>&1
echo     ok

echo.
echo === 3/6  Starting PostgreSQL ===
docker compose up -d postgres
if errorlevel 1 (
    echo [x] Could not start PostgreSQL. Is Docker running?
    exit /b 1
)

set /a attempt=0
:waitloop
set /a attempt+=1
docker compose exec -T postgres pg_isready -U outbox -d outbox >nul 2>&1
if not errorlevel 1 goto ready
if !attempt! geq 30 (
    echo [x] PostgreSQL did not become ready in 30 seconds.
    exit /b 1
)
timeout /t 1 /nobreak >nul
goto waitloop
:ready
echo     ready after !attempt!s

set POSTGRES_HOST=localhost
set POSTGRES_PORT=5432
set POSTGRES_USER=outbox
set POSTGRES_PASSWORD=outbox
set POSTGRES_DB=outbox
set PYTHONPATH=%CD%

echo.
echo === 4/6  Applying migrations ===
alembic upgrade head
if errorlevel 1 (
    echo [x] Migrations failed.
    exit /b 1
)

echo.
echo === 5/6  Running the pytest suite ===
python -m pytest
set PYTESTS=!errorlevel!

echo.
echo === 6/6  Running smoke tests ===
echo.
echo --- stage 1: writes and outbox atomicity ---
python scripts\smoke_stage1.py
set STAGE1=!errorlevel!

echo.
echo --- stage 2: event stream over a real WebSocket ---
python scripts\smoke_stage2.py
set STAGE2=!errorlevel!

echo.
echo --- stage 3: two workers, retention, shutdown ---
python scripts\smoke_stage3.py
set STAGE3=!errorlevel!

echo.
echo ======================================================
if !PYTESTS! equ 0 (echo   pytest : OK) else (echo   pytest : FAILED)
if !STAGE1! equ 0 (echo   stage 1: OK) else (echo   stage 1: FAILED)
if !STAGE2! equ 0 (echo   stage 2: OK) else (echo   stage 2: FAILED)
if !STAGE3! equ 0 (echo   stage 3: OK) else (echo   stage 3: FAILED)
echo ======================================================
echo.

if !PYTESTS! neq 0 exit /b 1
if !STAGE1! neq 0 exit /b 1
if !STAGE2! neq 0 exit /b 1
if !STAGE3! neq 0 exit /b 1
echo All checks passed.
exit /b 0
