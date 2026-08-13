@echo off
chcp 65001 >nul 2>nul
REM CoinX management script
REM Usage: start.bat [action]
REM Actions:
REM   (empty) : run in foreground, default
REM   start   : run in background
REM   stop    : stop the application
REM   restart : restart the application
REM   status  : check status

set ACTION=%1
if "%ACTION%"=="" set ACTION=run
if /I "%~1"=="--env" set ACTION=%3
if /I "%~1"=="--instance" set ACTION=%3
for /f "tokens=1,2 delims==" %%A in ("%~1") do (
    if /I "%%A"=="--env" set ACTION=%2
    if /I "%%A"=="--instance" set ACTION=%2
)

if exist "venv\Scripts\python.exe" (
    if "%~1"=="" (venv\Scripts\python.exe scripts\start_app.py run) else (venv\Scripts\python.exe scripts\start_app.py %*)
) else (
    if "%~1"=="" (python scripts\start_app.py run) else (python scripts\start_app.py %*)
)
if "%ACTION%"=="start" pause
if "%ACTION%"=="stop" pause
if "%ACTION%"=="restart" pause
if "%ACTION%"=="status" pause
REM run blocks until Ctrl+C, so no extra pause is needed
