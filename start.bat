@echo off
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

python scripts\start_app.py %ACTION%
if "%ACTION%"=="start" pause
if "%ACTION%"=="stop" pause
if "%ACTION%"=="restart" pause
if "%ACTION%"=="status" pause
REM run blocks until Ctrl+C, so no extra pause is needed
