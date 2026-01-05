@echo off
REM CoinX Management Script
REM Usage: start.bat [action]
REM Actions:
REM   (empty)  : Run in foreground (default)
REM   start    : Run in background
REM   stop     : Stop the application
REM   restart  : Restart the application
REM   status   : Check status

set ACTION=%1
if "%ACTION%"=="" set ACTION=run

python scripts\start_app.py %ACTION%
if "%ACTION%"=="start" pause
if "%ACTION%"=="stop" pause
if "%ACTION%"=="restart" pause
if "%ACTION%"=="status" pause
REM For 'run', we don't pause because it blocks until Ctrl+C
