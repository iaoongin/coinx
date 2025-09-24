@echo off
REM Flask应用启停脚本

set PYTHON_PATH=python

if "%1"=="" (
    echo 用法: start_app.bat [start^|stop^|restart^|status]
    goto :eof
)

if "%1"=="start" (
    echo 启动Flask应用...
    %PYTHON_PATH% start_app.py start
) else if "%1"=="stop" (
    echo 停止Flask应用...
    %PYTHON_PATH% start_app.py stop
) else if "%1"=="restart" (
    echo 重启Flask应用...
    %PYTHON_PATH% start_app.py restart
) else if "%1"=="status" (
    echo 查看Flask应用状态...
    %PYTHON_PATH% start_app.py status
) else (
    echo 未知命令，请使用: start, stop, restart, status
)

pause