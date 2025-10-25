#!/bin/bash
# Flask应用启停脚本

PYTHON_PATH=python3

if [ $# -eq 0 ]; then
    echo "用法: ./start_app.sh [start|stop|restart|status]"
    exit 1
fi

case "$1" in
    start)
        echo "启动Flask应用..."
        $PYTHON_PATH start_app.py start
        ;;
    stop)
        echo "停止Flask应用..."
        $PYTHON_PATH start_app.py stop
        ;;
    restart)
        echo "重启Flask应用..."
        $PYTHON_PATH start_app.py restart
        ;;
    status)
        echo "查看Flask应用状态..."
        $PYTHON_PATH start_app.py status
        ;;
    *)
        echo "未知命令，请使用: start, stop, restart, status"
        exit 1
        ;;
esac