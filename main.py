#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import threading
from src.scheduler import start_scheduler
from web.app import app

def main():
    """主函数"""
    print("币种数据监控系统")
    print("启动Web服务和数据更新服务...")
    
    # 在单独的线程中启动调度器
    scheduler_thread = threading.Thread(target=start_scheduler)
    scheduler_thread.daemon = True
    scheduler_thread.start()
    
    # 在主线程中启动Web服务
    app.run(debug=True, host='0.0.0.0', port=5000)

if __name__ == "__main__":
    main()