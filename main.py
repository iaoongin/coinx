#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import signal
import threading
from src.scheduler import start_scheduler, scheduler
from web.app import app
from src.utils import logger

def signal_handler(sig, frame):
    """信号处理函数，用于优雅地关闭应用"""
    logger.info("接收到关闭信号，正在停止应用...")
    try:
        # 关闭调度器
        if scheduler.running:
            scheduler.shutdown()
            logger.info("调度器已关闭")
    except Exception as e:
        logger.error(f"关闭调度器时出错: {e}")
    
    logger.info("应用已停止")
    sys.exit(0)

def main():
    """主函数"""
    logger.info("币种数据监控系统")
    logger.info("启动Web服务和数据更新服务...")
    
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # 在单独的线程中启动调度器
        scheduler_thread = threading.Thread(target=start_scheduler)
        scheduler_thread.daemon = True
        scheduler_thread.start()
        
        # 等待调度器启动
        time.sleep(1)
        
        # 在主线程中启动Web服务
        logger.info("启动Web服务...")
        app.run(debug=False, host='0.0.0.0', port=5000, use_reloader=False)
    except KeyboardInterrupt:
        logger.info("接收到键盘中断信号")
    except Exception as e:
        logger.error(f"主程序运行出错: {e}")
        logger.exception(e)
    finally:
        # 确保调度器被正确关闭
        try:
            if scheduler.running:
                scheduler.shutdown()
                logger.info("调度器已关闭")
        except Exception as e:
            logger.error(f"关闭调度器时出错: {e}")

if __name__ == "__main__":
    main()