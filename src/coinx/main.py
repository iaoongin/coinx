#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import signal
import sys
import logging

from coinx.config import WEB_DEBUG, WEB_HOST, WEB_PORT
from coinx.runtime import start_runtime_services
from coinx.web.app import app
from coinx.scheduler import scheduler
from coinx.utils import logger

def signal_handler(sig, frame):
    """信号处理函数，用于快速关闭应用与后台线程"""
    logger.info("接收到关闭信号，正在停止应用...")
    try:
        # 关闭调度器
        if scheduler.running:
            scheduler.shutdown(wait=False)
            logger.info("调度器已强制关闭")
    except Exception as e:
        logger.error(f"关闭调度器时出错: {e}")

    logger.info("应用已停止")
    logging.shutdown()
    os._exit(0)

def main():
    """主函数"""
    logger.info("币种数据监控系统")
    logger.info("启动Web服务和数据更新服务...")
    
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        if not WEB_DEBUG or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
            start_runtime_services(with_startup_repair=True, startup_delay_seconds=1)
        
        logger.info("")
        logger.info("============================================================")
        logger.info(f"启动Web服务: http://{WEB_HOST}:{WEB_PORT}")
        logger.info(f"启动Web服务: http://127.0.0.1:{WEB_PORT}")
        logger.info("============================================================")
        logger.info("")
        app.run(debug=WEB_DEBUG, host=WEB_HOST, port=WEB_PORT, use_reloader=WEB_DEBUG)
    except KeyboardInterrupt:
        logger.info("接收到键盘中断信号")
    except Exception as e:
        logger.error(f"主程序运行出错: {e}")
        logger.exception(e)
    finally:
        # 确保调度器被正确关闭
        try:
            if scheduler.running:
                scheduler.shutdown(wait=False)
                logger.info("调度器已强制关闭")
        except Exception as e:
            logger.error(f"关闭调度器时出错: {e}")

if __name__ == "__main__":
    main()
