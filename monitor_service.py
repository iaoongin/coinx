#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
服务监控脚本，用于监控和自动重启coinx服务
"""
import os
import sys
import time
import subprocess
import psutil
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.utils import logger


def is_service_running():
    """检查coinx服务是否正在运行"""
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['cmdline']:
                cmdline = ' '.join(proc.info['cmdline'])
                if 'main.py' in cmdline and 'python' in proc.info['name'].lower():
                    return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, FileNotFoundError):
            pass
    return False


def start_service():
    """启动coinx服务"""
    try:
        # 使用start_app.py启动服务
        cmd = [sys.executable, str(project_root / 'start_app.py'), 'start']
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(project_root))
        if result.returncode == 0:
            logger.info("服务启动成功")
            return True
        else:
            logger.error(f"服务启动失败: {result.stderr}")
            return False
    except Exception as e:
        logger.error(f"启动服务时出错: {e}")
        return False


def monitor_service():
    """监控服务并自动重启"""
    logger.info("开始监控coinx服务...")
    
    while True:
        try:
            if not is_service_running():
                logger.warning("检测到服务已停止，正在尝试重启...")
                if start_service():
                    logger.info("服务重启成功")
                else:
                    logger.error("服务重启失败")
            else:
                logger.info("服务运行正常")
            
            # 每30秒检查一次
            time.sleep(30)
        except KeyboardInterrupt:
            logger.info("监控服务被手动停止")
            break
        except Exception as e:
            logger.error(f"监控过程中出错: {e}")
            time.sleep(30)


if __name__ == "__main__":
    monitor_service()