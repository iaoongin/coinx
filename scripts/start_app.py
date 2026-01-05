#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flask应用启停脚本
"""
import os
import sys
import subprocess
import time
import signal
import psutil
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils import logger


class FlaskAppManager:
    def __init__(self):
        self.app_path = project_root / "main.py"  # 改为使用main.py启动应用
        self.pid_file = project_root / "data" / "app.pid"
        self.log_file = project_root / "logs" / "app_service.log"

    def is_app_running(self):
        """检查应用是否正在运行"""
        logger.info(f"检查应用是否正在运行...")
        logger.info(f"PID文件路径: {self.pid_file}")

        # 首先检查PID文件
        if self.pid_file.exists():
            try:
                with open(self.pid_file, "r") as f:
                    pid = int(f.read().strip())
                logger.info(f"从PID文件读取PID: {pid}")

                # 检查进程是否存在
                process = psutil.Process(pid)
                # 确认进程是Python并且正在运行相关文件
                if process.is_running() and any(
                    "python" in cmd.lower()
                    for cmd in [process.name()] + process.cmdline()
                ):
                    # 检查命令行参数是否包含main.py或web.app
                    cmdline = " ".join(process.cmdline())
                    logger.info(f"进程命令行: {cmdline}")
                    if (
                        "main.py" in cmdline
                        or "web.app" in cmdline
                        or "-m web.app" in cmdline
                    ):
                        logger.info(f"找到匹配的进程，PID: {pid}")
                        return True
                    else:
                        logger.info(f"进程不匹配: {cmdline}")
                else:
                    logger.info(f"进程不存在或不是Python进程")
            except (
                ValueError,
                psutil.NoSuchProcess,
                psutil.AccessDenied,
                FileNotFoundError,
            ) as e:
                logger.error(f"检查PID文件时出错: {e}")
                pass

        # 如果PID文件检查失败，尝试通过进程名查找
        logger.info("通过进程名查找应用...")
        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                if proc.info["cmdline"]:
                    cmdline = " ".join(proc.info["cmdline"])
                    if (
                        "main.py" in cmdline
                        or "web.app" in cmdline
                        or "-m web.app" in cmdline
                    ) and "python" in proc.info["name"].lower():
                        print(f"通过进程名找到应用，PID: {proc.info['pid']}")
                        # 保存PID到文件
                        with open(self.pid_file, "w") as f:
                            f.write(str(proc.info["pid"]))
                        return True
            except (psutil.NoSuchProcess, psutil.AccessDenied, FileNotFoundError):
                pass

        # 删除无效的PID文件
        if self.pid_file.exists():
            print("删除无效的PID文件")
            self.pid_file.unlink()
        return False

    def start(self, daemon=True):
        """启动应用"""
        mode = "后台" if daemon else "前台"
        logger.info(f"开始{mode}启动应用...")
        if self.is_app_running():
            logger.info("应用已经在运行中")
            return False

        try:
            # 确保日志目录存在
            self.log_file.parent.mkdir(parents=True, exist_ok=True)

            # 启动应用
            cmd = [sys.executable, str(self.app_path)]
            logger.info(f"启动命令: {' '.join(cmd)}")
            
            if daemon:
                # 后台模式：使用PIPE捕获输出，不阻塞
                process = subprocess.Popen(
                    cmd,
                    cwd=str(project_root),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                
                # 等待一小段时间确保进程启动
                time.sleep(3)

                # 检查进程是否仍在运行
                if process.poll() is not None:
                    # 进程已经结束，获取错误输出
                    stdout, stderr = process.communicate()
                    logger.error(f"应用启动失败，退出码: {process.returncode}")
                    logger.info(f"标准输出: {stdout}")
                    logger.error(f"错误输出: {stderr}")
                    return False

                # 保存PID
                logger.info(f"进程PID: {process.pid}")
                with open(self.pid_file, "w") as f:
                    f.write(str(process.pid))

                logger.info(f"应用已后台启动，PID: {process.pid}")
                logger.info("访问地址: http://127.0.0.1:5000")
                return True
            else:
                # 前台模式：直接显示输出，阻塞直到退出
                process = subprocess.Popen(
                    cmd,
                    cwd=str(project_root),
                    stdout=None,  # 继承标准输出
                    stderr=None,  # 继承标准错误
                )
                
                # 保存PID
                logger.info(f"进程PID: {process.pid}")
                with open(self.pid_file, "w") as f:
                    f.write(str(process.pid))
                
                logger.info(f"应用已前台启动，按 Ctrl+C 停止")
                logger.info("访问地址: http://127.0.0.1:5000")
                
                try:
                    process.wait()
                except KeyboardInterrupt:
                    logger.info("\n接收到停止信号，正在停止...")
                    process.terminate()
                    process.wait()
                finally:
                    # 清理PID文件
                    if self.pid_file.exists():
                        self.pid_file.unlink()
                        logger.info("PID文件已清理")
                return True

        except Exception as e:
            logger.error(f"启动应用失败: {e}")
            import traceback

            traceback.print_exc()
            # 尝试删除PID文件
            if self.pid_file.exists():
                self.pid_file.unlink()
            return False

    def stop(self):
        """停止应用"""
        logger.info("开始停止应用...")
        # 首先尝试通过进程查找停止应用
        stopped = False
        processes_to_kill = []
        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                if proc.info["cmdline"]:
                    cmdline = " ".join(proc.info["cmdline"])
                    if (
                        "main.py" in cmdline
                        or "web.app" in cmdline
                        or "-m web.app" in cmdline
                    ) and "python" in proc.info["name"].lower():
                        logger.info(f"找到运行中的应用进程，PID: {proc.info['pid']}")
                        processes_to_kill.append(proc.info["pid"])
            except (psutil.NoSuchProcess, psutil.AccessDenied, FileNotFoundError):
                pass

        for pid in processes_to_kill:
            try:
                proc = psutil.Process(pid)
                # 先尝试优雅关闭
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                    logger.info(f"进程 {pid} 已停止")
                    stopped = True
                except psutil.TimeoutExpired:
                    # 如果优雅关闭超时，则强制杀死进程
                    proc.kill()
                    proc.wait()
                    logger.info(f"进程 {pid} 已被强制停止")
                    stopped = True
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                # 进程可能已经退出
                logger.info(f"进程 {pid} 可能已经退出")
                stopped = True

        # 删除PID文件
        if self.pid_file.exists():
            self.pid_file.unlink()
            logger.info("PID文件已删除")

        if not stopped:
            logger.info("未找到运行中的应用")
            return False
        return True

    def restart(self):
        """重启应用"""
        logger.info("正在重启应用...")
        self.stop()
        # 等待一段时间确保应用完全停止
        time.sleep(2)
        return self.start()

    def status(self):
        """查看应用状态"""
        logger.info("检查应用状态...")
        # 检查PID文件
        if self.pid_file.exists():
            try:
                with open(self.pid_file, "r") as f:
                    pid = int(f.read().strip())
                logger.info(f"从PID文件读取PID: {pid}")

                # 检查进程是否存在
                process = psutil.Process(pid)
                # 确认进程是Python并且正在运行相关文件
                if process.is_running() and any(
                    "python" in cmd.lower()
                    for cmd in [process.name()] + process.cmdline()
                ):
                    # 检查命令行参数是否包含main.py或web.app
                    cmdline = " ".join(process.cmdline())
                    logger.info(f"进程命令行: {cmdline}")
                    if (
                        "main.py" in cmdline
                        or "web.app" in cmdline
                        or "-m web.app" in cmdline
                    ):
                        logger.info(f"应用正在运行，PID: {pid}")
                        return
                    else:
                        logger.info(f"进程不匹配: {cmdline}")
                else:
                    logger.info(f"进程不存在或不是Python进程")
            except (
                ValueError,
                psutil.NoSuchProcess,
                psutil.AccessDenied,
                FileNotFoundError,
            ) as e:
                logger.error(f"检查PID文件时出错: {e}")
                pass

        # 如果PID文件检查失败，尝试通过进程名查找
        logger.info("通过进程名查找应用...")
        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                if proc.info["cmdline"]:
                    cmdline = " ".join(proc.info["cmdline"])
                    if (
                        "main.py" in cmdline
                        or "web.app" in cmdline
                        or "-m web.app" in cmdline
                    ) and "python" in proc.info["name"].lower():
                        logger.info(f"通过进程名找到应用，PID: {proc.info['pid']}")
                        # 保存PID到文件
                        with open(self.pid_file, "w") as f:
                            f.write(str(proc.info["pid"]))
                        logger.info(f"应用正在运行，PID: {proc.info['pid']}")
                        return
            except (psutil.NoSuchProcess, psutil.AccessDenied, FileNotFoundError):
                pass

        # 删除无效的PID文件
        if self.pid_file.exists():
            logger.info("删除无效的PID文件")
            self.pid_file.unlink()

        logger.info("应用未运行")


def main():
    if len(sys.argv) < 2:
        logger.info("用法: python start_app.py [start|stop|restart|status]")
        return

    manager = FlaskAppManager()
    action = sys.argv[1].lower()

    if action == "run":
        manager.start(daemon=False)
    elif action == "start":
        manager.start(daemon=True)
    elif action == "stop":
        manager.stop()
    elif action == "restart":
        manager.restart()
    elif action == "status":
        manager.status()
    else:
        logger.info("未知命令，请使用: run (前台), start (后台), stop, restart, status")


if __name__ == "__main__":
    main()
