#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flask应用启停脚本
"""
import os
import sys
import time
import psutil
from pathlib import Path
from dotenv import load_dotenv

# 添加项目根目录的src到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

load_dotenv(project_root / ".env")

from coinx.config import WEB_HOST, WEB_PORT
from coinx.utils import logger


class FlaskAppManager:
    def __init__(self):
        self.app_path = project_root / "src" / "coinx" / "main.py"  # 改为使用main.py启动应用
        self.pid_file = project_root / "data" / "app.pid"
        self.log_file = project_root / "logs" / "app_service.log"
        self.error_log_file = project_root / "logs" / "app_service_error.log"

    def _is_python_process(self, proc):
        try:
            return any(
                "python" in cmd.lower()
                for cmd in [proc.name()] + proc.cmdline()
            )
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return False

    def _is_app_command(self, cmdline_args, allow_manager_run=True):
        # cmdline_args: 原始的进程参数列表 (proc.cmdline())
        # 仅匹配两种确切场景：
        #   1) python src/coinx/main.py  (后台启动)
        #   2) python scripts/start_app.py run  (前台启动)
        if not cmdline_args or len(cmdline_args) < 2:
            return False
        args = cmdline_args  # [python_exe, arg1, arg2, ...]

        def path_matches(value, expected):
            try:
                return Path(value).resolve() == expected.resolve()
            except (OSError, RuntimeError):
                return False

        # 场景 1: python src/coinx/main.py
        if len(args) >= 2 and path_matches(args[1], self.app_path):
            return True

        # 管理命令本身不是应用。run 仅在确认监听端口时才视为服务，
        # 否则新启动的前台管理命令会在启动检查阶段误判自己。
        if (
            allow_manager_run
            and len(args) >= 3
            and path_matches(args[1], project_root / "scripts" / "start_app.py")
            and args[2] == "run"
        ):
            return True

        # 场景 3: python -m coinx.main / python -m coinx.web.app
        if len(args) >= 3 and args[1] == "-m" and args[2] in ("coinx.main", "coinx.web.app"):
            return True

        return False

    def _find_app_processes(self):
        current_pid = os.getpid()
        found = []
        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                if proc.info["pid"] == current_pid or not proc.info["cmdline"]:
                    continue
                if self._is_app_command(proc.info["cmdline"], allow_manager_run=False) and "python" in proc.info["name"].lower():
                    found.append(proc.info["pid"])
            except (psutil.NoSuchProcess, psutil.AccessDenied, FileNotFoundError):
                pass
        return found

    def _is_app_process(self, pid):
        try:
            process = psutil.Process(pid)
            return (
                process.is_running()
                and self._is_python_process(process)
                and self._is_app_command(process.cmdline())
            )
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return False

    def _find_app_listener_processes(self):
        """Find CoinX Python processes currently listening on WEB_PORT.

        Port ownership alone is not enough to terminate a process: another
        application may legitimately use the configured port. Require both a
        listening socket and a recognized CoinX command line before returning
        a PID.
        """
        current_pid = os.getpid()
        listener_pids = set()
        try:
            connections = psutil.net_connections(kind="inet")
        except (psutil.AccessDenied, OSError) as e:
            logger.warning(f"无法读取端口 {WEB_PORT} 的监听进程: {e}")
            return []

        for connection in connections:
            if (
                connection.status == psutil.CONN_LISTEN
                and connection.laddr
                and connection.laddr.port == WEB_PORT
                and connection.pid
                and connection.pid != current_pid
            ):
                listener_pids.add(connection.pid)

        found = []
        for pid in sorted(listener_pids):
            try:
                process = psutil.Process(pid)
                if self._is_python_process(process) and self._is_app_command(process.cmdline()):
                    found.append(pid)
                else:
                    logger.warning(f"端口 {WEB_PORT} 的进程 {pid} 不是已识别的 CoinX 应用，跳过停止")
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        return found

    def is_app_running(self):
        """检查应用是否正在运行"""
        logger.info(f"检查应用是否正在运行...")
        logger.info(f"PID文件路径: {self.pid_file}")

        # 首先检查 PID 文件。启动命令 run 的 PID 只有在确实监听端口时才可信。
        if self.pid_file.exists():
            try:
                with open(self.pid_file, "r") as f:
                    pid = int(f.read().strip())
                logger.info(f"从PID文件读取PID: {pid}")

                # 检查进程是否存在
                process = psutil.Process(pid)
                # 确认进程是Python并且正在运行相关文件
                if process.is_running() and self._is_python_process(process):
                    # 检查命令行参数是否匹配应用入口
                    cmdline_args = process.cmdline()
                    logger.info(f"进程命令行: {' '.join(cmdline_args)}")
                    is_manager_run = len(cmdline_args) >= 3 and cmdline_args[2] == "run"
                    listener_pids = set(self._find_app_listener_processes()) if is_manager_run else set()
                    if self._is_app_command(cmdline_args) and (not is_manager_run or pid in listener_pids):
                        logger.info(f"找到匹配的进程，PID: {pid}")
                        return True
                    else:
                        logger.info(f"进程不匹配: {' '.join(cmdline_args)}")
                else:
                    logger.info(f"进程不存在或不是Python进程")
            except psutil.NoSuchProcess:
                logger.info(f"PID文件中的进程 {pid} 已退出")
            except (ValueError, psutil.AccessDenied, FileNotFoundError) as e:
                logger.warning(f"检查PID文件失败: {e}")

        # PID 文件可能丢失，优先按端口寻找真正已启动的服务。
        listener_pids = self._find_app_listener_processes()
        for pid in listener_pids:
            logger.info(f"通过端口 {WEB_PORT} 找到应用，PID: {pid}")
            with open(self.pid_file, "w") as f:
                f.write(str(pid))
            return True

        # 最后仅按直接服务入口查找，避免把 start_app.py run 自身误判为服务。
        logger.info("通过进程名查找应用...")
        for pid in self._find_app_processes():
            logger.info(f"通过进程名找到应用，PID: {pid}")
            # 保存PID到文件
            with open(self.pid_file, "w") as f:
                f.write(str(pid))
            return True

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
            
            # 设置环境变量，添加src到PYTHONPATH
            env = os.environ.copy()
            src_path = str(project_root / "src")
            if "PYTHONPATH" in env:
                env["PYTHONPATH"] = src_path + os.pathsep + env["PYTHONPATH"]
            else:
                env["PYTHONPATH"] = src_path
            
            if daemon:
                # 后台模式：输出重定向到日志文件，避免未消费 PIPE 导致子进程异常退出。
                stdout_file = open(self.log_file, "a", encoding="utf-8")
                stderr_file = open(self.error_log_file, "a", encoding="utf-8")
                process = psutil.Popen(
                    cmd,
                    cwd=str(project_root),
                    env=env,
                    stdout=stdout_file,
                    stderr=stderr_file,
                    text=True,
                )
                
                # 等待一小段时间确保进程启动
                time.sleep(3)

                # 检查进程是否仍在运行
                if process.poll() is not None:
                    logger.error(f"应用启动失败，退出码: {process.returncode}")
                    logger.error(f"请查看日志: {self.log_file}, {self.error_log_file}")
                    stdout_file.close()
                    stderr_file.close()
                    return False

                # 保存PID
                logger.info(f"进程PID: {process.pid}")
                with open(self.pid_file, "w") as f:
                    f.write(str(process.pid))

                logger.info(f"应用已后台启动，PID: {process.pid}")
                logger.info(f"访问地址: http://{WEB_HOST}:{WEB_PORT}")
                stdout_file.close()
                stderr_file.close()
                return True
            else:
                # 前台模式：直接在当前进程运行主程序，让 Ctrl+C 进入 main.py 的清理逻辑。
                os.environ.update(env)
                logger.info(f"进程PID: {os.getpid()}")
                with open(self.pid_file, "w") as f:
                    f.write(str(os.getpid()))
                
                logger.info(f"应用已前台启动，按 Ctrl+C 停止")
                logger.info(f"访问地址: http://{WEB_HOST}:{WEB_PORT}")
                
                try:
                    from coinx.main import main as run_main

                    run_main()
                except KeyboardInterrupt:
                    logger.info("\n收到停止信号，正在停止...")
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

    def _collect_process_tree(self, pid):
        try:
            root = psutil.Process(pid)
        except psutil.NoSuchProcess:
            return []
        protected_pids = {os.getpid()}
        try:
            protected_pids.update(parent.pid for parent in psutil.Process().parents())
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
        return [
            process
            for process in root.children(recursive=True) + [root]
            if process.pid not in protected_pids
        ]

    def _stop_process_tree(self, pid, timeout=10):
        processes = self._collect_process_tree(pid)
        if not processes:
            return False

        for proc in processes:
            try:
                logger.info(f"正在停止进程 {proc.pid}: {' '.join(proc.cmdline())}")
                proc.terminate()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        gone, alive = psutil.wait_procs(processes, timeout=timeout)
        for proc in alive:
            try:
                logger.info(f"进程 {proc.pid} 未及时退出，强制结束")
                proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        if alive:
            psutil.wait_procs(alive, timeout=5)
        return True

    def stop(self):
        """停止应用"""
        logger.info("开始停止应用...")
        stopped = False

        target_pids = set()
        if self.pid_file.exists():
            try:
                with open(self.pid_file, "r") as f:
                    pid = int(f.read().strip())
                if self._is_app_process(pid):
                    target_pids.add(pid)
                else:
                    logger.warning(f"PID文件中的进程 {pid} 不是运行中的 CoinX 应用，忽略")
            except (ValueError, FileNotFoundError) as e:
                logger.error(f"读取PID文件失败: {e}")

        # PID 文件失效或不完整时，通过命令行和监听端口兜底查找。
        target_pids.update(self._find_app_processes())
        listener_pids = self._find_app_listener_processes()
        if listener_pids:
            logger.info(f"通过端口 {WEB_PORT} 找到 CoinX 监听进程: {listener_pids}")
            target_pids.update(listener_pids)

        for pid in sorted(target_pids):
            logger.info(f"停止应用进程，PID: {pid}")
            if self._stop_process_tree(pid):
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
        if self.is_app_running():
            logger.info("应用正在运行")
        else:
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
