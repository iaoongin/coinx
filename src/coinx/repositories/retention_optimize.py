"""Conservative MySQL table optimization following retention cleanup."""

import configparser
import os
import shutil
import threading
import time
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import text

from coinx import config
from coinx.database import get_session
from coinx.models import MarketFuturesSmartMoneySignal, ScheduledJobRun
from coinx.repositories.retention import MYSQL_RETENTION_TARGETS
from coinx.utils import logger


def _disk_paths(connection):
    data_path = str(config.DATA_CLEANUP_OPTIMIZE_DATADIR_PATH or '').strip()
    temp_path = str(config.DATA_CLEANUP_OPTIMIZE_TMPDIR_PATH or '').strip()
    if not data_path or not temp_path:
        raise ValueError('未配置 MySQL 数据目录和临时目录对应的磁盘路径')
    if not os.path.isabs(data_path) or not os.path.isabs(temp_path):
        raise ValueError('MySQL 磁盘路径必须是绝对路径')
    if not os.path.isdir(data_path) or not os.path.isdir(temp_path):
        raise ValueError('MySQL 磁盘路径不存在或不是目录')
    parser = configparser.ConfigParser()
    try:
        with (Path(data_path) / 'auto.cnf').open(encoding='utf-8') as source:
            parser.read_file(source)
        mounted_uuid = parser.get('auto', 'server-uuid')
    except (OSError, configparser.Error, KeyError) as exc:
        raise ValueError('无法读取 MySQL 数据目录的实例标识') from exc
    try:
        server_uuid = connection.execute(text('SELECT @@GLOBAL.server_uuid')).scalar()
    except Exception as exc:
        raise ValueError('无法读取 MySQL 实例标识') from exc
    if not server_uuid or mounted_uuid.casefold() != str(server_uuid).casefold():
        raise ValueError('数据目录与当前 MySQL 实例不匹配')
    return data_path, temp_path


@contextmanager
def _isolated_connection(bind):
    with bind.connect() as connection:
        try:
            yield connection
        finally:
            connection.invalidate()


@contextmanager
def _optimization_heartbeat(callback):
    if callback is None:
        yield
        return
    stop = threading.Event()
    interval = min(60, max(1, int(config.TASK_RUN_STALE_TIMEOUT_SECONDS) / 3))

    def run():
        while not stop.wait(interval):
            try:
                callback()
            except Exception:
                logger.exception('[数据清理] 优化心跳失败')

    thread = threading.Thread(target=run, daemon=True, name='coinx-cleanup-optimize-heartbeat')
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=1)


def optimize_cleaned_tables(table_names, progress_callback=None, heartbeat_callback=None):
    """Optimize eligible tables; explicitly configured paths attest the MySQL disks."""
    if not table_names:
        return []
    allowed = {model.__tablename__ for model, _ in MYSQL_RETENTION_TARGETS}
    allowed.update({MarketFuturesSmartMoneySignal.__tablename__, ScheduledJobRun.__tablename__})
    if any(name not in allowed for name in table_names):
        raise ValueError('优化目标不在清理表列表中')
    db = get_session()
    results = []

    def report(stage, message, **details):
        logger.info('[数据清理] %s', message)
        if progress_callback:
            try:
                progress_callback(stage, message, **details)
            except Exception:
                logger.exception('[数据清理] 优化进度记录失败')

    try:
        bind = db.get_bind()
        if bind.dialect.name != 'mysql':
            report('optimize_skipped', '跳过优化 | 当前数据库不是 MySQL')
            return [{'status': 'skipped', 'reason': '当前数据库不是 MySQL'}]
        threshold = float(config.DATA_CLEANUP_OPTIMIZE_FREE_PERCENT)
        reserve_gb = float(config.DATA_CLEANUP_OPTIMIZE_RESERVE_GB)
        if not 0 <= threshold <= 100 or reserve_gb < 0:
            raise ValueError('优化阈值或预留空间配置无效')
        with _isolated_connection(bind) as connection:
            connection.execute(text('SET SESSION information_schema_stats_expiry=0'))
            connection.execute(text('SET SESSION lock_wait_timeout=5'))
            for table_name in table_names:
                try:
                    row = connection.execute(text(
                        'SELECT DATA_LENGTH, INDEX_LENGTH, DATA_FREE '
                        'FROM information_schema.TABLES '
                        'WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name'
                    ), {'table_name': table_name}).first()
                    if row is None or any(value is None for value in row):
                        raise ValueError('无法读取表空间统计')
                    data_bytes, index_bytes, free_bytes = (int(value) for value in row)
                    total_bytes = data_bytes + index_bytes + free_bytes
                    free_percent = free_bytes * 100 / total_bytes if total_bytes else 0
                    if free_percent <= threshold:
                        reason = f'空闲率 {free_percent:.1f}% 未超过 {threshold:g}%'
                        report('optimize_skipped', f'优化跳过 | {table_name} | {reason}', table_name=table_name)
                        results.append({'table_name': table_name, 'status': 'skipped', 'reason': reason})
                        continue
                    try:
                        space_type = connection.execute(text(
                            "SELECT SPACE_TYPE FROM information_schema.INNODB_TABLES "
                            "WHERE NAME=CONCAT(DATABASE(), '/', :table_name)"
                        ), {'table_name': table_name}).scalar()
                    except Exception as exc:
                        raise ValueError('无法读取独立表空间信息，请检查 PROCESS 权限') from exc
                    if str(space_type).lower() != 'single':
                        raise ValueError('无法确认表使用独立表空间')
                    data_path, temp_path = _disk_paths(connection)
                    required = int((data_bytes + index_bytes) * 1.2 + reserve_gb * 1024 ** 3)
                    for path in (data_path, temp_path):
                        try:
                            available = shutil.disk_usage(path).free
                        except OSError as exc:
                            raise ValueError('无法读取 MySQL 磁盘可用空间') from exc
                        if available < required:
                            raise ValueError(f'磁盘可用空间不足，至少需要 {required / 1024 ** 3:.2f} GiB')
                    report('optimize_started', f'优化开始 | {table_name} | 空闲率 {free_percent:.1f}%', table_name=table_name)
                    started = time.perf_counter()
                    quoted = bind.dialect.identifier_preparer.quote(table_name)
                    with _optimization_heartbeat(heartbeat_callback):
                        rows = connection.exec_driver_sql(f'OPTIMIZE TABLE {quoted}').mappings().all()
                    errors = [str(row.get('Msg_text', '')) for row in rows if str(row.get('Msg_type', '')).lower() == 'error']
                    statuses = [str(row.get('Msg_text', '')) for row in rows if str(row.get('Msg_type', '')).lower() == 'status']
                    if errors or not statuses or statuses[-1].upper() != 'OK':
                        raise RuntimeError('; '.join(errors or statuses) or '未收到成功状态')
                    duration_ms = (time.perf_counter() - started) * 1000
                    report('optimize_finished', f'优化完成 | {table_name}', table_name=table_name, total_duration_ms=duration_ms)
                    results.append({'table_name': table_name, 'status': 'success', 'duration_ms': round(duration_ms)})
                except ValueError as exc:
                    report('optimize_skipped', f'优化跳过 | {table_name} | {exc}', table_name=table_name)
                    results.append({'table_name': table_name, 'status': 'skipped', 'reason': str(exc)})
                except Exception as exc:
                    logger.exception('[数据清理] 优化失败 | %s', table_name)
                    report('optimize_error', f'优化失败 | {table_name} | {exc}', table_name=table_name)
                    results.append({'table_name': table_name, 'status': 'error', 'reason': str(exc)[:500]})
        return results
    finally:
        db.close()
