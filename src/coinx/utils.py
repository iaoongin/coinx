import sys
import os
import json
import uuid
import logging
import tempfile
from datetime import datetime
from sqlalchemy import desc, func

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, project_root)

from coinx.config import LOGS_DIR, TIME_INTERVALS, DATA_DIR
from coinx.database import db_session
from coinx.models import MarketSnapshot

# 配置日志
def setup_logger():
    """设置日志配置"""
    # 确保日志目录存在
    os.makedirs(LOGS_DIR, exist_ok=True)
    
    log_file = os.getenv('APP_LOG_FILE') or os.path.join(LOGS_DIR, 'app.log')
    log_file = os.path.abspath(log_file)
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # 获取根日志记录器
    root_logger = logging.getLogger()
    
    # 清除现有的处理器
    if root_logger.handlers:
        root_logger.handlers.clear()
    
    # 设置根日志级别
    root_logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s [%(filename)15s:%(lineno)4d] - %(message)s')
    
    # 文件处理器（轮转日志）。开发机上旧进程可能锁住默认日志文件；
    # 日志不可用不应阻断 API 进程启动，因此回退到用户临时目录。
    from logging.handlers import RotatingFileHandler
    try:
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
    except (PermissionError, OSError) as exc:
        fallback_file = os.path.join(tempfile.gettempdir(), f'coinx-{os.getpid()}.log')
        file_handler = RotatingFileHandler(
            fallback_file,
            maxBytes=10*1024*1024,
            backupCount=5,
            encoding='utf-8'
        )
        log_file = fallback_file
        logging.getLogger(__name__).warning(
            '日志文件不可写，已回退到临时文件 %s: %s', fallback_file, exc
        )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # 添加处理器到根记录器
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # 配置其他特定库的日志
    loggers_to_configure = ['werkzeug', 'apscheduler', 'urllib3', 'sqlalchemy']
    for logger_name in loggers_to_configure:
        try:
            lib_logger = logging.getLogger(logger_name)
            # lib_logger.handlers = []  # 不要清除，否则可能会影响其他的
            if logger_name == 'sqlalchemy':
                 lib_logger.setLevel(logging.WARNING) # SQL Alchemy日志设为Warning
            lib_logger.propagate = True 
        except Exception as e:
            root_logger.warning(f"配置 {logger_name} 日志失败: {e}")

    root_logger.info(f"日志系统已初始化，日志文件路径: {log_file}")
    return root_logger

logger = setup_logger()

def save_all_coins_data(data):
    from coinx.write_backend import (
        get_clickhouse_write_repository,
        is_clickhouse_write,
        market_write_lock,
    )
    if is_clickhouse_write():
        if not data:
            return 0
        batch_id = str(uuid.uuid4())
        snapshot_time = int(datetime.now().timestamp() * 1000)
        version = datetime.now()
        rows = []
        for coin_data in data:
            symbol = coin_data.get('symbol')
            if not symbol:
                continue
            current = coin_data.get('current', {})
            price = current.get('price')
            open_interest = current.get('openInterest')
            open_interest_value = current.get('openInterestValue')
            if price is None and open_interest and open_interest_value:
                try:
                    price = float(open_interest_value) / float(open_interest)
                except (TypeError, ValueError, ZeroDivisionError):
                    pass
            rows.append({
                'snapshot_time': snapshot_time,
                'symbol': symbol,
                'batch_id': batch_id,
                'price': price,
                'open_interest': open_interest,
                'open_interest_value': open_interest_value,
                'data_json': json.dumps(coin_data, ensure_ascii=False, separators=(',', ':')),
                'created_at': version,
            })
        if not rows:
            return 0
        with market_write_lock('market_snapshots', batch_id):
            saved = get_clickhouse_write_repository().insert_rows(
                'market_snapshots',
                [
                    'snapshot_time', 'symbol', 'batch_id', 'price',
                    'open_interest', 'open_interest_value', 'data_json', 'created_at',
                ],
                rows,
                batch_id=f'snapshots_{batch_id}',
            )
        cleanup_old_data()
        logger.info('所有币种数据已保存到 ClickHouse，批次: %s，共 %d 条记录', batch_id, saved)
        return saved
    """保存所有币种数据到数据库"""
    try:
        if not data:
            return

        # 生成批次ID
        batch_id = str(uuid.uuid4())
        snapshot_time = int(datetime.now().timestamp() * 1000)

        snapshots = []
        for coin_data in data:
            symbol = coin_data.get('symbol')
            if not symbol:
                continue
                
            current = coin_data.get('current', {})
            
            # 准备数据JSON
            # 这里我们把原始数据存入JSON字段，以便后续查询时能还原完整结构
            # 同时也把关键指标提取到列中，方便SQL查询
            
            snapshot = MarketSnapshot(
                batch_id=batch_id,
                symbol=symbol,
                price=current.get('price'), # 假设 api 返回的数据里有 price，或者通过 current 获取
                # 注意：原始数据中 current.price 可能是 None，需要逻辑中计算
                # 这里我们尽量存原始值
                open_interest=current.get('openInterest'),
                open_interest_value=current.get('openInterestValue'),
                data_json=coin_data,
                snapshot_time=snapshot_time
            )
            
            # 补充价格计算逻辑，如果原始数据没有直接提供
            if snapshot.price is None and snapshot.open_interest and snapshot.open_interest_value:
                 try:
                     snapshot.price = float(snapshot.open_interest_value) / float(snapshot.open_interest)
                 except:
                     pass

            snapshots.append(snapshot)
        
        # 批量插入
        if snapshots:
            db_session.add_all(snapshots)
            db_session.commit()
            logger.info(f"所有币种数据已保存到DB，批次: {batch_id}，共 {len(snapshots)} 条记录")
            
            # 清理旧数据 (保留最近10个批次)
            # 这一步可以选择性执行，或者由独立的清理任务执行
            cleanup_old_data()

    except Exception as e:
        db_session.rollback()
        logger.error(f"保存所有币种数据失败: {e}")
        logger.exception(e)

def cleanup_old_data(keep_batches=20):
    from coinx.write_backend import get_clickhouse_write_repository, is_clickhouse_write
    if is_clickhouse_write():
        try:
            from coinx.read_backend import get_clickhouse_repository

            table = get_clickhouse_repository()._table('market_snapshots', final=False)
            rows = get_clickhouse_repository().client.query_rows(
                f'SELECT snapshot_time FROM {table} '
                f'GROUP BY snapshot_time ORDER BY snapshot_time DESC LIMIT {max(1, int(keep_batches))}'
            )
            if len(rows) >= keep_batches:
                min_time = int(rows[-1]['snapshot_time'])
                get_clickhouse_write_repository().client.execute(
                    f'ALTER TABLE {table} DELETE WHERE snapshot_time < {min_time}'
                )
            return 0
        except Exception:
            logger.exception('ClickHouse market snapshot cleanup failed')
            # Cleanup is part of the CK snapshot write contract.  Surface the
            # failure so the scheduler records this batch as failed instead
            # of silently reporting a successful write with unbounded data.
            raise
    """清理旧数据，只保留最近的N个批次"""
    try:
        # 查询最近的N个批次的时间戳
        # SELECT DISTINCT snapshot_time FROM market_snapshots ORDER BY snapshot_time DESC LIMIT N
        
        # 使用 SQLAlchemy 查询
        subquery = db_session.query(MarketSnapshot.snapshot_time).\
            group_by(MarketSnapshot.snapshot_time).\
            order_by(desc(MarketSnapshot.snapshot_time)).\
            limit(keep_batches).subquery()
            
        # 找到第N个批次的时间（最小时间）
        # 这种方式可能比较复杂，更简单的是找到第N个distinct snapshot_time
        
        times = db_session.query(MarketSnapshot.snapshot_time).\
            group_by(MarketSnapshot.snapshot_time).\
            order_by(desc(MarketSnapshot.snapshot_time)).\
            limit(keep_batches).all()
            
        if len(times) >= keep_batches:
            min_time = times[-1][0]
            
            # 删除小于该时间的记录
            # DELETE FROM market_snapshots WHERE snapshot_time < min_time
            deleted = db_session.query(MarketSnapshot).\
                filter(MarketSnapshot.snapshot_time < min_time).\
                delete(synchronize_session=False)
                
            db_session.commit()
            if deleted > 0:
                logger.info(f"清理了旧数据: {deleted} 条记录")
                
    except Exception as e:
        db_session.rollback()
        logger.warning(f"清理旧数据失败: {e}")

def load_all_coins_data():
    from coinx.write_backend import is_clickhouse_write
    if is_clickhouse_write():
        try:
            from coinx.read_backend import get_clickhouse_repository

            table = get_clickhouse_repository()._table('market_snapshots', final=False)
            rows = get_clickhouse_repository().client.query_rows(
                'WITH deduplicated AS ('
                'SELECT symbol, snapshot_time, batch_id, '
                'argMax(data_json, created_at) AS data_json '
                f'FROM {table} '
                'GROUP BY symbol, snapshot_time, batch_id) '
                'SELECT symbol, snapshot_time, data_json '
                'FROM deduplicated '
                'ORDER BY symbol ASC, snapshot_time DESC, batch_id DESC '
                'LIMIT 1 BY symbol'
            )
            result = []
            seen = set()
            for row in rows:
                symbol = row.get('symbol')
                if not symbol or symbol in seen:
                    continue
                seen.add(symbol)
                payload = row.get('data_json')
                if payload:
                    result.append(json.loads(payload) if isinstance(payload, str) else payload)
            return result
        except Exception:
            logger.exception('从 ClickHouse 加载所有币种数据失败')
            return []
    """从数据库加载每个币种的最新数据"""
    try:
        # 使用子查询找到每个symbol最新的snapshot_time
        subquery = db_session.query(
            MarketSnapshot.symbol,
            func.max(MarketSnapshot.snapshot_time).label('max_time')
        ).group_by(MarketSnapshot.symbol).subquery()
        
        # 连接查询获取完整记录
        snapshots = db_session.query(MarketSnapshot).join(
            subquery,
            (MarketSnapshot.symbol == subquery.c.symbol) &
            (MarketSnapshot.snapshot_time == subquery.c.max_time)
        ).all()
            
        # 还原为原始列表格式
        result = []
        for snapshot in snapshots:
            if snapshot.data_json:
                result.append(snapshot.data_json)
                
        return result
        
    except Exception as e:
        logger.error(f"加载所有币种数据失败: {e}")
        return []

def get_latest_open_interest(symbol_data, interval):
    """从币种数据中获取指定时间间隔的最新持仓量数据"""
    try:
        # 查找指定时间间隔的数据
        for item in symbol_data.get('intervals', []):
            if item.get('interval') == interval:
                return [item.get('timestamp', 0), item.get('openInterest', 0)]
        return None
    except Exception as e:
        logger.error(f"获取最新持仓量数据失败: {e}")
        return None

def calculate_change_ratio(current, past):
    """计算变化比例"""
    if past is None or past[1] == 0:
        return 0
    ratio = ((current[1] - past[1]) / past[1]) * 100
    return round(ratio, 2)

def get_cache_update_time():
    from coinx.write_backend import is_clickhouse_write
    if is_clickhouse_write():
        try:
            from coinx.read_backend import get_clickhouse_repository

            table = get_clickhouse_repository()._table('market_snapshots', final=False)
            value = get_clickhouse_repository().client.query_scalar(
                f'SELECT max(snapshot_time) FROM {table}'
            )
            return int(value) if value is not None else None
        except Exception:
            logger.exception('从 ClickHouse 获取快照更新时间失败')
            return None
    """获取缓存更新时间"""
    try:
        # 尝试从数据库获取最新更新时间
        latest_time_row = db_session.query(MarketSnapshot.snapshot_time).\
            order_by(desc(MarketSnapshot.snapshot_time)).\
            first()
            
        if latest_time_row:
            return latest_time_row[0]
            
        return None
    except Exception as e:
        logger.warning(f"获取缓存更新时间失败: {e}")
        return None
