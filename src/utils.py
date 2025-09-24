import sys
import os

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, project_root)

import os
import json
import logging
from datetime import datetime
from src.config import DATA_DIR, LOGS_DIR

# 配置日志
def setup_logger():
    """设置日志配置"""
    # 确保日志目录存在
    os.makedirs(LOGS_DIR, exist_ok=True)
    
    log_file = os.path.join(LOGS_DIR, 'app.log')
    
    # 创建日志记录器
    logger = logging.getLogger(__name__)
    
    # 如果记录器已经有处理器，先清除
    if logger.handlers:
        logger.handlers.clear()
    
    # 设置日志级别
    logger.setLevel(logging.INFO)
    
    # 创建格式化器
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    
    # 文件处理器（轮转日志）
    from logging.handlers import RotatingFileHandler
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # 添加处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"日志系统已初始化，日志文件路径: {log_file}")
    return logger

logger = setup_logger()

def save_all_coins_data(data):
    """保存所有币种数据到本地文件"""
    try:
        filename = "coins_data.json"
        filepath = os.path.join(DATA_DIR, filename)
        
        # 如果文件存在，读取现有数据
        existing_data = []
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                # 验证数据格式
                if not isinstance(existing_data, list):
                    existing_data = []
            except (json.JSONDecodeError, Exception) as e:
                logger.error(f"读取现有数据文件失败: {e}")
                existing_data = []
        
        # 添加新数据
        new_entry = {
            'timestamp': int(datetime.now().timestamp() * 1000),
            'data': data
        }
        
        existing_data.append(new_entry)
        
        # 只保留最近的10条记录，避免文件过大
        if len(existing_data) > 10:
            existing_data = existing_data[-10:]
        
        # 在保存前验证数据是否可序列化
        try:
            json.dumps(existing_data, ensure_ascii=False)
        except Exception as e:
            logger.error(f"数据序列化失败: {e}")
            # 尝试简化数据结构
            simplified_data = []
            for entry in existing_data:
                simplified_entry = {
                    'timestamp': entry.get('timestamp', 0),
                    'data': []
                }
                for item in entry.get('data', []):
                    simplified_item = {
                        'symbol': item.get('symbol', ''),
                        'current_open_interest': item.get('current', {}).get('openInterest') if item.get('current') else None,
                        'changes': item.get('changes', {})
                    }
                    simplified_entry['data'].append(simplified_item)
                simplified_data.append(simplified_entry)
            existing_data = simplified_data
        
        # 保存数据，先写入临时文件再重命名，避免写入过程中断导致文件损坏
        temp_filepath = filepath + ".tmp"
        try:
            with open(temp_filepath, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, ensure_ascii=False, indent=2)
            # 原子性地替换原文件
            os.replace(temp_filepath, filepath)
            logger.info(f"所有币种数据已保存: {filename}，共 {len(existing_data)} 条记录")
        except Exception as e:
            # 清理临时文件
            if os.path.exists(temp_filepath):
                os.remove(temp_filepath)
            # 记录错误而不是抛出异常
            logger.error(f"保存数据时出错: {e}")
            logger.exception(e)
            
    except Exception as e:
        logger.error(f"保存所有币种数据失败: {e}")
        logger.exception(e)

def load_all_coins_data():
    """从本地文件加载所有币种数据"""
    try:
        filename = "coins_data.json"
        filepath = os.path.join(DATA_DIR, filename)
        
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                # 验证数据格式并返回最新的数据
                if isinstance(data, list) and len(data) > 0:
                    latest_data = data[-1].get('data', [])
                    # 确保返回的是列表而不是None
                    return latest_data if latest_data is not None else []
                else:
                    return []
            except (json.JSONDecodeError, Exception) as e:
                logger.error(f"加载所有币种数据失败: {e}")
                # 如果文件损坏，尝试创建新文件
                try:
                    with open(filepath, 'w', encoding='utf-8') as f:
                        json.dump([], f, ensure_ascii=False, indent=2)
                except Exception as write_error:
                    logger.error(f"重置数据文件失败: {write_error}")
                return []
        return []
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
    """获取缓存更新时间"""
    try:
        import os
        import json
        from datetime import datetime
        
        # 缓存文件路径
        CACHE_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'open_interest_cache.json')
        
        logger.info(f"尝试获取缓存更新时间，缓存文件路径: {CACHE_FILE}")
        
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # 获取最新的缓存时间戳
            if cache_data:
                timestamps = sorted(cache_data.keys(), key=int)
                latest_timestamp = int(timestamps[-1])
                logger.info(f"找到缓存数据，最新时间戳: {latest_timestamp}")
                return latest_timestamp * 1000  # 转换为毫秒
            else:
                logger.info("缓存文件存在但无数据")
        else:
            logger.info("缓存文件不存在")
        
        return None
    except Exception as e:
        logger.error(f"获取缓存更新时间失败: {e}")
        logger.exception(e)
        return None
