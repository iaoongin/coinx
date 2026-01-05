import os
import json
import time
from datetime import datetime
from coinx.utils import logger

# 缓存文件路径
# __file__ -> src/coinx/collector/binance/cache.py
# dirname -> src/coinx/collector/binance
# dirname -> src/coinx
# dirname -> src
# dirname -> project_root
CACHE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data', 'open_interest_cache.json')
DROP_LIST_CACHE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))), 'data', 'drop_list_cache.json')

def get_cache_key():
    """获取当前自然5分钟的时间戳作为缓存键"""
    now = datetime.now()
    # 计算当前自然5分钟的时间点（向下取整到最近的5分钟）
    cache_time = now.replace(minute=(now.minute // 5) * 5, second=0, microsecond=0)
    return int(cache_time.timestamp())

def load_cached_data():
    """从缓存文件加载数据"""
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                return cache_data
    except Exception as e:
        logger.error(f"加载缓存数据失败: {e}")
    return {}

def save_cached_data(cache_key, data):
    """将数据保存到缓存文件"""
    try:
        # 确保数据目录存在
        os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
        
        cache_data = load_cached_data()
        cache_data[str(cache_key)] = {
            'timestamp': cache_key,
            'data': data,
            'created_at': int(time.time())
        }
        
        # 只保留最近的12个缓存（1小时内的数据）
        cache_items = list(cache_data.items())
        cache_items.sort(key=lambda x: int(x[0]), reverse=True)
        cache_data = dict(cache_items[:12])
        
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)
        logger.info(f"数据已缓存到 {CACHE_FILE}")
    except Exception as e:
        logger.error(f"保存缓存数据失败: {e}")

def should_update_cache():
    """检查是否需要更新缓存（基于自然5分钟间隔）"""
    cache_key = get_cache_key()
    cache_data = load_cached_data()
    needs_update = str(cache_key) not in cache_data
    logger.info(f"检查缓存更新: 当前缓存键={cache_key}, 需要更新={needs_update}")
    return needs_update

def load_drop_list_cache():
    """从缓存文件加载跌幅榜数据"""
    try:
        if os.path.exists(DROP_LIST_CACHE_FILE):
            with open(DROP_LIST_CACHE_FILE, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                return cache_data
    except Exception as e:
        logger.error(f"加载跌幅榜缓存数据失败: {e}")
    return {}

def save_drop_list_cache(cache_key, data):
    """将跌幅榜数据保存到缓存文件"""
    try:
        # 确保数据目录存在
        os.makedirs(os.path.dirname(DROP_LIST_CACHE_FILE), exist_ok=True)
        
        cache_data = load_drop_list_cache()
        cache_data[str(cache_key)] = {
            'timestamp': cache_key,
            'data': data,
            'created_at': int(time.time())
        }
        
        # 只保留最近的12个缓存（1小时内的数据）
        cache_items = list(cache_data.items())
        cache_items.sort(key=lambda x: int(x[0]), reverse=True)
        cache_data = dict(cache_items[:12])
        
        with open(DROP_LIST_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)
        logger.info(f"跌幅榜数据已缓存到 {DROP_LIST_CACHE_FILE}")
    except Exception as e:
        logger.error(f"保存跌幅榜缓存数据失败: {e}")

def should_update_drop_list_cache():
    """检查是否需要更新跌幅榜缓存（基于自然5分钟间隔）"""
    cache_key = get_cache_key()
    cache_data = load_drop_list_cache()
    needs_update = str(cache_key) not in cache_data
def get_cache_update_time():
    """获取缓存更新时间"""
    try:
        import os
        import json
        
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
