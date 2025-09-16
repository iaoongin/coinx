import os
import json
import logging
from datetime import datetime
from .config import DATA_DIR, LOGS_DIR

# 配置日志
def setup_logger():
    """设置日志配置"""
    log_file = os.path.join(LOGS_DIR, 'app.log')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logger()

def save_all_coins_data(data):
    """保存所有币种数据到本地文件"""
    try:
        filename = "coins_data.json"
        filepath = os.path.join(DATA_DIR, filename)
        
        # 如果文件存在，读取现有数据
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
        else:
            existing_data = []
        
        # 添加新数据
        existing_data.append({
            'timestamp': int(datetime.now().timestamp() * 1000),
            'data': data
        })
        
        # 只保留最近的100条记录
        if len(existing_data) > 100:
            existing_data = existing_data[-100:]
        
        # 保存数据
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"所有币种数据已保存: {filename}")
    except Exception as e:
        logger.error(f"保存所有币种数据失败: {e}")

def load_all_coins_data():
    """从本地文件加载所有币种数据"""
    try:
        filename = "coins_data.json"
        filepath = os.path.join(DATA_DIR, filename)
        
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # 返回最新的数据
                if data:
                    return data[-1]['data']
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