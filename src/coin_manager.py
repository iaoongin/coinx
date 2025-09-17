import sys
import os

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, project_root)

import requests
import json
import logging
from datetime import datetime, timedelta
from src.config import DATA_DIR, BINANCE_BASE_URL, USE_PROXY, PROXY_URL, HTTPS_PROXY_URL
from src.utils import logger

# 币种配置文件路径
COINS_CONFIG_FILE = os.path.join(DATA_DIR, 'coins_config.json')

def get_session():
    """创建带代理配置的会话"""
    session = requests.Session()
    
    if USE_PROXY:
        proxies = {
            'http': PROXY_URL,
            'https': HTTPS_PROXY_URL
        }
        session.proxies.update(proxies)
        logger.info(f"使用代理: {PROXY_URL}")
    
    return session

def get_all_coins_from_binance():
    """
    从币安获取所有交易对列表
    :return: USDT交易对列表
    """
    try:
        url = f"{BINANCE_BASE_URL}/fapi/v1/exchangeInfo"
        
        # 使用会话
        session = get_session()
        response = session.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # 筛选出USDT交易对
        usdt_pairs = []
        for symbol_info in data['symbols']:
            if symbol_info['quoteAsset'] == 'USDT' and symbol_info['status'] == 'TRADING':
                usdt_pairs.append({
                    'symbol': symbol_info['symbol'],
                    'baseAsset': symbol_info['baseAsset'],
                    'quoteAsset': symbol_info['quoteAsset']
                })
        
        logger.info(f"从币安获取到 {len(usdt_pairs)} 个USDT交易对")
        return usdt_pairs
    except Exception as e:
        logger.error(f"从币安获取交易对列表失败: {e}")
        return []

def load_coins_config():
    """
    从本地文件加载币种配置
    :return: 币种配置列表（只包含启用跟踪的币种）
    """
    try:
        if os.path.exists(COINS_CONFIG_FILE):
            with open(COINS_CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
                # 如果是旧格式，转换为新格式
                if isinstance(config, list):
                    # 转换旧格式为新格式
                    new_config = {
                        'updated_time': datetime.now().isoformat(),
                        'coins': {coin: True for coin in config}  # 默认都启用跟踪
                    }
                    save_coins_config_dict(new_config['coins'])
                    return list(new_config['coins'].keys())
                elif isinstance(config, dict) and 'coins' in config:
                    # 新格式：返回启用跟踪的币种
                    tracked_coins = [coin for coin, tracked in config['coins'].items() if tracked]
                    return tracked_coins
                else:
                    return []
        else:
            # 如果配置文件不存在，创建默认配置
            default_coins = {'BTCUSDT': True, 'ETHUSDT': True, 'BNBUSDT': True}
            save_coins_config_dict(default_coins)
            return list(default_coins.keys())
    except Exception as e:
        logger.error(f"加载币种配置失败: {e}")
        return ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']

def load_coins_config_dict():
    """
    从本地文件加载币种配置字典（包含跟踪状态）
    :return: 币种配置字典 {symbol: tracked}
    """
    try:
        if os.path.exists(COINS_CONFIG_FILE):
            with open(COINS_CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
                # 如果是旧格式，转换为新格式
                if isinstance(config, dict) and 'coins' in config:
                    coins_data = config['coins']
                    if isinstance(coins_data, list):
                        # 转换旧格式为新格式
                        new_config = {
                            'updated_time': config.get('updated_time', datetime.now().isoformat()),
                            'coins': {coin: True for coin in coins_data}  # 默认都启用跟踪
                        }
                        save_coins_config_dict(new_config['coins'])
                        return new_config['coins']
                    elif isinstance(coins_data, dict):
                        return coins_data
                else:
                    # 如果格式不正确，创建默认配置
                    default_coins = {'BTCUSDT': True, 'ETHUSDT': True, 'BNBUSDT': True}
                    save_coins_config_dict(default_coins)
                    return default_coins
        else:
            # 如果配置文件不存在，创建默认配置
            default_coins = {'BTCUSDT': True, 'ETHUSDT': True, 'BNBUSDT': True}
            save_coins_config_dict(default_coins)
            return default_coins
    except Exception as e:
        logger.error(f"加载币种配置字典失败: {e}")
        # 出错时返回默认配置
        default_coins = {'BTCUSDT': True, 'ETHUSDT': True, 'BNBUSDT': True}
        return default_coins

def save_coins_config_dict(coins_dict):
    """
    保存币种配置字典到本地文件
    :param coins_dict: 币种配置字典 {symbol: tracked}
    """
    try:
        config = {
            'updated_time': datetime.now().isoformat(),
            'coins': coins_dict
        }
        
        with open(COINS_CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        
        logger.info(f"币种配置已保存: {len(coins_dict)} 个币种")
    except Exception as e:
        logger.error(f"保存币种配置失败: {e}")

def update_coins_config():
    """
    更新币种配置：从币安获取最新交易对并更新配置
    """
    try:
        logger.info("开始更新币种配置...")
        
        # 从币安获取所有USDT交易对
        all_coins = get_all_coins_from_binance()
        if not all_coins:
            logger.warning("未能从币安获取到交易对列表")
            return False
        
        # 提取交易对名称
        coin_symbols = [coin['symbol'] for coin in all_coins]
        
        # 加载现有配置
        current_config = load_coins_config_dict()
        
        # 更新配置：保留现有币种的跟踪状态，添加新币种（默认不跟踪）
        updated_config = current_config.copy()
        for symbol in coin_symbols:
            if symbol not in updated_config:
                updated_config[symbol] = False  # 新币种默认不跟踪
        
        # 保存更新后的配置
        save_coins_config_dict(updated_config)
        
        logger.info(f"币种配置更新完成，共 {len(updated_config)} 个币种")
        return True
    except Exception as e:
        logger.error(f"更新币种配置失败: {e}")
        return False

def get_active_coins(filter_symbols=None):
    """
    获取活跃的币种列表（启用跟踪的币种）
    :param filter_symbols: 可选的筛选币种列表
    :return: 币种列表
    """
    # 加载启用跟踪的币种配置
    tracked_coins = load_coins_config()
    
    # 如果提供了筛选列表，则只返回筛选后的币种
    if filter_symbols:
        # 确保筛选的币种在配置列表中且启用跟踪
        filtered_coins = [coin for coin in tracked_coins if coin in filter_symbols]
        return filtered_coins
    
    return tracked_coins

def set_coin_tracking(symbol, tracked):
    """
    设置币种的跟踪状态
    :param symbol: 币种符号
    :param tracked: 是否跟踪（True/False）
    """
    try:
        # 加载现有配置
        coins_config = load_coins_config_dict()
        
        # 更新币种跟踪状态
        coins_config[symbol] = tracked
        
        # 保存配置
        save_coins_config_dict(coins_config)
        
        logger.info(f"币种 {symbol} 跟踪状态已更新为: {tracked}")
        return True
    except Exception as e:
        logger.error(f"更新币种跟踪状态失败: {e}")
        return False

def add_coin(symbol, tracked=True):
    """
    添加币种到配置
    :param symbol: 币种符号
    :param tracked: 是否跟踪（默认True）
    """
    try:
        # 加载现有配置
        coins_config = load_coins_config_dict()
        
        # 添加币种
        coins_config[symbol] = tracked
        
        # 保存配置
        save_coins_config_dict(coins_config)
        
        logger.info(f"币种 {symbol} 已添加到配置，跟踪状态: {tracked}")
        return True
    except Exception as e:
        logger.error(f"添加币种失败: {e}")
        return False

def remove_coin(symbol):
    """
    从配置中移除币种
    :param symbol: 币种符号
    """
    try:
        # 加载现有配置
        coins_config = load_coins_config_dict()
        
        # 移除币种
        if symbol in coins_config:
            del coins_config[symbol]
            
            # 保存配置
            save_coins_config_dict(coins_config)
            
            logger.info(f"币种 {symbol} 已从配置中移除")
            return True
        else:
            logger.warning(f"币种 {symbol} 不在配置中")
            return False
    except Exception as e:
        logger.error(f"移除币种失败: {e}")
        return False

if __name__ == "__main__":
    # 测试功能
    print("测试币种管理功能...")
    
    # 更新币种配置
    update_coins_config()
    
    # 获取活跃币种
    active_coins = get_active_coins()
    print(f"活跃币种数量: {len(active_coins)}")
    print(f"前10个币种: {active_coins[:10]}")