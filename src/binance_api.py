import sys
import os

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, project_root)

import requests
import time
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.config import BINANCE_BASE_URL, TIME_INTERVALS, USE_PROXY, PROXY_URL, HTTPS_PROXY_URL
from src.utils import save_all_coins_data, logger
from src.coin_manager import get_active_coins, get_all_coins_from_binance
import json

# 创建一个全局会话对象，用于复用连接
_global_session = None

# 缓存文件路径
CACHE_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'open_interest_cache.json')

def get_session():
    """创建带代理配置的会话"""
    global _global_session
    if _global_session is None:
        _global_session = requests.Session()
        
        if USE_PROXY:
            proxies = {
                'http': PROXY_URL,
                'https': HTTPS_PROXY_URL
            }
            _global_session.proxies.update(proxies)
            logger.info(f"使用代理: {PROXY_URL}")
    
    return _global_session

def get_latest_price(symbol):
    """
    获取指定币种的最新价格
    :param symbol: 币种对，如 BTCUSDT
    :return: 最新价格
    """
    try:
        url = f"{BINANCE_BASE_URL}/fapi/v2/ticker/price"
        params = {
            'symbol': symbol
        }
        
        # 使用会话
        session = get_session()
        logger.info(f"请求最新价格数据: {url}?symbol={symbol}")
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        logger.info(f"最新价格数据响应: {data}")
        
        return float(data['price'])
    except requests.exceptions.RequestException as e:
        logger.error(f"网络请求失败: {symbol}, 错误: {e}")
        return None
    except Exception as e:
        logger.error(f"获取最新价格失败: {symbol}, 错误: {e}")
        return None

def get_24hr_ticker(symbol):
    """
    获取指定币种的24小时价格变化数据
    :param symbol: 币种对，如 BTCUSDT
    :return: 24小时价格变化数据
    """
    try:
        url = f"{BINANCE_BASE_URL}/fapi/v1/ticker/24hr"
        params = {
            'symbol': symbol
        }
        
        # 使用会话
        session = get_session()
        logger.info(f"请求24小时价格变化数据: {url}?symbol={symbol}")
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        logger.info(f"24小时价格变化数据响应: {data}")
        
        return {
            'priceChange': float(data['priceChange']),
            'priceChangePercent': float(data['priceChangePercent']),
            'lastPrice': float(data['lastPrice']),
            'highPrice': float(data['highPrice']),
            'lowPrice': float(data['lowPrice']),
            'volume': float(data['volume'])
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"网络请求失败: {symbol}, 错误: {e}")
        return None
    except Exception as e:
        logger.error(f"获取24小时价格变化数据失败: {symbol}, 错误: {e}")
        return None

def get_open_interest(symbol):
    """
    获取指定币种的当前持仓量数据
    :param symbol: 币种对，如 BTCUSDT
    :return: 当前持仓量数据
    """
    try:
        url = f"{BINANCE_BASE_URL}/fapi/v1/openInterest"
        params = {
            'symbol': symbol
        }
        
        # 使用会话
        session = get_session()
        logger.info(f"请求持仓量数据: {url}?symbol={symbol}")
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        logger.info(f"持仓量数据响应: {data}")
        
        # 尝试从响应中获取sumOpenInterestValue，如果没有则设为0
        open_interest_value = 0
        if 'sumOpenInterestValue' in data:
            open_interest_value = float(data['sumOpenInterestValue'])
        
        return {
            'timestamp': int(time.time() * 1000),
            'symbol': symbol,
            'openInterest': float(data['openInterest']),
            'openInterestValue': open_interest_value,
            'time': data['time']
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"网络请求失败: {symbol}, 错误: {e}")
        return None
    except Exception as e:
        logger.error(f"获取持仓量数据失败: {symbol}, 错误: {e}")
        # 不再返回模拟数据，直接返回None
        return None

def get_open_interest_history(symbol, interval, limit=2):
    """
    获取持仓量历史数据
    :param symbol: 币种对
    :param interval: 时间间隔
    :param limit: 数据点数量，至少为2以获取当前和前一个数据点
    :return: 历史数据
    """
    try:
        url = f"{BINANCE_BASE_URL}/futures/data/openInterestHist"
        params = {
            'symbol': symbol,
            'period': interval,
            'limit': max(2, limit)  # 确保至少获取2个数据点
        }
        
        # 使用会话
        session = get_session()
        logger.info(f"请求历史持仓量数据: {url}?symbol={symbol}&period={interval}&limit={limit}")
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        logger.info(f"历史持仓量数据响应: {data}")
        
        if data and len(data) >= 2:
            # 取最新的两个数据点（当前数据和前一个数据点）
            current_item = data[0]
            # previous_item = data[1]
            
            return {
                'timestamp': current_item['timestamp'],
                'symbol': symbol,
                'interval': interval,
                'openInterest': float(current_item['sumOpenInterest']),
                'openInterestValue': float(current_item.get('sumOpenInterestValue', 0)) if 'sumOpenInterestValue' in current_item else 0,
                # 'previous_openInterest': float(previous_item['sumOpenInterest']),
                # 'previous_openInterestValue': float(previous_item.get('sumOpenInterestValue', 0)) if 'sumOpenInterestValue' in previous_item else 0,
                'time': current_item['timestamp']
            }
        elif data and len(data) >= 1:
            # 如果只有一个数据点，只取最新的数据点
            current_item = data[0]
            
            return {
                'timestamp': current_item['timestamp'],
                'symbol': symbol,
                'interval': interval,
                'openInterest': float(current_item['sumOpenInterest']),
                'openInterestValue': float(current_item.get('sumOpenInterestValue', 0)) if 'sumOpenInterestValue' in current_item else 0,
                # 'previous_openInterest': None,
                # 'previous_openInterestValue': None,
                'time': current_item['timestamp']
            }
        
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"网络请求失败: {symbol}, {interval}, 错误: {e}")
        return None
    except Exception as e:
        logger.error(f"获取历史持仓量数据失败: {symbol}, {interval}, 错误: {e}")
        # 不再返回模拟数据，直接返回None
        return None

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

def get_all_coins_list():
    """获取所有币种列表"""
    try:
        all_coins = get_all_coins_from_binance()
        if all_coins:
            return [coin['symbol'] for coin in all_coins]
        else:
            # 如果无法从币安获取，使用已知的币种列表
            return ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'ADAUSDT', 'DOGEUSDT', 'DOTUSDT']
    except Exception as e:
        logger.error(f"获取所有币种列表失败: {e}")
        # 返回默认列表
        return ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']

def update_all_data(symbols=None, force_update=False):
    """
    更新所有币种的数据（并行处理）
    :param symbols: 币种列表，如果为None则从配置中获取
    :param force_update: 是否强制更新，忽略缓存
    """
    # 检查是否需要更新缓存
    if not force_update and not should_update_cache():
        logger.info("当前5分钟周期内已有缓存数据，跳过更新")
        return None
    
    # 如果没有提供币种列表，则获取所有币种
    if symbols is None:
        symbols = get_all_coins_list()
        logger.info(f"获取到 {len(symbols)} 个币种进行数据更新")
    
    logger.info(f"开始更新数据，共 {len(symbols)} 个币种...")
    
    all_coins_data = []
    
    # 使用线程池并行处理多个币种，减少线程数避免资源耗尽
    try:
        with ThreadPoolExecutor(max_workers=min(3, len(symbols) or 1)) as executor:
            # 提交所有币种的任务
            future_to_symbol = {
                executor.submit(update_single_coin_data, symbol): symbol 
                for symbol in symbols
            }
            
            # 收集结果
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    coin_data = future.result(timeout=30)  # 设置超时时间
                    if coin_data:
                        all_coins_data.append(coin_data)
                        logger.info(f"已更新 {symbol} 数据")
                    else:
                        logger.warning(f"未获取到 {symbol} 数据")
                except Exception as e:
                    logger.error(f"更新 {symbol} 数据时出错: {e}")
                    logger.exception(e)
    except RuntimeError as e:
        if "cannot schedule new futures after interpreter shutdown" in str(e):
            logger.error("检测到解释器关闭，停止并行处理")
            # 降级为串行处理
            for symbol in symbols:
                coin_data = update_single_coin_data(symbol)
                if coin_data:
                    all_coins_data.append(coin_data)
                    logger.info(f"已更新 {symbol} 数据（串行）")
                else:
                    logger.warning(f"未获取到 {symbol} 数据（串行）")
        else:
            logger.error(f"运行时错误: {e}")
            logger.exception(e)
            # 避免抛出异常导致服务中断
            # raise e
    except Exception as e:
        logger.error(f"并行处理过程中出现未预期的错误: {e}")
        logger.exception(e)
        # 避免抛出异常导致服务中断
        # raise e
    
    # 保存所有币种数据
    if all_coins_data:
        try:
            save_all_coins_data(all_coins_data)
            # 保存到缓存
            cache_key = get_cache_key()
            save_cached_data(cache_key, all_coins_data)
            logger.info("所有币种数据已保存")
        except Exception as e:
            logger.error(f"保存所有币种数据失败: {e}")
            logger.exception(e)
    else:
        logger.warning("没有币种数据需要保存")
    
    logger.info("数据更新完成")
    return all_coins_data

def update_single_coin_data(symbol):
    """
    更新单个币种的数据（并行处理多个时间周期）
    :param symbol: 币种
    """
    try:
        # 获取当前持仓量
        current_data = get_open_interest(symbol)
        if not current_data:
            logger.warning(f"未获取到 {symbol} 当前持仓量数据")
            return None
        
        # 获取最新价格并计算持仓价值
        if current_data and current_data.get('openInterestValue', 0) == 0:
            latest_price = get_latest_price(symbol)
            if latest_price is not None:
                open_interest = current_data.get('openInterest', 0)
                calculated_value = open_interest * latest_price
                current_data['openInterestValue'] = calculated_value
                logger.info(f"使用最新价格计算 {symbol} 持仓价值: {open_interest} * {latest_price} = {calculated_value}")
            else:
                logger.warning(f"无法获取 {symbol} 最新价格，持仓价值仍为0")
        
        # 获取24小时价格变化数据
        price_change_data = get_24hr_ticker(symbol)
        
        # 并行获取各时间间隔的历史数据，减少线程数避免资源耗尽
        intervals_data = []
        latest_history_data = {}  # 用于存储各时间间隔的最新数据
        
        try:
            with ThreadPoolExecutor(max_workers=min(3, len(TIME_INTERVALS) or 1)) as executor:
                # 提交所有时间周期的任务
                future_to_interval = {
                    executor.submit(get_open_interest_history, symbol, interval, limit=2): interval 
                    for interval in TIME_INTERVALS
                }
                
                # 收集结果
                for future in as_completed(future_to_interval):
                    interval = future_to_interval[future]
                    try:
                        history_data = future.result(timeout=30)  # 设置超时时间
                        if history_data:
                            # 保存历史数据用于计算变化比例
                            intervals_data.append(history_data)
                            
                            logger.info(f"已获取 {symbol} {interval} 数据: {history_data['openInterest']}")
                        else:
                            logger.warning(f"未获取到 {symbol} {interval} 历史数据")
                    except Exception as e:
                        logger.error(f"获取 {symbol} {interval} 数据时出错: {e}")
                        logger.exception(e)
        except RuntimeError as e:
            if "cannot schedule new futures after interpreter shutdown" in str(e):
                logger.error(f"检测到解释器关闭，停止并行处理 {symbol} 的时间周期数据")
                # 降级为串行处理
                for interval in TIME_INTERVALS:
                    history_data = get_open_interest_history(symbol, interval, limit=2)
                    if history_data:
                        intervals_data.append(history_data)
                        
                        logger.info(f"已获取 {symbol} {interval} 数据（串行）: {history_data['openInterest']}")
                    else:
                        logger.warning(f"未获取到 {symbol} {interval} 历史数据（串行）")
            else:
                logger.error(f"运行时错误: {e}")
                logger.exception(e)
                # 避免抛出异常导致服务中断
                # raise e
        except Exception as e:
            logger.error(f"并行处理时间周期数据过程中出现未预期的错误: {e}")
            logger.exception(e)
            # 避免抛出异常导致服务中断
            # raise e
        
        # 组装币种数据
        coin_data = {
            'symbol': symbol,
            'current': current_data,
            'intervals': intervals_data,
            'price_change': price_change_data,  # 添加价格变化数据
            'update_time': int(time.time() * 1000)
        }
        
        return coin_data
    except Exception as e:
        logger.error(f"更新 {symbol} 数据时出错: {e}")
        logger.exception(e)
        return None