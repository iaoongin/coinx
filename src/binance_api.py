import requests
import time
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from .config import BINANCE_BASE_URL, TIME_INTERVALS, USE_PROXY, PROXY_URL, HTTPS_PROXY_URL
from .utils import save_all_coins_data, logger

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
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        return {
            'timestamp': int(time.time() * 1000),
            'symbol': symbol,
            'openInterest': float(data['openInterest']),
            'time': data['time']
        }
    except Exception as e:
        logger.error(f"获取持仓量数据失败: {symbol}, 错误: {e}")
        # 不再返回模拟数据，直接返回None
        return None

def get_open_interest_history(symbol, interval, limit=1):
    """
    获取持仓量历史数据
    :param symbol: 币种对
    :param interval: 时间间隔
    :param limit: 数据点数量
    :return: 历史数据
    """
    try:
        url = f"{BINANCE_BASE_URL}/futures/data/openInterestHist"
        params = {
            'symbol': symbol,
            'period': interval,
            'limit': limit
        }
        
        # 使用会话
        session = get_session()
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data:
            item = data[0]  # 取最新的数据点
            return {
                'timestamp': item['timestamp'],
                'symbol': symbol,
                'interval': interval,
                'openInterest': float(item['sumOpenInterest']),
                'time': item['timestamp']
            }
        
        return None
    except Exception as e:
        logger.error(f"获取历史持仓量数据失败: {symbol}, {interval}, 错误: {e}")
        # 不再返回模拟数据，直接返回None
        return None

def update_all_data(symbols=['BTCUSDT']):
    """
    更新所有币种的数据（并行处理）
    :param symbols: 币种列表
    """
    logger.info("开始更新数据...")
    
    all_coins_data = []
    
    # 使用线程池并行处理多个币种
    with ThreadPoolExecutor(max_workers=5) as executor:
        # 提交所有币种的任务
        future_to_symbol = {
            executor.submit(update_single_coin_data, symbol): symbol 
            for symbol in symbols
        }
        
        # 收集结果
        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                coin_data = future.result()
                if coin_data:
                    all_coins_data.append(coin_data)
                    logger.info(f"已更新 {symbol} 数据")
                else:
                    logger.warning(f"未获取到 {symbol} 数据")
            except Exception as e:
                logger.error(f"更新 {symbol} 数据时出错: {e}")
    
    # 保存所有币种数据
    if all_coins_data:
        save_all_coins_data(all_coins_data)
        logger.info("所有币种数据已保存")
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
        
        # 并行获取各时间间隔的历史数据
        intervals_data = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            # 提交所有时间周期的任务
            future_to_interval = {
                executor.submit(get_open_interest_history, symbol, interval, limit=1): interval 
                for interval in TIME_INTERVALS
            }
            
            # 收集结果
            for future in as_completed(future_to_interval):
                interval = future_to_interval[future]
                try:
                    history_data = future.result()
                    if history_data:
                        intervals_data.append(history_data)
                        logger.info(f"已获取 {symbol} {interval} 数据")
                    else:
                        logger.warning(f"未获取到 {symbol} {interval} 数据")
                except Exception as e:
                    logger.error(f"获取 {symbol} {interval} 数据时出错: {e}")
        
        # 组装币种数据
        coin_data = {
            'symbol': symbol,
            'current': current_data,
            'intervals': intervals_data,
            'update_time': int(time.time() * 1000)
        }
        
        return coin_data
    except Exception as e:
        logger.error(f"更新 {symbol} 数据时出错: {e}")
        return None
