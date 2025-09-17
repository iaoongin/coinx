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
from src.coin_manager import get_active_coins

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
    except Exception as e:
        logger.error(f"获取最新价格失败: {symbol}, 错误: {e}")
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
        
        if data and len(data) >= 1:
            # 只取最新的数据点（当前数据）
            current_item = data[0]
            
            return {
                'timestamp': current_item['timestamp'],
                'symbol': symbol,
                'interval': interval,
                'openInterest': float(current_item['sumOpenInterest']),
                'openInterestValue': float(current_item.get('sumOpenInterestValue', 0)) if 'sumOpenInterestValue' in current_item else 0,
                'time': current_item['timestamp']
            }
        
        return None
    except Exception as e:
        logger.error(f"获取历史持仓量数据失败: {symbol}, {interval}, 错误: {e}")
        # 不再返回模拟数据，直接返回None
        return None

def update_all_data(symbols=None):
    """
    更新所有币种的数据（并行处理）
    :param symbols: 币种列表，如果为None则从配置中获取
    """
    # 如果没有提供币种列表，则从配置中获取
    if symbols is None:
        symbols = get_active_coins()
    
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
            raise e
    
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
                raise e
        
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
