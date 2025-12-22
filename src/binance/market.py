import time
import requests
from src.config import BINANCE_BASE_URL
from src.utils import logger
from .client import get_session, request_with_retry

def get_futures_kline_latest(symbol, interval):
    """获取期货K线的最新一根，返回字典包含 quoteVolume 与 takerBuyQuoteVolume 等"""
    try:
        url = f"{BINANCE_BASE_URL}/fapi/v1/klines"
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': 1
        }
        session = get_session()
        response = request_with_retry(session, url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        if not data:
            return None
        k = data[0]
        # 参考返回字段顺序
        # [ openTime, open, high, low, close, volume, closeTime, quoteVolume, count, takerBuyBaseVolume, takerBuyQuoteVolume, ignore ]
        return {
            'openTime': k[0],
            'closeTime': k[6],
            'quoteVolume': float(k[7]),
            'takerBuyBaseVolume': float(k[9]),
            'takerBuyQuoteVolume': float(k[10])
        }
    except Exception as e:
        logger.error(f"获取期货K线失败: {symbol}, {interval}, 错误: {e}")
        return None

def aggregate_futures_kline(symbol, base_interval, count):
    """聚合多根K线，返回合计的 quoteVolume 与 takerBuyQuoteVolume"""
    try:
        url = f"{BINANCE_BASE_URL}/fapi/v1/klines"
        params = {
            'symbol': symbol,
            'interval': base_interval,
            'limit': max(1, min(1000, count))
        }
        session = get_session()
        response = request_with_retry(session, url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        if not data:
            return None
        # 只取最近 count 根
        klines = data[-count:]
        sum_quote = 0.0
        sum_taker_buy_quote = 0.0
        for k in klines:
            sum_quote += float(k[7])
            sum_taker_buy_quote += float(k[10])
        return {
            'quoteVolume': sum_quote,
            'takerBuyQuoteVolume': sum_taker_buy_quote
        }
    except Exception as e:
        logger.error(f"聚合期货K线失败: {symbol}, {base_interval} x {count}, 错误: {e}")
        return None

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
        response = request_with_retry(session, url, params=params, timeout=10)
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
        response = request_with_retry(session, url, params=params, timeout=10)
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
        response = request_with_retry(session, url, params=params, timeout=10)
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
        response = request_with_retry(session, url, params=params, timeout=10)
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

def get_funding_rate(symbol):
    """
    获取指定币种的资金费率
    :param symbol: 币种对，如 BTCUSDT
    :return: 资金费率数据
    """
    try:
        url = f"{BINANCE_BASE_URL}/fapi/v1/premiumIndex"
        params = {
            'symbol': symbol
        }
        
        # 使用会话
        session = get_session()
        logger.info(f"请求资金费率数据: {url}?symbol={symbol}")
        response = request_with_retry(session, url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        logger.info(f"资金费率数据响应: {data}")
        
        return {
            'symbol': symbol,
            'markPrice': float(data['markPrice']),
            'indexPrice': float(data['indexPrice']),
            'estimatedSettlePrice': float(data['estimatedSettlePrice']),
            'lastFundingRate': float(data['lastFundingRate']),
            'nextFundingTime': int(data['nextFundingTime']),
            'interestRate': float(data['interestRate']),
            'time': int(data['time'])
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"网络请求失败: {symbol}, 错误: {e}")
        return None
    except Exception as e:
        logger.error(f"获取资金费率失败: {symbol}, 错误: {e}")
        return None

def get_long_short_ratio(symbol, period='5m', limit=30):
    """
    获取指定币种的多空比数据
    注意：币安API可能不提供此数据，返回模拟数据
    :param symbol: 币种对，如 BTCUSDT
    :param period: 时间周期，如 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d
    :param limit: 数据点数量
    :return: 多空比数据
    """
    try:
        # 币安API可能不提供多空比数据，返回模拟数据
        logger.info(f"多空比数据API不可用，返回模拟数据: {symbol}")
        
        # 返回模拟的多空比数据
        return {
            'symbol': symbol,
            'period': period,
            'longShortRatio': 1.25,  # 模拟数据
            'longAccount': 0.55,      # 55%多头账户
            'shortAccount': 0.45,     # 45%空头账户
            'timestamp': int(time.time() * 1000)
        }
        
    except Exception as e:
        logger.error(f"获取多空比数据失败: {symbol}, {period}, 错误: {e}")
        return None
