"""Binance 资金费率采集模块"""
from coinx.config import BINANCE_BASE_URL
from coinx.collector.binance.client import get_session, request_with_binance_retry
from coinx.utils import logger


def _optional_float(value):
    if value is None or value == '':
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_int(value):
    if value is None or value == '':
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _required_event_time(value):
    event_time = _optional_int(value)
    if event_time is None:
        raise ValueError('Binance funding-rate response is missing time')
    return event_time


def fetch_all_premium_index(session=None):
    """
    批量获取所有币种的资金费率（单次请求）

    Args:
        session: HTTP session（可选）

    Returns:
        list: 所有币种的资金费率数据列表
    """
    url = f"{BINANCE_BASE_URL}/fapi/v1/premiumIndex"
    http_session = session or get_session()
    response = request_with_binance_retry(http_session, url)
    response.raise_for_status()
    data_list = response.json()

    results = []
    for data in data_list:
        symbol = data.get('symbol')
        if not symbol:
            logger.warning('跳过缺少 symbol 的 Binance 资金费率记录')
            continue
        event_time = _optional_int(data.get('time'))
        if event_time is None:
            logger.warning('跳过缺少 time 的 Binance 资金费率记录: symbol=%s', symbol)
            continue

        results.append({
            'symbol': symbol,
            'period': '5m',
            'event_time': event_time,
            'funding_rate': _optional_float(data.get('lastFundingRate')),
            'predicted_rate': _optional_float(data.get('nextFundingRate')),
            'next_funding_time': _optional_int(data.get('nextFundingTime')),
            'mark_price': _optional_float(data.get('markPrice')),
        })

    return results


def fetch_premium_index(symbol, session=None):
    """
    获取单个币种的预测资金费率

    Args:
        symbol: 交易对名称，如 'BTCUSDT'
        session: HTTP session（可选）

    Returns:
        dict: 包含 funding_rate, predicted_rate, next_funding_time, mark_price
    """
    params = {'symbol': symbol}
    url = f"{BINANCE_BASE_URL}/fapi/v1/premiumIndex"
    http_session = session or get_session()
    response = request_with_binance_retry(http_session, url, params=params)
    response.raise_for_status()
    data = response.json()

    event_time = _optional_int(data.get('time'))
    if event_time is None:
        logger.warning('Binance 资金费率响应缺少 time，跳过记录: symbol=%s', symbol)
        return None

    return {
        'symbol': symbol,
        'period': '5m',
        'event_time': event_time,
        'funding_rate': _optional_float(data.get('lastFundingRate')),
        'predicted_rate': _optional_float(data.get('nextFundingRate')),
        'next_funding_time': _optional_int(data.get('nextFundingTime')),
        'mark_price': _optional_float(data.get('markPrice')),
    }


def parse_funding_rate(payload, symbol, period='5m'):
    """
    解析资金费率数据（兼容 series 框架）

    Args:
        payload: API 响应数据
        symbol: 交易对名称
        period: 采集周期

    Returns:
        list: 解析后的记录列表
    """
    if not isinstance(payload, dict) or not symbol:
        return []
    try:
        event_time = _required_event_time(payload.get('time'))
    except ValueError:
        logger.warning('Binance 资金费率响应缺少 time，跳过记录: symbol=%s', symbol)
        return []

    return [{
        'symbol': symbol,
        'period': period,
        'event_time': event_time,
        'funding_rate': _optional_float(payload.get('lastFundingRate')),
        'predicted_rate': _optional_float(payload.get('nextFundingRate')),
        'next_funding_time': _optional_int(payload.get('nextFundingTime')),
        'mark_price': _optional_float(payload.get('markPrice')),
    }]
