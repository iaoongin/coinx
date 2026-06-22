"""Binance 资金费率采集模块"""
from coinx.config import BINANCE_BASE_URL
from coinx.collector.binance.client import get_session, request_with_binance_retry


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
        symbol = data.get('symbol', '')
        predicted_rate = data.get('nextFundingRate')
        if predicted_rate is not None:
            predicted_rate = float(predicted_rate)

        results.append({
            'symbol': symbol,
            'period': '5m',
            'event_time': int(data.get('time', 0)),
            'funding_rate': float(data.get('lastFundingRate', 0)),
            'predicted_rate': predicted_rate,
            'next_funding_time': int(data.get('nextFundingTime', 0)),
            'mark_price': float(data.get('markPrice', 0)),
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

    # 注意: Binance API 不保证返回 nextFundingRate 字段
    predicted_rate = data.get('nextFundingRate')
    if predicted_rate is not None:
        predicted_rate = float(predicted_rate)

    return {
        'symbol': symbol,
        'period': '5m',
        'event_time': int(data.get('time', 0)),
        'funding_rate': float(data.get('lastFundingRate', 0)),
        'predicted_rate': predicted_rate,
        'next_funding_time': int(data.get('nextFundingTime', 0)),
        'mark_price': float(data.get('markPrice', 0)),
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
    # 注意: Binance API 不保证返回 nextFundingRate 字段
    predicted_rate = payload.get('nextFundingRate')
    if predicted_rate is not None:
        predicted_rate = float(predicted_rate)

    return [{
        'symbol': symbol,
        'period': period,
        'event_time': int(payload.get('time', 0)),
        'funding_rate': float(payload.get('lastFundingRate', 0)),
        'predicted_rate': predicted_rate,
        'next_funding_time': int(payload.get('nextFundingTime', 0)),
        'mark_price': float(payload.get('markPrice', 0)),
    }]
