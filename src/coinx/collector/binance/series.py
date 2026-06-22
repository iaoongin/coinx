from coinx.config import BINANCE_BASE_URL
from coinx.collector.binance.client import get_session, request_with_binance_retry
from .funding_rate import fetch_premium_index as fetch_funding_rate
from .funding_rate import parse_funding_rate


SERIES_ENDPOINTS = {
    'open_interest_hist': '/futures/data/openInterestHist',
    'klines': '/fapi/v1/klines',
    'taker_buy_sell_vol': '/futures/data/takerlongshortRatio',
    'funding_rate': '/fapi/v1/premiumIndex',
}

DEFAULT_SERIES_TYPES = list(SERIES_ENDPOINTS.keys())


def _to_float(value):
    if value is None or value == '':
        return None
    return float(value)


def _request_series(path, params, session=None, timeout=10):
    http_session = session or get_session()
    url = f"{BINANCE_BASE_URL}{path}"
    response = request_with_binance_retry(http_session, url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def fetch_open_interest_hist(symbol, period, limit, session=None, start_time=None, end_time=None):
    params = {'symbol': symbol, 'period': period, 'limit': limit}
    if start_time is not None:
        params['startTime'] = start_time
    if end_time is not None:
        params['endTime'] = end_time
    return _request_series(
        SERIES_ENDPOINTS['open_interest_hist'],
        params,
        session=session,
    )


def fetch_klines(symbol, period, limit, session=None, start_time=None, end_time=None):
    params = {'symbol': symbol, 'interval': period, 'limit': limit}
    if start_time is not None:
        params['startTime'] = start_time
    if end_time is not None:
        params['endTime'] = end_time
    return _request_series(
        SERIES_ENDPOINTS['klines'],
        params,
        session=session,
    )


def fetch_taker_buy_sell_vol(symbol, period, limit, session=None, start_time=None, end_time=None):
    params = {'symbol': symbol, 'period': period, 'limit': limit}
    if start_time is not None:
        params['startTime'] = start_time
    if end_time is not None:
        params['endTime'] = end_time
    return _request_series(
        SERIES_ENDPOINTS['taker_buy_sell_vol'],
        params,
        session=session,
    )


def parse_open_interest_hist(payload, symbol, period):
    return [
        {
            'symbol': item.get('symbol', symbol),
            'period': period,
            'event_time': int(item['timestamp']),
            'sum_open_interest': _to_float(item.get('sumOpenInterest')),
            'sum_open_interest_value': _to_float(item.get('sumOpenInterestValue')),
        }
        for item in payload
    ]


def parse_klines(payload, symbol, period):
    parsed = []
    for item in payload:
        parsed.append(
            {
                'symbol': symbol,
                'period': period,
                'open_time': int(item[0]),
                'close_time': int(item[6]),
                'open_price': _to_float(item[1]),
                'high_price': _to_float(item[2]),
                'low_price': _to_float(item[3]),
                'close_price': _to_float(item[4]),
                'volume': _to_float(item[5]),
                'quote_volume': _to_float(item[7]),
                'trade_count': int(item[8]),
                'taker_buy_base_volume': _to_float(item[9]),
                'taker_buy_quote_volume': _to_float(item[10]),
            }
        )
    return parsed


def parse_taker_buy_sell_vol(payload, symbol, period):
    return [
        {
            'symbol': item.get('symbol', symbol),
            'period': period,
            'event_time': int(item['timestamp']),
            'buy_sell_ratio': _to_float(item.get('buySellRatio')),
            'buy_vol': _to_float(item.get('buyVol')),
            'sell_vol': _to_float(item.get('sellVol')),
        }
        for item in payload
    ]


def fetch_series_payload(series_type, symbol, period, limit, session=None, start_time=None, end_time=None):
    if series_type == 'funding_rate':
        return fetch_funding_rate(symbol, session=session)

    fetchers = {
        'open_interest_hist': fetch_open_interest_hist,
        'klines': fetch_klines,
        'taker_buy_sell_vol': fetch_taker_buy_sell_vol,
    }

    try:
        fetcher = fetchers[series_type]
    except KeyError as exc:
        raise ValueError(f"不支持的类型: {series_type}") from exc

    return fetcher(
        symbol,
        period,
        limit,
        session=session,
        start_time=start_time,
        end_time=end_time,
    )


def parse_series_payload(series_type, payload, symbol, period):
    parsers = {
        'open_interest_hist': parse_open_interest_hist,
        'klines': parse_klines,
        'taker_buy_sell_vol': parse_taker_buy_sell_vol,
        'funding_rate': parse_funding_rate,
    }

    try:
        parser = parsers[series_type]
    except KeyError as exc:
        raise ValueError(f"涓嶆敮鎸佺殑搴忓垪绫诲瀷: {series_type}") from exc

    return parser(payload, symbol, period)
