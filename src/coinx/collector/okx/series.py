import time

from coinx.config import OKX_BASE_URL
from coinx.collector.binance.client import get_session, request_with_retry
from coinx.repositories.series import upsert_series_records
from coinx.utils import logger


OKX_EXCHANGE_ID = 'okx'
SUPPORTED_SERIES_TYPES = ('klines', 'open_interest_hist', 'taker_buy_sell_vol')


def _to_float(value):
    if value is None or value == '':
        return None
    return float(value)


def _base_asset(symbol):
    if symbol.endswith('USDT'):
        return symbol[:-4]
    return symbol


def to_exchange_symbol(symbol):
    if '-' in symbol:
        return symbol
    if symbol.endswith('USDT'):
        return f"{symbol[:-4]}-USDT-SWAP"
    return symbol


def to_internal_symbol(inst_id):
    parts = inst_id.split('-')
    if len(parts) >= 2 and parts[-1] == 'SWAP':
        return f"{parts[0]}{parts[1]}"
    return inst_id.replace('-', '')


def _request_okx(path, params, session=None, timeout=10):
    http_session = session or get_session()
    url = f"{OKX_BASE_URL}{path}"
    response = request_with_retry(http_session, url, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict) and payload.get('code') not in (None, '0', 0):
        raise ValueError(f"OKX 接口返回错误 {payload.get('code')}: {payload.get('msg')}")
    return payload.get('data', payload)


def _okx_bar(period):
    return period


def fetch_klines(symbol, period, limit, session=None, start_time=None, end_time=None):
    params = {
        'instId': to_exchange_symbol(symbol),
        'bar': _okx_bar(period),
        'limit': str(limit),
    }
    if end_time is not None:
        params['before'] = str(end_time)
    if start_time is not None:
        params['after'] = str(start_time)
    return _request_okx('/api/v5/market/history-candles', params, session=session)


def fetch_open_interest_hist(symbol, period, limit, session=None, start_time=None, end_time=None):
    params = {
        'ccy': _base_asset(symbol),
        'period': _okx_bar(period),
    }
    payload = _request_okx('/api/v5/rubik/stat/contracts/open-interest-volume', params, session=session)
    if limit:
        return payload[:limit]
    return payload


def fetch_taker_buy_sell_vol(symbol, period, limit, session=None, start_time=None, end_time=None):
    params = {
        'ccy': _base_asset(symbol),
        'instType': 'CONTRACTS',
        'period': _okx_bar(period),
    }
    payload = _request_okx('/api/v5/rubik/stat/taker-volume', params, session=session)
    if limit:
        return payload[:limit]
    return payload


def parse_klines(payload, symbol, period):
    parsed = []
    for item in payload:
        # OKX K线数组: [时间戳, 开盘价, 最高价, 最低价, 收盘价, 成交量, 币种成交量, 计价成交额, 是否确认]
        open_time = int(item[0])
        parsed.append(
            {
                'symbol': symbol,
                'period': period,
                'open_time': open_time,
                'close_time': open_time + _period_to_ms(period) - 1,
                'open_price': _to_float(item[1]),
                'high_price': _to_float(item[2]),
                'low_price': _to_float(item[3]),
                'close_price': _to_float(item[4]),
                'volume': _to_float(item[5]) if len(item) > 5 else None,
                'quote_volume': _to_float(item[7]) if len(item) > 7 else None,
                'trade_count': None,
                'taker_buy_base_volume': None,
                'taker_buy_quote_volume': None,
                'raw_json': item,
            }
        )
    return parsed


def parse_open_interest_hist(payload, symbol, period):
    parsed = []
    for item in payload:
        if isinstance(item, dict):
            event_time = int(item.get('ts') or item.get('timestamp') or item.get('time'))
            open_interest = _to_float(
                item.get('oi')
                or item.get('openInterest')
                or item.get('openInterestContract')
                or item.get('sumOpenInterest')
            )
            open_interest_value = _to_float(
                item.get('oiCcy')
                or item.get('oiUsd')
                or item.get('openInterestValue')
                or item.get('sumOpenInterestValue')
            )
        else:
            # OKX 统计序列通常是时间戳在第一位的数组。
            event_time = int(item[0])
            open_interest = None
            open_interest_value = _to_float(item[1]) if len(item) > 1 else None

        parsed.append(
            {
                'symbol': symbol,
                'period': period,
                'event_time': event_time,
                'sum_open_interest': open_interest,
                'sum_open_interest_value': open_interest_value,
                'raw_json': item,
            }
        )
    return parsed


def parse_taker_buy_sell_vol(payload, symbol, period):
    parsed = []
    for item in payload:
        if isinstance(item, dict):
            event_time = int(item.get('ts') or item.get('timestamp') or item.get('time'))
            buy_vol = _to_float(item.get('buyVol') or item.get('buyVolume') or item.get('buy'))
            sell_vol = _to_float(item.get('sellVol') or item.get('sellVolume') or item.get('sell'))
        else:
            # OKX 主动买卖量序列通常是时间戳在第一位的数组。
            event_time = int(item[0])
            sell_vol = _to_float(item[1]) if len(item) > 1 else None
            buy_vol = _to_float(item[2]) if len(item) > 2 else None

        ratio = None
        if sell_vol not in (None, 0):
            ratio = buy_vol / sell_vol if buy_vol is not None else None
        parsed.append(
            {
                'symbol': symbol,
                'period': period,
                'event_time': event_time,
                'buy_sell_ratio': ratio,
                'buy_vol': buy_vol,
                'sell_vol': sell_vol,
                'raw_json': item,
            }
        )
    return parsed


def fetch_series_payload(series_type, symbol, period, limit, session=None, start_time=None, end_time=None):
    fetchers = {
        'klines': fetch_klines,
        'open_interest_hist': fetch_open_interest_hist,
        'taker_buy_sell_vol': fetch_taker_buy_sell_vol,
    }
    try:
        fetcher = fetchers[series_type]
    except KeyError as exc:
        raise ValueError(f"不支持的 OKX 序列类型: {series_type}") from exc
    return fetcher(symbol, period, limit, session=session, start_time=start_time, end_time=end_time)


def parse_series_payload(series_type, payload, symbol, period):
    parsers = {
        'klines': parse_klines,
        'open_interest_hist': parse_open_interest_hist,
        'taker_buy_sell_vol': parse_taker_buy_sell_vol,
    }
    try:
        parser = parsers[series_type]
    except KeyError as exc:
        raise ValueError(f"不支持的 OKX 序列类型: {series_type}") from exc
    return parser(payload, symbol, period)


def collect_and_store_series(series_type, symbol, period, limit, http_session=None, db_session=None):
    logger.info(f"开始采集 OKX 序列数据: 类型={series_type}, 币种={symbol}, 周期={period}, 条数={limit}")
    payload = fetch_series_payload(series_type, symbol, period, limit, session=http_session)
    records = parse_series_payload(series_type, payload, symbol, period)
    affected = upsert_series_records(OKX_EXCHANGE_ID, series_type, records, session=db_session)
    logger.info(f"OKX 序列数据采集完成: 类型={series_type}, 币种={symbol}, 影响行数={affected}")
    return {
        'exchange': OKX_EXCHANGE_ID,
        'series_type': series_type,
        'symbol': symbol,
        'period': period,
        'limit': limit,
        'affected': affected,
        'records': records,
    }


def collect_series_batch(symbols, periods, series_types=None, limit=30, http_session=None, db_session=None):
    active_series_types = series_types or list(SUPPORTED_SERIES_TYPES)
    results = []
    for symbol in symbols:
        for period in periods:
            for series_type in active_series_types:
                try:
                    result = collect_and_store_series(
                        series_type,
                        symbol,
                        period,
                        limit,
                        http_session=http_session,
                        db_session=db_session,
                    )
                    result['status'] = 'success'
                    results.append(result)
                except Exception as exc:
                    results.append(
                        {
                            'exchange': OKX_EXCHANGE_ID,
                            'series_type': series_type,
                            'symbol': symbol,
                            'period': period,
                            'limit': limit,
                            'status': 'error',
                            'error': str(exc),
                        }
                    )
    success_count = sum(1 for item in results if item.get('status') == 'success')
    return {
        'exchange': OKX_EXCHANGE_ID,
        'symbols': symbols,
        'periods': periods,
        'series_types': active_series_types,
        'limit': limit,
        'success_count': success_count,
        'failure_count': len(results) - success_count,
        'results': results,
    }


def _period_to_ms(period):
    if period.endswith('m'):
        return int(period[:-1]) * 60 * 1000
    if period.endswith('h'):
        return int(period[:-1]) * 60 * 60 * 1000
    if period.endswith('d'):
        return int(period[:-1]) * 24 * 60 * 60 * 1000
    raise ValueError(f'不支持的周期: {period}')
