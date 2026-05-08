import time

from coinx.config import OKX_BASE_URL
from coinx.collector.binance.client import get_session, request_with_retry
from coinx.repositories.series import upsert_series_records
from coinx.utils import logger


OKX_EXCHANGE_ID = 'okx'
SUPPORTED_SERIES_TYPES = ('klines', 'open_interest_hist', 'taker_buy_sell_vol')
RUBIK_HISTORY_LIMIT_MS = {
    '5m': 2 * 24 * 60 * 60 * 1000,
    '1H': 30 * 24 * 60 * 60 * 1000,
    '1h': 30 * 24 * 60 * 60 * 1000,
    '1D': 180 * 24 * 60 * 60 * 1000,
    '1d': 180 * 24 * 60 * 60 * 1000,
}
_SUPPORTED_SYMBOLS_TTL_SECONDS = 60 * 60
_supported_symbols_cache = {
    'loaded_at': 0,
    'failed_at': 0,
    'symbols': None,
}


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


def _is_live_usdt_swap(instrument):
    inst_id = instrument.get('instId') if isinstance(instrument, dict) else None
    if not inst_id or not inst_id.endswith('-USDT-SWAP'):
        return False
    state = instrument.get('state')
    return state in (None, '', 'live')


def clear_supported_symbols_cache():
    _supported_symbols_cache['loaded_at'] = 0
    _supported_symbols_cache['failed_at'] = 0
    _supported_symbols_cache['symbols'] = None


def get_supported_symbols(session=None, ttl_seconds=_SUPPORTED_SYMBOLS_TTL_SECONDS):
    now = time.time()
    cached_symbols = _supported_symbols_cache.get('symbols')
    loaded_at = _supported_symbols_cache.get('loaded_at') or 0
    if cached_symbols is not None and now - loaded_at < ttl_seconds:
        return cached_symbols

    instruments = _request_okx(
        '/api/v5/public/instruments',
        {'instType': 'SWAP'},
        session=session,
    )
    symbols = {
        to_internal_symbol(instrument['instId'])
        for instrument in instruments
        if _is_live_usdt_swap(instrument)
    }
    _supported_symbols_cache['symbols'] = symbols
    _supported_symbols_cache['loaded_at'] = now
    return symbols


def is_symbol_supported(symbol, series_type=None, session=None):
    if series_type not in (None, *SUPPORTED_SERIES_TYPES):
        return False
    failed_at = _supported_symbols_cache.get('failed_at') or 0
    if _supported_symbols_cache.get('symbols') is None and time.time() - failed_at < 60:
        return True
    try:
        return symbol in get_supported_symbols(session=session)
    except Exception as exc:
        _supported_symbols_cache['failed_at'] = time.time()
        logger.warning(f"OKX supported symbol cache unavailable, fallback to request path: {exc}")
        return True


def _request_okx(path, params, session=None, timeout=10):
    http_session = session or get_session()
    url = f"{OKX_BASE_URL}{path}"
    response = request_with_retry(http_session, url, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict) and payload.get('code') not in (None, '0', 0):
        raise ValueError(f"OKX 鎺ュ彛杩斿洖閿欒 {payload.get('code')}: {payload.get('msg')}")
    return payload.get('data', payload)


def _okx_bar(period):
    return period


def _rubik_time_window(period, start_time=None, end_time=None, now_ms=None):
    history_limit_ms = RUBIK_HISTORY_LIMIT_MS.get(period)
    if history_limit_ms is None:
        return start_time, end_time

    current_time_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    earliest_time = current_time_ms - history_limit_ms
    effective_start = max(start_time, earliest_time) if start_time is not None else earliest_time
    effective_end = end_time

    if effective_end is not None and effective_end < earliest_time:
        return None, None
    if effective_end is not None and effective_start is not None and effective_start > effective_end:
        return None, None
    return effective_start, effective_end


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
    start_time, end_time = _rubik_time_window(period, start_time=start_time, end_time=end_time)
    if start_time is None and end_time is None:
        return []

    params = {
        'ccy': _base_asset(symbol),
        'period': _okx_bar(period),
    }
    if start_time is not None:
        params['begin'] = str(start_time)
    if end_time is not None:
        params['end'] = str(end_time)
    payload = _request_okx('/api/v5/rubik/stat/contracts/open-interest-volume', params, session=session)
    if limit:
        return payload[:limit]
    return payload


def fetch_taker_buy_sell_vol(symbol, period, limit, session=None, start_time=None, end_time=None):
    start_time, end_time = _rubik_time_window(period, start_time=start_time, end_time=end_time)
    if start_time is None and end_time is None:
        return []

    params = {
        'ccy': _base_asset(symbol),
        'instType': 'CONTRACTS',
        'period': _okx_bar(period),
    }
    if start_time is not None:
        params['begin'] = str(start_time)
    if end_time is not None:
        params['end'] = str(end_time)
    payload = _request_okx('/api/v5/rubik/stat/taker-volume', params, session=session)
    if limit:
        return payload[:limit]
    return payload


def get_funding_rate(symbol, session=None):
    payload = _request_okx(
        '/api/v5/public/funding-rate',
        {'instId': to_exchange_symbol(symbol)},
        session=session,
    )
    row = payload[0] if isinstance(payload, list) and payload else payload
    if not isinstance(row, dict):
        return None
    return {
        'exchange': OKX_EXCHANGE_ID,
        'symbol': symbol,
        'instId': row.get('instId'),
        'fundingRate': _to_float(row.get('fundingRate')),
        'nextFundingTime': int(row['nextFundingTime']) if row.get('nextFundingTime') else None,
        'fundingTime': int(row['fundingTime']) if row.get('fundingTime') else None,
    }


def get_all_funding_rates(session=None):
    logger.info('开始加载全量资金费率: exchange=okx')
    payload = _request_okx(
        '/api/v5/public/funding-rate',
        {'instId': 'ANY'},
        session=session,
    )
    rows = payload if isinstance(payload, list) else []
    result = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = to_internal_symbol(row.get('instId', '')) if row.get('instId') else None
        if not symbol:
            continue
        result[symbol] = {
            'exchange': OKX_EXCHANGE_ID,
            'symbol': symbol,
            'instId': row.get('instId'),
            'fundingRate': _to_float(row.get('fundingRate')),
            'nextFundingTime': int(row['nextFundingTime']) if row.get('nextFundingTime') else None,
            'fundingTime': int(row['fundingTime']) if row.get('fundingTime') else None,
        }
    logger.info('全量资金费率加载完成: exchange=okx count=%d', len(result))
    return result


def parse_klines(payload, symbol, period):
    parsed = []
    for item in payload:
        # OKX K绾挎暟缁? [鏃堕棿鎴? 寮€鐩樹环, 鏈€楂樹环, 鏈€浣庝环, 鏀剁洏浠? 鎴愪氦閲? 甯佺鎴愪氦閲? 璁′环鎴愪氦棰? 鏄惁纭]
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
        raise ValueError(f"涓嶆敮鎸佺殑 OKX 搴忓垪绫诲瀷: {series_type}") from exc
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
        raise ValueError(f"涓嶆敮鎸佺殑 OKX 搴忓垪绫诲瀷: {series_type}") from exc
    return parser(payload, symbol, period)


def collect_and_store_series(series_type, symbol, period, limit, http_session=None, db_session=None):
    logger.info(f"寮€濮嬮噰闆?OKX 搴忓垪鏁版嵁: 绫诲瀷={series_type}, 甯佺={symbol}, 鍛ㄦ湡={period}, 鏉℃暟={limit}")
    payload = fetch_series_payload(series_type, symbol, period, limit, session=http_session)
    records = parse_series_payload(series_type, payload, symbol, period)
    affected = upsert_series_records(OKX_EXCHANGE_ID, series_type, records, session=db_session)
    logger.info(f"OKX 搴忓垪鏁版嵁閲囬泦瀹屾垚: 绫诲瀷={series_type}, 甯佺={symbol}, 褰卞搷琛屾暟={affected}")
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
    raise ValueError(f'涓嶆敮鎸佺殑鍛ㄦ湡: {period}')
