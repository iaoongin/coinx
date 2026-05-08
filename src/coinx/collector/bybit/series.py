import time

from coinx.collector.binance.client import get_session, request_with_retry
from coinx.config import BYBIT_BASE_URL, BYBIT_CATEGORY
from coinx.repositories.series import upsert_series_records
from coinx.utils import logger


BYBIT_EXCHANGE_ID = 'bybit'
SUPPORTED_SERIES_TYPES = ('klines', 'open_interest_hist')
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


def _period_to_ms(period):
    if period.endswith('m'):
        return int(period[:-1]) * 60 * 1000
    if period.endswith('h'):
        return int(period[:-1]) * 60 * 60 * 1000
    if period.endswith('d'):
        return int(period[:-1]) * 24 * 60 * 60 * 1000
    raise ValueError(f'unsupported Bybit period: {period}')


def _bybit_interval(period):
    if period.endswith('m'):
        return period[:-1]
    if period.endswith('h'):
        return str(int(period[:-1]) * 60)
    if period.endswith('d') and period[:-1] == '1':
        return 'D'
    raise ValueError(f'unsupported Bybit kline period: {period}')


def _bybit_open_interest_interval(period):
    mapping = {
        '5m': '5min',
        '15m': '15min',
        '30m': '30min',
        '1h': '1h',
        '4h': '4h',
        '1d': '1d',
    }
    try:
        return mapping[period]
    except KeyError as exc:
        raise ValueError(f'unsupported Bybit open interest period: {period}') from exc


def _request_bybit(path, params, session=None, timeout=10):
    http_session = session or get_session()
    url = f"{BYBIT_BASE_URL}{path}"
    response = request_with_retry(http_session, url, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    ret_code = payload.get('retCode') if isinstance(payload, dict) else None
    if ret_code not in (None, 0, '0'):
        raise ValueError(f"Bybit API error {payload.get('retCode')}: {payload.get('retMsg')}")
    return payload.get('result', payload)


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

    symbols = set()
    cursor = None
    while True:
        params = {'category': BYBIT_CATEGORY, 'status': 'Trading'}
        if cursor:
            params['cursor'] = cursor
        result = _request_bybit('/v5/market/instruments-info', params, session=session)
        for instrument in result.get('list', []):
            symbol = instrument.get('symbol')
            quote_coin = instrument.get('quoteCoin')
            status = instrument.get('status')
            if symbol and quote_coin == 'USDT' and status in (None, 'Trading'):
                symbols.add(symbol)
        cursor = result.get('nextPageCursor')
        if not cursor:
            break

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
        logger.warning(f"Bybit supported symbol cache unavailable, fallback to request path: {exc}")
        return True


def fetch_klines(symbol, period, limit, session=None, start_time=None, end_time=None):
    params = {
        'category': BYBIT_CATEGORY,
        'symbol': symbol,
        'interval': _bybit_interval(period),
        'limit': str(min(int(limit), 1000)),
    }
    if start_time is not None:
        params['start'] = str(start_time)
    if end_time is not None:
        params['end'] = str(end_time)
    return _request_bybit('/v5/market/kline', params, session=session)


def fetch_open_interest_hist(symbol, period, limit, session=None, start_time=None, end_time=None):
    params = {
        'category': BYBIT_CATEGORY,
        'symbol': symbol,
        'intervalTime': _bybit_open_interest_interval(period),
        'limit': str(min(int(limit), 200)),
    }
    if start_time is not None:
        params['startTime'] = str(start_time)
    if end_time is not None:
        params['endTime'] = str(end_time)
    return _request_bybit('/v5/market/open-interest', params, session=session)


def get_funding_rate(symbol, session=None):
    payload = _request_bybit(
        '/v5/market/tickers',
        {
            'category': BYBIT_CATEGORY,
            'symbol': symbol,
        },
        session=session,
    )
    rows = payload.get('list', payload) if isinstance(payload, dict) else payload
    row = rows[0] if isinstance(rows, list) and rows else None
    if not isinstance(row, dict):
        return None
    return {
        'exchange': BYBIT_EXCHANGE_ID,
        'symbol': symbol,
        'fundingRate': _to_float(row.get('fundingRate')),
        'nextFundingTime': int(row['nextFundingTime']) if row.get('nextFundingTime') else None,
    }


def get_all_funding_rates(session=None):
    logger.info('开始加载全量资金费率: exchange=bybit')
    payload = _request_bybit(
        '/v5/market/tickers',
        {
            'category': BYBIT_CATEGORY,
        },
        session=session,
    )
    rows = payload.get('list', payload) if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        return {}

    result = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = row.get('symbol')
        if not symbol:
            continue
        result[symbol] = {
            'exchange': BYBIT_EXCHANGE_ID,
            'symbol': symbol,
            'fundingRate': _to_float(row.get('fundingRate')),
            'nextFundingTime': int(row['nextFundingTime']) if row.get('nextFundingTime') else None,
        }
    logger.info('全量资金费率加载完成: exchange=bybit count=%d', len(result))
    return result


def parse_klines(payload, symbol, period):
    rows = payload.get('list', payload) if isinstance(payload, dict) else payload
    parsed = []
    for item in rows:
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
                'quote_volume': _to_float(item[6]) if len(item) > 6 else None,
                'trade_count': None,
                'taker_buy_base_volume': None,
                'taker_buy_quote_volume': None,
                'raw_json': item,
            }
        )
    return parsed


def parse_open_interest_hist(payload, symbol, period):
    rows = payload.get('list', payload) if isinstance(payload, dict) else payload
    parsed = []
    for item in rows:
        parsed.append(
            {
                'symbol': symbol,
                'period': period,
                'event_time': int(item['timestamp']),
                'sum_open_interest': _to_float(item.get('openInterest')),
                'sum_open_interest_value': None,
                'raw_json': item,
            }
        )
    return parsed


def fetch_series_payload(series_type, symbol, period, limit, session=None, start_time=None, end_time=None):
    fetchers = {
        'klines': fetch_klines,
        'open_interest_hist': fetch_open_interest_hist,
    }
    try:
        fetcher = fetchers[series_type]
    except KeyError as exc:
        raise ValueError(f"unsupported Bybit series type: {series_type}") from exc
    return fetcher(symbol, period, limit, session=session, start_time=start_time, end_time=end_time)


def parse_series_payload(series_type, payload, symbol, period):
    parsers = {
        'klines': parse_klines,
        'open_interest_hist': parse_open_interest_hist,
    }
    try:
        parser = parsers[series_type]
    except KeyError as exc:
        raise ValueError(f"unsupported Bybit series type: {series_type}") from exc
    return parser(payload, symbol, period)


def collect_and_store_series(series_type, symbol, period, limit, http_session=None, db_session=None):
    logger.info(f"start collecting Bybit series: type={series_type}, symbol={symbol}, period={period}, limit={limit}")
    payload = fetch_series_payload(series_type, symbol, period, limit, session=http_session)
    records = parse_series_payload(series_type, payload, symbol, period)
    affected = upsert_series_records(BYBIT_EXCHANGE_ID, series_type, records, session=db_session)
    logger.info(f"Bybit series collected: type={series_type}, symbol={symbol}, affected={affected}")
    return {
        'exchange': BYBIT_EXCHANGE_ID,
        'series_type': series_type,
        'symbol': symbol,
        'period': period,
        'limit': limit,
        'affected': affected,
        'records': records,
    }
