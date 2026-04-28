import time

from coinx.config import BINANCE_BASE_URL
from coinx.utils import logger
from coinx.collector.binance.client import get_session, request_with_retry
from coinx.repositories.binance_series import upsert_series_records


SERIES_ENDPOINTS = {
    'top_long_short_position_ratio': '/futures/data/topLongShortPositionRatio',
    'top_long_short_account_ratio': '/futures/data/topLongShortAccountRatio',
    'open_interest_hist': '/futures/data/openInterestHist',
    'klines': '/fapi/v1/klines',
    'global_long_short_account_ratio': '/futures/data/globalLongShortAccountRatio',
    'taker_buy_sell_vol': '/futures/data/takerlongshortRatio',
}

DEFAULT_SERIES_TYPES = list(SERIES_ENDPOINTS.keys())


def _to_float(value):
    if value is None or value == '':
        return None
    return float(value)


def _request_series(path, params, session=None, timeout=10):
    http_session = session or get_session()
    url = f"{BINANCE_BASE_URL}{path}"
    response = request_with_retry(http_session, url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def fetch_top_long_short_position_ratio(symbol, period, limit, session=None, start_time=None, end_time=None):
    params = {'symbol': symbol, 'period': period, 'limit': limit}
    if start_time is not None:
        params['startTime'] = start_time
    if end_time is not None:
        params['endTime'] = end_time
    return _request_series(
        SERIES_ENDPOINTS['top_long_short_position_ratio'],
        params,
        session=session,
    )


def fetch_top_long_short_account_ratio(symbol, period, limit, session=None, start_time=None, end_time=None):
    params = {'symbol': symbol, 'period': period, 'limit': limit}
    if start_time is not None:
        params['startTime'] = start_time
    if end_time is not None:
        params['endTime'] = end_time
    return _request_series(
        SERIES_ENDPOINTS['top_long_short_account_ratio'],
        params,
        session=session,
    )


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


def fetch_global_long_short_account_ratio(symbol, period, limit, session=None, start_time=None, end_time=None):
    params = {'symbol': symbol, 'period': period, 'limit': limit}
    if start_time is not None:
        params['startTime'] = start_time
    if end_time is not None:
        params['endTime'] = end_time
    return _request_series(
        SERIES_ENDPOINTS['global_long_short_account_ratio'],
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


def parse_top_long_short_position_ratio(payload, symbol, period):
    return [
        {
            'symbol': item.get('symbol', symbol),
            'period': period,
            'event_time': int(item['timestamp']),
            'long_short_ratio': _to_float(item.get('longShortRatio')),
            'long_account': _to_float(item.get('longAccount')),
            'short_account': _to_float(item.get('shortAccount')),
        }
        for item in payload
    ]


def parse_top_long_short_account_ratio(payload, symbol, period):
    return [
        {
            'symbol': item.get('symbol', symbol),
            'period': period,
            'event_time': int(item['timestamp']),
            'long_short_ratio': _to_float(item.get('longShortRatio')),
            'long_account': _to_float(item.get('longAccount')),
            'short_account': _to_float(item.get('shortAccount')),
        }
        for item in payload
    ]


def parse_open_interest_hist(payload, symbol, period):
    return [
        {
            'symbol': item.get('symbol', symbol),
            'period': period,
            'event_time': int(item['timestamp']),
            'sum_open_interest': _to_float(item.get('sumOpenInterest')),
            'sum_open_interest_value': _to_float(item.get('sumOpenInterestValue')),
            'cmc_circulating_supply': _to_float(item.get('CMCCirculatingSupply')),
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


def parse_global_long_short_account_ratio(payload, symbol, period):
    return [
        {
            'symbol': item.get('symbol', symbol),
            'period': period,
            'event_time': int(item['timestamp']),
            'long_short_ratio': _to_float(item.get('longShortRatio')),
            'long_account': _to_float(item.get('longAccount')),
            'short_account': _to_float(item.get('shortAccount')),
        }
        for item in payload
    ]


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
    fetchers = {
        'top_long_short_position_ratio': fetch_top_long_short_position_ratio,
        'top_long_short_account_ratio': fetch_top_long_short_account_ratio,
        'open_interest_hist': fetch_open_interest_hist,
        'klines': fetch_klines,
        'global_long_short_account_ratio': fetch_global_long_short_account_ratio,
        'taker_buy_sell_vol': fetch_taker_buy_sell_vol,
    }

    try:
        fetcher = fetchers[series_type]
    except KeyError as exc:
        raise ValueError(f"涓嶆敮鎸佺殑搴忓垪绫诲瀷: {series_type}") from exc

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
        'top_long_short_position_ratio': parse_top_long_short_position_ratio,
        'top_long_short_account_ratio': parse_top_long_short_account_ratio,
        'open_interest_hist': parse_open_interest_hist,
        'klines': parse_klines,
        'global_long_short_account_ratio': parse_global_long_short_account_ratio,
        'taker_buy_sell_vol': parse_taker_buy_sell_vol,
    }

    try:
        parser = parsers[series_type]
    except KeyError as exc:
        raise ValueError(f"涓嶆敮鎸佺殑搴忓垪绫诲瀷: {series_type}") from exc

    return parser(payload, symbol, period)


def collect_and_store_series(series_type, symbol, period, limit, http_session=None, db_session=None, now_ms=None):
    logger.info(f"寮€濮嬮噰闆嗗簭鍒楁暟鎹? type={series_type}, symbol={symbol}, period={period}, limit={limit}")
    payload = fetch_series_payload(series_type, symbol, period, limit, session=http_session)
    records = parse_series_payload(series_type, payload, symbol, period)
    if now_ms is None:
        now_ms = int(time.time() * 1000)

    from coinx.collector.binance.repair import trim_unclosed_series_records

    records = trim_unclosed_series_records(
        series_type=series_type,
        records=records,
        now_ms=now_ms,
        period=period,
    )
    affected = upsert_series_records(series_type, records, session=db_session)
    logger.info(f"搴忓垪鏁版嵁閲囬泦瀹屾垚: type={series_type}, symbol={symbol}, affected={affected}")
    return {
        'series_type': series_type,
        'symbol': symbol,
        'period': period,
        'limit': limit,
        'affected': affected,
        'records': records,
    }


def collect_series_batch(symbols, periods, series_types=None, limit=30, http_session=None, db_session=None):
    active_series_types = series_types or DEFAULT_SERIES_TYPES
    results = []

    for symbol in symbols:
        for period in periods:
            for series_type in active_series_types:
                try:
                    result = collect_and_store_series(
                        series_type=series_type,
                        symbol=symbol,
                        period=period,
                        limit=limit,
                        http_session=http_session,
                        db_session=db_session,
                    )
                    result['status'] = 'success'
                    results.append(result)
                except Exception as exc:
                    logger.error(
                        f"鎵归噺閲囬泦澶辫触: type={series_type}, symbol={symbol}, period={period}, error={exc}"
                    )
                    results.append(
                        {
                            'series_type': series_type,
                            'symbol': symbol,
                            'period': period,
                            'limit': limit,
                            'status': 'error',
                            'error': str(exc),
                        }
                    )

    success_count = sum(1 for item in results if item.get('status') == 'success')
    failure_count = len(results) - success_count
    return {
        'symbols': symbols,
        'periods': periods,
        'series_types': active_series_types,
        'limit': limit,
        'success_count': success_count,
        'failure_count': failure_count,
        'results': results,
    }
