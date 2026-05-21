import time
import requests

from coinx.collector.binance.client import get_session, request_with_retry
from coinx.collector.rate_limit import RateLimitRegistry, RateLimitUnavailable, record_rate_limit_wait_seconds
from coinx.config import (
    GATE_403_RETRY_FALLBACK_SECONDS,
    GATE_BASE_URL,
    GATE_MIN_INTERVAL_MS,
    GATE_SETTLE,
)
from coinx.repositories.series import upsert_series_records
from coinx.utils import logger


GATE_EXCHANGE_ID = 'gate'
SUPPORTED_SERIES_TYPES = ('klines', 'open_interest_hist')
_SUPPORTED_SYMBOLS_TTL_SECONDS = 60 * 60
_supported_symbols_cache = {
    'loaded_at': 0,
    'failed_at': 0,
    'fallback_logged': False,
    'symbols': None,
    'unsupported_symbols': set(),
}
_gate_rate_limits = RateLimitRegistry()
_GATE_REQUEST_HEADERS = {
    'User-Agent': 'curl/8.5.0',
    'Accept': '*/*',
    'Accept-Language': None,
}


class GateRateLimitUnavailable(RateLimitUnavailable):
    """Raised when Gate does not expose usable rate-limit headers for this host/api window."""

    def __init__(self, wait_seconds, reason='budget_unavailable'):
        super().__init__('gate', 'default', wait_seconds, reason=reason)


class GateUnsupportedContract(RuntimeError):
    """Raised when Gate reports that a futures contract does not exist."""


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
    raise ValueError(f'unsupported Gate period: {period}')


def _gate_interval(period):
    if period.endswith('m'):
        return f'{int(period[:-1])}m'
    if period.endswith('h'):
        return f'{int(period[:-1])}h'
    if period.endswith('d'):
        return f'{int(period[:-1])}d'
    raise ValueError(f'unsupported Gate interval: {period}')


def to_exchange_symbol(symbol):
    if '_' in symbol:
        return symbol
    if symbol.endswith('USDT'):
        return f'{symbol[:-4]}_USDT'
    return symbol


def to_internal_symbol(contract):
    return str(contract or '').replace('_', '')


def _safe_int(value):
    if value in (None, ''):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _interesting_gate_headers(response):
    if response is None:
        return {}
    headers = {}
    for key, value in response.headers.items():
        lower_key = key.lower()
        if (
            lower_key.startswith('x-gate-')
            or 'ratelimit' in lower_key
            or 'retry' in lower_key
            or 'cf-' in lower_key
            or 'cloudflare' in lower_key
            or lower_key == 'server'
            or lower_key == 'content-type'
        ):
            headers[key] = value
    return headers


def _gate_response_body_snippet(response, limit=600):
    if response is None:
        return ''
    try:
        text = response.text or ''
    except Exception:
        return ''
    text = ' '.join(str(text).split())
    if len(text) <= limit:
        return text
    return text[:limit] + '...'


def _gate_response_label(response):
    if response is None:
        return None
    try:
        payload = response.json()
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    return payload.get('label')


def _mark_gate_contract_unsupported(contract):
    symbol = to_internal_symbol(contract)
    if not symbol:
        return
    _supported_symbols_cache.setdefault('unsupported_symbols', set()).add(symbol)


def is_gate_symbol_unsupported(symbol):
    return symbol in (_supported_symbols_cache.get('unsupported_symbols') or set())


def _gate_budget_unavailable_remaining_seconds():
    return _gate_rate_limits.unavailable_remaining_seconds('gate', 'default')


def is_gate_budget_unavailable():
    return _gate_budget_unavailable_remaining_seconds() > 0


def _wait_for_gate_slot():
    _gate_rate_limits.wait_for_slot('gate', 'default', min_interval_ms=GATE_MIN_INTERVAL_MS, consume_budget=True)


def _update_gate_rate_limit_state(response):
    if response is None:
        return

    headers = response.headers or {}
    remain = _safe_int(headers.get('x-gate-ratelimit-requests-remain') or headers.get('X-Gate-Ratelimit-Requests-Remain'))
    reset_at = _safe_int(headers.get('x-gate-ratelimit-reset-timestamp') or headers.get('X-Gate-Ratelimit-Reset-Timestamp'))
    limit = _safe_int(headers.get('x-gate-ratelimit-limit') or headers.get('X-Gate-Ratelimit-Limit'))

    now = time.time()
    next_allowed_at = None

    if reset_at is not None and remain is not None and remain <= 0:
        next_allowed_at = max(now, float(reset_at))
    elif remain is not None and remain <= 0:
        next_allowed_at = now + max(float(GATE_403_RETRY_FALLBACK_SECONDS), 1.0)
    elif GATE_MIN_INTERVAL_MS and int(GATE_MIN_INTERVAL_MS) > 0:
        next_allowed_at = now + (int(GATE_MIN_INTERVAL_MS) / 1000.0)

    _gate_rate_limits.update_budget(
        'gate',
        'default',
        limit=limit,
        remain=remain,
        reset_at=reset_at,
        next_allowed_at=next_allowed_at,
        headers={
            'limit': limit,
            'remain': remain,
            'reset_at': reset_at,
        },
    )


def _has_gate_rate_limit_headers(response):
    if response is None:
        return False
    headers = response.headers or {}
    return (
        headers.get('x-gate-ratelimit-requests-remain')
        or headers.get('X-Gate-Ratelimit-Requests-Remain')
        or headers.get('x-gate-ratelimit-reset-timestamp')
        or headers.get('X-Gate-Ratelimit-Reset-Timestamp')
    ) is not None


def _mark_gate_budget_unavailable():
    _gate_rate_limits.mark_cooldown(
        'gate',
        'default',
        float(GATE_403_RETRY_FALLBACK_SECONDS),
        budget_unavailable=True,
    )


def _request_gate(path, params, session=None, timeout=10):
    http_session = session or get_session()
    url = f"{GATE_BASE_URL}{path}"
    unavailable_seconds = _gate_budget_unavailable_remaining_seconds()
    if unavailable_seconds > 0:
        logger.warning(
            'Gate host/api budget unavailable, skip request until cooldown ends: path=%s params=%s wait=%.2fs',
            path,
            params,
            unavailable_seconds,
        )
        raise GateRateLimitUnavailable(unavailable_seconds)

    _wait_for_gate_slot()
    try:
        response = request_with_retry(
            http_session,
            url,
            params=params,
            timeout=timeout,
            max_retries=0,
            headers=_GATE_REQUEST_HEADERS,
        )
        _update_gate_rate_limit_state(response)
    except requests.exceptions.HTTPError as exc:
        response = getattr(exc, 'response', None)
        _update_gate_rate_limit_state(response)
        if _gate_response_label(response) == 'CONTRACT_NOT_FOUND':
            contract = params.get('contract') if isinstance(params, dict) else None
            _mark_gate_contract_unsupported(contract)
            logger.warning(
                'Gate contract not found, mark symbol unsupported: path=%s params=%s body=%s',
                path,
                params,
                _gate_response_body_snippet(response),
            )
            raise GateUnsupportedContract(f'gate contract not found: {contract}') from exc
        if response is None or response.status_code != 403:
            raise

        headers_snapshot = _gate_rate_limits.get_state_snapshot('gate', 'default').last_headers or {}
        reset_at = headers_snapshot.get('reset_at')
        remain = headers_snapshot.get('remain')
        now = time.time()
        backoff_seconds = float(GATE_403_RETRY_FALLBACK_SECONDS)

        if reset_at is not None and remain is not None and remain <= 0:
            backoff_seconds = max(backoff_seconds, max(0.0, float(reset_at) - now))
            _gate_rate_limits.update_budget('gate', 'default', next_allowed_at=float(reset_at))

        logger.warning(
            'Gate 403 response details: path=%s params=%s headers=%s body=%s',
            path,
            params,
            _interesting_gate_headers(response),
            _gate_response_body_snippet(response),
        )
        logger.warning(
            'Gate request rejected, evaluate cooldown from response headers: path=%s params=%s status=%s wait=%.2fs headers=%s',
            path,
            params,
            response.status_code,
            backoff_seconds,
            headers_snapshot,
        )

        if remain is None and reset_at is None:
            logger.warning(
                'Gate response missing remain/reset headers, mark host/api budget unavailable and stop retrying: path=%s params=%s',
                path,
                params,
            )
            _mark_gate_budget_unavailable()
            raise GateRateLimitUnavailable(float(GATE_403_RETRY_FALLBACK_SECONDS)) from exc

        record_rate_limit_wait_seconds(backoff_seconds)
        time.sleep(backoff_seconds)
        _wait_for_gate_slot()
        response = request_with_retry(
            http_session,
            url,
            params=params,
            timeout=timeout,
            max_retries=1,
            base_delay=1.0,
            headers=_GATE_REQUEST_HEADERS,
        )
        _update_gate_rate_limit_state(response)

    response.raise_for_status()
    return response.json()


def clear_supported_symbols_cache():
    _supported_symbols_cache['loaded_at'] = 0
    _supported_symbols_cache['failed_at'] = 0
    _supported_symbols_cache['fallback_logged'] = False
    _supported_symbols_cache['symbols'] = None
    _supported_symbols_cache['unsupported_symbols'] = set()


def clear_gate_rate_limit_state():
    _gate_rate_limits.clear()


def get_supported_symbols(session=None, ttl_seconds=_SUPPORTED_SYMBOLS_TTL_SECONDS):
    now = time.time()
    cached_symbols = _supported_symbols_cache.get('symbols')
    loaded_at = _supported_symbols_cache.get('loaded_at') or 0
    if cached_symbols is not None and now - loaded_at < ttl_seconds:
        return cached_symbols

    rows = _request_gate(f'/api/v4/futures/{GATE_SETTLE}/contracts', {}, session=session)
    symbols = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get('in_delisting'):
            continue
        contract = row.get('name') or row.get('contract')
        if not contract or not str(contract).endswith('_USDT'):
            continue
        symbols.add(to_internal_symbol(contract))

    _supported_symbols_cache['symbols'] = symbols
    _supported_symbols_cache['loaded_at'] = now
    return symbols


def is_symbol_supported(symbol, series_type=None, session=None):
    if series_type not in (None, *SUPPORTED_SERIES_TYPES):
        return False
    if is_gate_symbol_unsupported(symbol):
        return False
    cached_symbols = _supported_symbols_cache.get('symbols')
    if cached_symbols is None:
        cached_symbols = get_supported_symbols(session=session)
        if not _supported_symbols_cache.get('fallback_logged'):
            logger.info('Gate supported symbol cache loaded from /contracts during precheck')
            _supported_symbols_cache['fallback_logged'] = True
    if cached_symbols is None:
        return False
    return symbol in cached_symbols


def fetch_klines(symbol, period, limit, session=None, start_time=None, end_time=None):
    params = {
        'contract': to_exchange_symbol(symbol),
        'interval': _gate_interval(period),
    }
    if start_time is not None or end_time is not None:
        params['from'] = str(int(start_time) // 1000)
        params['to'] = str(int(end_time) // 1000)
    else:
        params['limit'] = str(min(int(limit), 1000))
    return _request_gate(f'/api/v4/futures/{GATE_SETTLE}/candlesticks', params, session=session)


def fetch_open_interest_hist(symbol, period, limit, session=None, start_time=None, end_time=None):
    params = {
        'contract': to_exchange_symbol(symbol),
        'interval': _gate_interval(period),
    }
    if start_time is not None or end_time is not None:
        params['from'] = str(int(start_time) // 1000)
        params['to'] = str(int(end_time) // 1000)
    else:
        params['limit'] = str(min(int(limit), 1000))
    return _request_gate(f'/api/v4/futures/{GATE_SETTLE}/contract_stats', params, session=session)


def get_funding_rate(symbol, session=None):
    payload = _request_gate(
        f'/api/v4/futures/{GATE_SETTLE}/funding_rate',
        {'contract': to_exchange_symbol(symbol)},
        session=session,
    )
    if not isinstance(payload, dict):
        return None
    return {
        'exchange': GATE_EXCHANGE_ID,
        'symbol': symbol,
        'fundingRate': _to_float(payload.get('r') or payload.get('funding_rate') or payload.get('fundingRate')),
        'fundingTime': int(float(payload['t']) * 1000) if payload.get('t') not in (None, '') else None,
    }


def get_all_funding_rates(session=None):
    logger.info('开始加载全量资金费率: exchange=gate')
    rows = _request_gate(f'/api/v4/futures/{GATE_SETTLE}/tickers', {}, session=session)
    result = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        contract = row.get('contract')
        if not contract:
            continue
        symbol = to_internal_symbol(contract)
        result[symbol] = {
            'exchange': GATE_EXCHANGE_ID,
            'symbol': symbol,
            'fundingRate': _to_float(row.get('funding_rate') or row.get('fundingRate')),
            'fundingTime': int(float(row['funding_next_apply']) * 1000) if row.get('funding_next_apply') not in (None, '') else None,
        }
    logger.info('全量资金费率加载完成: exchange=gate count=%d', len(result))
    return result


def parse_klines(payload, symbol, period):
    parsed = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        open_time_seconds = item.get('t') or item.get('time')
        if open_time_seconds in (None, ''):
            continue
        open_time = int(float(open_time_seconds) * 1000)
        parsed.append(
            {
                'symbol': symbol,
                'period': period,
                'open_time': open_time,
                'close_time': open_time + _period_to_ms(period) - 1,
                'open_price': _to_float(item.get('o') or item.get('open')),
                'high_price': _to_float(item.get('h') or item.get('high')),
                'low_price': _to_float(item.get('l') or item.get('low')),
                'close_price': _to_float(item.get('c') or item.get('close')),
                'volume': _to_float(item.get('v') or item.get('volume')),
                'quote_volume': _to_float(item.get('sum') or item.get('quote_volume') or item.get('amount')),
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
        if not isinstance(item, dict):
            continue
        event_time_seconds = item.get('t') or item.get('time')
        if event_time_seconds in (None, ''):
            continue
        parsed.append(
            {
                'symbol': symbol,
                'period': period,
                'event_time': int(float(event_time_seconds) * 1000),
                'sum_open_interest': _to_float(item.get('open_interest')),
                'sum_open_interest_value': _to_float(item.get('open_interest_usd')),
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
        raise ValueError(f"unsupported Gate series type: {series_type}") from exc
    return fetcher(symbol, period, limit, session=session, start_time=start_time, end_time=end_time)


def parse_series_payload(series_type, payload, symbol, period):
    parsers = {
        'klines': parse_klines,
        'open_interest_hist': parse_open_interest_hist,
    }
    try:
        parser = parsers[series_type]
    except KeyError as exc:
        raise ValueError(f"unsupported Gate series type: {series_type}") from exc
    return parser(payload, symbol, period)


def collect_and_store_series(series_type, symbol, period, limit, http_session=None, db_session=None):
    logger.info(f"start collecting Gate series: type={series_type}, symbol={symbol}, period={period}, limit={limit}")
    payload = fetch_series_payload(series_type, symbol, period, limit, session=http_session)
    records = parse_series_payload(series_type, payload, symbol, period)
    affected = upsert_series_records(GATE_EXCHANGE_ID, series_type, records, session=db_session)
    logger.info(f"Gate series collected: type={series_type}, symbol={symbol}, affected={affected}")
    return {
        'exchange': GATE_EXCHANGE_ID,
        'series_type': series_type,
        'symbol': symbol,
        'period': period,
        'limit': limit,
        'affected': affected,
        'records': records,
    }
