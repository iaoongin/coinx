import requests

from coinx.collector.okx import series as okx_series
from coinx.collector.rate_limit import RateLimitRegistry
from coinx.collector.okx.series import (
    OKXRateLimitUnavailable,
    clear_supported_symbols_cache,
    clear_okx_rate_limit_state,
    fetch_open_interest_hist,
    fetch_taker_buy_sell_vol,
    get_supported_symbols,
    is_symbol_supported,
    parse_klines,
    parse_open_interest_hist,
    parse_taker_buy_sell_vol,
    to_exchange_symbol,
    to_internal_symbol,
)
from coinx.collector.exchange_adapters import get_exchange_adapter


def test_okx_symbol_mapping_uses_usdt_swap_contracts():
    assert to_exchange_symbol('BTCUSDT') == 'BTC-USDT-SWAP'
    assert to_internal_symbol('BTC-USDT-SWAP') == 'BTCUSDT'


def test_parse_okx_klines_maps_candle_array():
    payload = [
        [
            '1711526400000',
            '68000.10',
            '68100.20',
            '67950.30',
            '68020.40',
            '123.45',
            '12.34',
            '8398765.43',
            '1',
        ]
    ]

    records = parse_klines(payload, symbol='BTCUSDT', period='5m')

    assert records[0]['symbol'] == 'BTCUSDT'
    assert records[0]['open_time'] == 1711526400000
    assert records[0]['close_time'] == 1711526699999
    assert records[0]['close_price'] == 68020.40
    assert records[0]['quote_volume'] == 8398765.43


def test_parse_okx_open_interest_accepts_dict_payload():
    payload = [
        {
            'ts': '1711526400000',
            'oi': '12345.67',
            'oiUsd': '987654.32',
        }
    ]

    records = parse_open_interest_hist(payload, symbol='BTCUSDT', period='5m')

    assert records[0]['event_time'] == 1711526400000
    assert records[0]['sum_open_interest'] == 12345.67
    assert records[0]['sum_open_interest_value'] == 987654.32


def test_parse_okx_open_interest_array_treats_value_as_open_interest_value():
    payload = [['1711526400000', '987654.32', '12345.67']]

    records = parse_open_interest_hist(payload, symbol='BTCUSDT', period='5m')

    assert records[0]['event_time'] == 1711526400000
    assert records[0]['sum_open_interest'] is None
    assert records[0]['sum_open_interest_value'] == 987654.32


def test_parse_okx_taker_buy_sell_vol_calculates_ratio():
    payload = [['1711526400000', '150.0', '100.0']]

    records = parse_taker_buy_sell_vol(payload, symbol='BTCUSDT', period='5m')

    assert records[0]['event_time'] == 1711526400000
    assert records[0]['buy_vol'] == 100.0
    assert records[0]['sell_vol'] == 150.0
    assert round(records[0]['buy_sell_ratio'], 4) == round(100.0 / 150.0, 4)


def test_okx_rubik_fetchers_pass_begin_and_end(monkeypatch):
    calls = []

    def fake_request(path, params, session=None):
        calls.append((path, params))
        return []

    monkeypatch.setattr('coinx.collector.okx.series._request_okx', fake_request)
    monkeypatch.setattr('coinx.collector.okx.series.time.time', lambda: 10)

    fetch_open_interest_hist('BTCUSDT', '5m', 500, start_time=1000, end_time=2000)
    fetch_taker_buy_sell_vol('BTCUSDT', '5m', 500, start_time=1000, end_time=2000)

    assert calls[0] == (
        '/api/v5/rubik/stat/contracts/open-interest-history',
        {
            'instId': 'BTC-USDT-SWAP',
            'period': '5m',
            'begin': '1000',
            'end': '2000',
        },
    )
    assert calls[1] == (
        '/api/v5/rubik/stat/taker-volume-contract',
        {
            'instId': 'BTC-USDT-SWAP',
            'period': '5m',
            'begin': '1000',
            'end': '2000',
        },
    )


def test_okx_fetch_klines_uses_after_for_start_and_before_for_end(monkeypatch):
    calls = []

    def fake_request(path, params, session=None):
        calls.append((path, params))
        return []

    monkeypatch.setattr('coinx.collector.okx.series._request_okx', fake_request)

    okx_series.fetch_klines('BTCUSDT', '5m', 500, start_time=1000, end_time=2000)

    assert calls == [
        (
            '/api/v5/market/history-candles',
            {
                'instId': 'BTC-USDT-SWAP',
                'bar': '5m',
                'limit': '300',
                'before': '1000',
                'after': '2000',
            },
        )
    ]


def test_okx_rubik_5m_window_keeps_168h_requests(monkeypatch):
    calls = []

    def fake_request(path, params, session=None):
        calls.append((path, params))
        return []

    monkeypatch.setattr('coinx.collector.okx.series._request_okx', fake_request)
    monkeypatch.setattr('coinx.collector.okx.series.time.time', lambda: 8 * 24 * 60 * 60)

    start_time = 24 * 60 * 60 * 1000
    end_time = 8 * 24 * 60 * 60 * 1000
    fetch_open_interest_hist('BTCUSDT', '5m', 500, start_time=start_time, end_time=end_time)

    assert calls == [
        (
            '/api/v5/rubik/stat/contracts/open-interest-history',
            {
                'instId': 'BTC-USDT-SWAP',
                'period': '5m',
                'begin': str(start_time),
                'end': str(end_time),
            },
        )
    ]


def test_okx_open_interest_history_uses_inst_id_and_returns_last_page_within_window(monkeypatch):
    calls = []

    def fake_request(path, params, session=None):
        calls.append((path, params))
        return [
            ['1779556200000', '3500484.79', '35004.8479', '2641056265.81'],
            ['1779526500000', '3480898.71', '34808.9871', '2598080140.96'],
        ]

    monkeypatch.setattr('coinx.collector.okx.series._request_okx', fake_request)
    monkeypatch.setattr('coinx.collector.okx.series.time.time', lambda: 1779556200000 / 1000)

    payload = fetch_open_interest_hist(
        'BTCUSDT',
        '5m',
        500,
        start_time=1778951400000,
        end_time=1779556200000,
    )

    assert calls == [
        (
            '/api/v5/rubik/stat/contracts/open-interest-history',
            {
                'instId': 'BTC-USDT-SWAP',
                'period': '5m',
                'begin': '1778951400000',
                'end': '1779556200000',
            },
        )
    ]
    assert payload[0][0] == '1779556200000'
    assert payload[-1][0] == '1779526500000'


def test_okx_rubik_fetchers_keep_old_windows(monkeypatch):
    calls = []

    def fake_request(path, params, session=None):
        calls.append((path, params))
        return []

    monkeypatch.setattr('coinx.collector.okx.series._request_okx', fake_request)
    monkeypatch.setattr('coinx.collector.okx.series.time.time', lambda: 3 * 24 * 60 * 60)

    assert fetch_open_interest_hist('BTCUSDT', '5m', 500, start_time=0, end_time=1000) == []
    assert fetch_taker_buy_sell_vol('BTCUSDT', '5m', 500, start_time=0, end_time=1000) == []
    assert calls == [
        (
            '/api/v5/rubik/stat/contracts/open-interest-history',
            {
                'instId': 'BTC-USDT-SWAP',
                'period': '5m',
                'begin': '0',
                'end': '1000',
            },
        ),
        (
            '/api/v5/rubik/stat/taker-volume-contract',
            {
                'instId': 'BTC-USDT-SWAP',
                'period': '5m',
                'begin': '0',
                'end': '1000',
            },
        ),
    ]


def test_okx_rubik_fetchers_keep_requested_windows(monkeypatch):
    calls = []

    def fake_request(path, params, session=None):
        calls.append((path, params))
        return []

    monkeypatch.setattr('coinx.collector.okx.series._request_okx', fake_request)
    monkeypatch.setattr('coinx.collector.okx.series.time.time', lambda: 40 * 24 * 60 * 60)

    end_time = 40 * 24 * 60 * 60 * 1000
    fetch_open_interest_hist('BTCUSDT', '1H', 500, start_time=0, end_time=end_time)

    assert calls[0][1]['begin'] == '0'
    assert calls[0][1]['end'] == str(end_time)


def test_okx_adapter_marks_homepage_series_as_precise_windows():
    adapter = get_exchange_adapter('okx')

    assert adapter.supports_time_window('klines') is True
    assert adapter.supports_time_window('open_interest_hist') is True
    assert adapter.supports_time_window('taker_buy_sell_vol') is True
    assert adapter.periods_for_series('taker_buy_sell_vol') == ('5m', '1H')
    assert adapter.taker_period_for_interval('24h') == '5m'
    assert adapter.taker_period_for_interval('48h') == '1H'
    assert adapter.taker_period_for_interval('168h') == '1H'


def test_okx_supported_symbols_maps_live_usdt_swaps(monkeypatch):
    clear_supported_symbols_cache()

    monkeypatch.setattr(
        'coinx.collector.okx.series._request_okx',
        lambda path, params, session=None: [
            {'instId': 'BTC-USDT-SWAP', 'state': 'live'},
            {'instId': 'ETH-USDT-SWAP', 'state': 'live'},
            {'instId': 'BTC-USD-SWAP', 'state': 'live'},
            {'instId': 'OLD-USDT-SWAP', 'state': 'suspend'},
        ],
    )

    assert get_supported_symbols(ttl_seconds=0) == {'BTCUSDT', 'ETHUSDT'}


def test_okx_is_symbol_supported_false_for_missing_swap(monkeypatch):
    clear_supported_symbols_cache()

    monkeypatch.setattr(
        'coinx.collector.okx.series._request_okx',
        lambda path, params, session=None: [{'instId': 'BTC-USDT-SWAP', 'state': 'live'}],
    )

    assert is_symbol_supported('BTCUSDT') is True
    assert is_symbol_supported('PROMUSDT') is False


def test_okx_request_uses_retry_after_header_on_429(monkeypatch):
    clear_okx_rate_limit_state()

    class FakeResponse:
        status_code = 429
        reason = 'Too Many Requests'
        headers = {'Retry-After': '7'}

    def fake_request(session, url, params=None, timeout=10, max_retries=3, base_delay=0.5):
        error = requests.exceptions.HTTPError('429 Too Many Requests')
        error.response = FakeResponse()
        raise error

    monkeypatch.setattr(okx_series, 'request_with_retry', fake_request)
    monkeypatch.setattr(okx_series, 'OKX_RUBIK_MIN_INTERVAL_MS', 0)
    monkeypatch.setattr(okx_series, 'choose_okx_proxy_id', lambda: 'proxy-test')

    try:
        okx_series._request_okx('/api/v5/rubik/stat/taker-volume', {'ccy': 'BTC'}, proxy_id='proxy-test')
    except requests.exceptions.HTTPError:
        pass

    try:
        okx_series._request_okx('/api/v5/rubik/stat/taker-volume', {'ccy': 'BTC'}, proxy_id='proxy-test')
        assert False, 'expected OKXRateLimitUnavailable'
    except OKXRateLimitUnavailable as exc:
        assert round(exc.wait_seconds, 1) <= 7.0
        assert round(exc.wait_seconds, 1) > 0


def test_okx_request_uses_fallback_backoff_without_retry_header(monkeypatch):
    clear_okx_rate_limit_state()

    class FakeResponse:
        status_code = 429
        reason = 'Too Many Requests'
        headers = {}

    def fake_request(session, url, params=None, timeout=10, max_retries=3, base_delay=0.5):
        error = requests.exceptions.HTTPError('429 Too Many Requests')
        error.response = FakeResponse()
        raise error

    monkeypatch.setattr(okx_series, 'request_with_retry', fake_request)
    monkeypatch.setattr(okx_series, 'OKX_RUBIK_MIN_INTERVAL_MS', 0)
    monkeypatch.setattr(okx_series, 'OKX_429_RETRY_FALLBACK_SECONDS', 9)
    monkeypatch.setattr(okx_series, 'choose_okx_proxy_id', lambda: 'proxy-test')

    try:
        okx_series._request_okx('/api/v5/rubik/stat/taker-volume', {'ccy': 'BTC'}, proxy_id='proxy-test')
    except requests.exceptions.HTTPError:
        pass

    try:
        okx_series._request_okx('/api/v5/rubik/stat/taker-volume', {'ccy': 'BTC'}, proxy_id='proxy-test')
        assert False, 'expected OKXRateLimitUnavailable'
    except OKXRateLimitUnavailable as exc:
        assert round(exc.wait_seconds, 1) <= 9.0
        assert round(exc.wait_seconds, 1) > 0


def test_okx_rubik_requests_respect_min_interval(monkeypatch):
    clear_okx_rate_limit_state()
    sleep_calls = []
    time_values = iter([10.0, 10.3, 10.3, 11.3])

    monkeypatch.setattr('coinx.collector.rate_limit.time.time', lambda: next(time_values))
    monkeypatch.setattr('coinx.collector.rate_limit.time.sleep', lambda seconds: sleep_calls.append(round(seconds, 2)))

    okx_series._okx_rate_limits.wait_for_slot('okx', 'rubik', proxy_id='proxy-test', min_interval_ms=1000)
    okx_series._okx_rate_limits.wait_for_slot('okx', 'rubik', proxy_id='proxy-test', min_interval_ms=1000)

    assert sleep_calls == [0.7, 0.7]


def test_okx_min_intervals_are_grouped_by_endpoint(monkeypatch):
    monkeypatch.setattr(okx_series, 'OKX_RUBIK_MIN_INTERVAL_MS', 500)

    assert okx_series._okx_rate_limit_group('/api/v5/rubik/stat/taker-volume') == 'rubik'
    assert okx_series._okx_min_interval_ms('rubik') == 500
    assert okx_series._okx_rate_limit_group('/api/v5/public/funding-rate') == 'funding'
    assert okx_series._okx_min_interval_ms('funding') == 200
    assert okx_series._okx_rate_limit_group('/api/v5/market/history-candles') == 'default'
    assert okx_series._okx_min_interval_ms('default') == 100


def test_rate_limit_registry_uses_direct_proxy_by_default():
    registry = RateLimitRegistry()

    registry.mark_cooldown('okx', 'rubik', 5, proxy_id='direct')

    assert registry.unavailable_remaining_seconds('okx', 'rubik') > 0
    assert registry.unavailable_remaining_seconds('okx', 'rubik', proxy_id='direct') > 0
    assert registry.unavailable_remaining_seconds('okx', 'rubik', proxy_id='proxy-a') == 0


def test_okx_429_cooldown_only_blocks_current_proxy(monkeypatch):
    clear_okx_rate_limit_state()

    class FakeResponse:
        status_code = 429
        reason = 'Too Many Requests'
        headers = {'Retry-After': '7'}

    def fake_request(session, url, params=None, timeout=10, max_retries=3, base_delay=0.5):
        error = requests.exceptions.HTTPError('429 Too Many Requests')
        error.response = FakeResponse()
        raise error

    monkeypatch.setattr(okx_series, 'request_with_retry', fake_request)
    monkeypatch.setattr(okx_series, 'OKX_RUBIK_MIN_INTERVAL_MS', 0)
    monkeypatch.setattr(okx_series, 'get_okx_session', lambda proxy_id=None: requests.Session())

    try:
        okx_series._request_okx('/api/v5/rubik/stat/taker-volume', {'ccy': 'BTC'}, proxy_id='proxy-a')
    except requests.exceptions.HTTPError:
        pass

    with requests.Session() as session:
        try:
            okx_series._request_okx('/api/v5/rubik/stat/taker-volume', {'ccy': 'BTC'}, session=session, proxy_id='proxy-a')
            assert False, 'expected OKXRateLimitUnavailable for proxy-a'
        except OKXRateLimitUnavailable as exc:
            assert round(exc.wait_seconds, 1) <= 7.0
            assert round(exc.wait_seconds, 1) > 0

    try:
        okx_series._request_okx('/api/v5/rubik/stat/taker-volume', {'ccy': 'BTC'}, proxy_id='proxy-b')
        assert False, 'expected upstream HTTPError for proxy-b'
    except requests.exceptions.HTTPError:
        pass


def test_okx_request_uses_selected_proxy_session(monkeypatch):
    calls = []

    class FakeSession:
        pass

    def fake_get_okx_session(proxy_id=None):
        calls.append(proxy_id)
        return FakeSession()

    monkeypatch.setattr(okx_series, 'get_okx_session', fake_get_okx_session)
    monkeypatch.setattr(okx_series, 'choose_okx_proxy_id', lambda: 'proxy-jp')
    monkeypatch.setattr(okx_series, 'OKX_RUBIK_MIN_INTERVAL_MS', 0)
    monkeypatch.setattr(
        okx_series,
        'request_with_retry',
        lambda session, url, params=None, timeout=10, max_retries=3, base_delay=0.5: type(
            'Resp',
            (),
            {
                'status_code': 200,
                'headers': {},
                'raise_for_status': lambda self: None,
                'json': lambda self: {'code': '0', 'data': []},
            },
        )(),
    )

    okx_series._request_okx('/api/v5/rubik/stat/taker-volume', {'ccy': 'BTC'})

    assert calls == ['proxy-jp']
