import requests

from coinx.collector.okx import series as okx_series
from coinx.collector.okx.series import (
    clear_supported_symbols_cache,
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
        '/api/v5/rubik/stat/contracts/open-interest-volume',
        {
            'ccy': 'BTC',
            'period': '5m',
            'begin': '1000',
            'end': '2000',
        },
    )
    assert calls[1] == (
        '/api/v5/rubik/stat/taker-volume',
        {
            'ccy': 'BTC',
            'instType': 'CONTRACTS',
            'period': '5m',
            'begin': '1000',
            'end': '2000',
        },
    )


def test_okx_rubik_fetchers_skip_windows_older_than_retention(monkeypatch):
    calls = []

    monkeypatch.setattr('coinx.collector.okx.series._request_okx', lambda *args, **kwargs: calls.append(args) or [])
    monkeypatch.setattr('coinx.collector.okx.series.time.time', lambda: 3 * 24 * 60 * 60)

    assert fetch_open_interest_hist('BTCUSDT', '5m', 500, start_time=0, end_time=1000) == []
    assert fetch_taker_buy_sell_vol('BTCUSDT', '5m', 500, start_time=0, end_time=1000) == []
    assert calls == []


def test_okx_rubik_fetchers_clip_windows_to_retention(monkeypatch):
    calls = []

    def fake_request(path, params, session=None):
        calls.append((path, params))
        return []

    monkeypatch.setattr('coinx.collector.okx.series._request_okx', fake_request)
    monkeypatch.setattr('coinx.collector.okx.series.time.time', lambda: 3 * 24 * 60 * 60)

    earliest_time = 24 * 60 * 60 * 1000
    fetch_open_interest_hist('BTCUSDT', '5m', 500, start_time=0, end_time=earliest_time + 1000)

    assert calls[0][1]['begin'] == str(earliest_time)
    assert calls[0][1]['end'] == str(earliest_time + 1000)


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
    sleeps = []

    class FakeResponse:
        status_code = 429
        reason = 'Too Many Requests'
        headers = {'Retry-After': '7'}

    def fake_request(session, url, params=None, timeout=10, max_retries=3, base_delay=0.5):
        error = requests.exceptions.HTTPError('429 Too Many Requests')
        error.response = FakeResponse()
        raise error

    monkeypatch.setattr(okx_series, 'request_with_retry', fake_request)
    monkeypatch.setattr(okx_series.time, 'sleep', lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(okx_series, 'OKX_RUBIK_MIN_INTERVAL_MS', 0)

    try:
        okx_series._request_okx('/api/v5/rubik/stat/taker-volume', {'ccy': 'BTC'})
    except requests.exceptions.HTTPError:
        pass

    assert sleeps == [7.0]


def test_okx_request_uses_fallback_backoff_without_retry_header(monkeypatch):
    sleeps = []

    class FakeResponse:
        status_code = 429
        reason = 'Too Many Requests'
        headers = {}

    def fake_request(session, url, params=None, timeout=10, max_retries=3, base_delay=0.5):
        error = requests.exceptions.HTTPError('429 Too Many Requests')
        error.response = FakeResponse()
        raise error

    monkeypatch.setattr(okx_series, 'request_with_retry', fake_request)
    monkeypatch.setattr(okx_series.time, 'sleep', lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(okx_series, 'OKX_RUBIK_MIN_INTERVAL_MS', 0)
    monkeypatch.setattr(okx_series, 'OKX_429_RETRY_FALLBACK_SECONDS', 9)

    try:
        okx_series._request_okx('/api/v5/rubik/stat/taker-volume', {'ccy': 'BTC'})
    except requests.exceptions.HTTPError:
        pass

    assert sleeps == [9.0]


def test_okx_rubik_requests_respect_min_interval(monkeypatch):
    okx_series._okx_rate_limit_state.clear()
    sleep_calls = []
    time_values = iter([10.0, 10.3, 11.3])

    monkeypatch.setattr(okx_series, 'OKX_RUBIK_MIN_INTERVAL_MS', 1000)
    monkeypatch.setattr(okx_series.time, 'time', lambda: next(time_values))
    monkeypatch.setattr(okx_series.time, 'sleep', lambda seconds: sleep_calls.append(round(seconds, 2)))
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
    okx_series._request_okx('/api/v5/rubik/stat/taker-volume', {'ccy': 'BTC'})

    assert sleep_calls == [0.7]
