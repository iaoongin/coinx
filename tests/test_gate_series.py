import requests

from coinx.collector.exchange_adapters import get_exchange_adapter
from coinx.collector.gate import series as gate_series


def test_gate_symbol_mapping_round_trip():
    assert gate_series.to_exchange_symbol('BTCUSDT') == 'BTC_USDT'
    assert gate_series.to_internal_symbol('BTC_USDT') == 'BTCUSDT'


def test_parse_gate_klines_maps_candle_payload():
    payload = [
        {
            't': 1711526400,
            'o': '68000.10',
            'h': '68100.20',
            'l': '67950.30',
            'c': '68020.40',
            'v': '123.45',
            'sum': '8398765.43',
        }
    ]

    records = gate_series.parse_klines(payload, symbol='BTCUSDT', period='5m')

    assert records[0]['symbol'] == 'BTCUSDT'
    assert records[0]['open_time'] == 1711526400000
    assert records[0]['close_time'] == 1711526699999
    assert records[0]['open_price'] == 68000.10
    assert records[0]['close_price'] == 68020.40
    assert records[0]['volume'] == 123.45
    assert records[0]['quote_volume'] == 8398765.43


def test_parse_gate_open_interest_maps_history_rows():
    payload = [
        {
            't': 1711526400,
            'open_interest': '12345.67',
            'open_interest_usd': '987654.32',
        }
    ]

    records = gate_series.parse_open_interest_hist(payload, symbol='BTCUSDT', period='5m')

    assert records[0]['symbol'] == 'BTCUSDT'
    assert records[0]['event_time'] == 1711526400000
    assert records[0]['sum_open_interest'] == 12345.67
    assert records[0]['sum_open_interest_value'] == 987654.32


def test_gate_fetchers_use_expected_request_params(monkeypatch):
    calls = []

    def fake_request(path, params, session=None, timeout=10):
        calls.append((path, params))
        return []

    monkeypatch.setattr(gate_series, '_request_gate', fake_request)

    gate_series.fetch_klines('BTCUSDT', '5m', 9999, start_time=1000, end_time=2000)
    gate_series.fetch_open_interest_hist('BTCUSDT', '5m', 9999, start_time=1000, end_time=2000)

    assert calls[0] == (
        '/api/v4/futures/usdt/candlesticks',
        {
            'contract': 'BTC_USDT',
            'interval': '5m',
            'from': '1',
            'to': '2',
        },
    )
    assert calls[1] == (
        '/api/v4/futures/usdt/contract_stats',
        {
            'contract': 'BTC_USDT',
            'interval': '5m',
            'from': '1',
            'limit': '1000',
        },
    )


def test_gate_fetchers_use_limit_without_time_window(monkeypatch):
    calls = []

    def fake_request(path, params, session=None, timeout=10):
        calls.append((path, params))
        return []

    monkeypatch.setattr(gate_series, '_request_gate', fake_request)

    gate_series.fetch_klines('BTCUSDT', '5m', 9999)
    gate_series.fetch_open_interest_hist('BTCUSDT', '5m', 9999)

    assert calls[0][1] == {
        'contract': 'BTC_USDT',
        'interval': '5m',
        'limit': '1000',
    }
    assert calls[1][1] == {
        'contract': 'BTC_USDT',
        'interval': '5m',
        'limit': '1000',
    }


def test_gate_adapter_supports_klines_open_interest_and_taker():
    adapter = get_exchange_adapter('gate')

    assert adapter.exchange_id == 'gate'
    assert 'klines' in adapter.supported_series_types
    assert 'open_interest_hist' in adapter.supported_series_types
    assert 'taker_buy_sell_vol' in adapter.supported_series_types
    assert adapter.supports_time_window('klines') is True
    assert adapter.supports_time_window('open_interest_hist') is True
    assert adapter.page_limit('klines') == 1000
    assert adapter.page_limit('open_interest_hist') == 1000


def test_gate_adapter_support_state_uses_contracts_lookup(monkeypatch):
    gate_series.clear_supported_symbols_cache()
    gate_series.clear_gate_rate_limit_state()

    calls = []

    def fake_is_symbol_supported(symbol, series_type=None, session=None):
        calls.append((symbol, session))
        return symbol == 'BTCUSDT'

    monkeypatch.setattr(gate_series, 'is_symbol_supported', fake_is_symbol_supported)
    adapter = get_exchange_adapter('gate')

    result = adapter.symbol_support_state('BTCUSDT')

    assert result == {
        'state': 'supported',
        'supported': True,
        'known': True,
    }
    assert calls == [('BTCUSDT', None)]


def test_gate_is_symbol_supported_fetches_contracts_when_cache_empty(monkeypatch):
    gate_series.clear_supported_symbols_cache()
    gate_series.clear_gate_rate_limit_state()

    calls = []

    def fake_get_supported_symbols(session=None, ttl_seconds=None):
        calls.append(session)
        return {'BTCUSDT'}

    monkeypatch.setattr(gate_series, 'get_supported_symbols', fake_get_supported_symbols)

    assert gate_series.is_symbol_supported('BTCUSDT', series_type='klines') is True
    assert gate_series.is_symbol_supported('ETHUSDT', series_type='klines') is False
    assert calls == [None, None]


def test_gate_is_symbol_supported_returns_false_and_enters_backoff_when_contract_lookup_fails(monkeypatch):
    gate_series.clear_supported_symbols_cache()
    gate_series.clear_gate_rate_limit_state()

    calls = []

    def fail_get_supported_symbols(session=None, ttl_seconds=None):
        calls.append(session)
        raise RuntimeError('contracts unavailable')

    monkeypatch.setattr(gate_series, 'get_supported_symbols', fail_get_supported_symbols)
    monkeypatch.setattr(gate_series.logger, 'warning', lambda *args, **kwargs: None)

    assert gate_series.is_symbol_supported('BTCUSDT', series_type='klines') is False
    assert gate_series.is_symbol_supported('ETHUSDT', series_type='klines') is False
    assert calls == [None]


def test_gate_warm_supported_symbols_cache_swallows_lookup_error_and_enters_backoff(monkeypatch):
    gate_series.clear_supported_symbols_cache()
    gate_series.clear_gate_rate_limit_state()

    calls = []

    def fail_get_supported_symbols(session=None, ttl_seconds=None):
        calls.append(session)
        raise RuntimeError('contracts unavailable')

    monkeypatch.setattr(gate_series, 'get_supported_symbols', fail_get_supported_symbols)
    monkeypatch.setattr(gate_series.logger, 'warning', lambda *args, **kwargs: None)

    assert gate_series.warm_supported_symbols_cache() is None
    assert gate_series.warm_supported_symbols_cache() is None
    assert calls == [None]


def test_gate_403_without_headers_marks_budget_unavailable(monkeypatch):
    gate_series.clear_gate_rate_limit_state()

    response = requests.Response()
    response.status_code = 403
    response.reason = 'Forbidden'
    response.url = 'https://example.com'
    response._content = b'{"label":"FORBIDDEN"}'
    response.headers['Content-Type'] = 'application/json'

    def fake_request(*args, **kwargs):
        error = requests.exceptions.HTTPError('403 Forbidden')
        error.response = response
        raise error

    monkeypatch.setattr(gate_series, 'request_with_retry', fake_request)

    try:
        gate_series._request_gate('/api/v4/futures/usdt/candlesticks', {'contract': 'BTC_USDT'})
        assert False, 'expected GateRateLimitUnavailable'
    except gate_series.GateRateLimitUnavailable:
        pass

    assert gate_series.is_gate_budget_unavailable() is True


def test_gate_contract_not_found_marks_symbol_unsupported(monkeypatch):
    gate_series.clear_supported_symbols_cache()
    gate_series.clear_gate_rate_limit_state()

    response = requests.Response()
    response.status_code = 404
    response.reason = 'Not Found'
    response.url = 'https://example.com'
    response._content = b'{"label":"CONTRACT_NOT_FOUND"}'
    response.headers['Content-Type'] = 'application/json'

    def fake_request(*args, **kwargs):
        error = requests.exceptions.HTTPError('404 Not Found')
        error.response = response
        raise error

    monkeypatch.setattr(gate_series, 'request_with_retry', fake_request)

    try:
        gate_series.fetch_klines('MSTRUSDT', '5m', 5, start_time=1778850000000, end_time=1778851200000)
        assert False, 'expected GateUnsupportedContract'
    except gate_series.GateUnsupportedContract:
        pass

    assert gate_series.is_symbol_supported('MSTRUSDT', series_type='klines') is False
