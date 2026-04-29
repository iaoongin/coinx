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
