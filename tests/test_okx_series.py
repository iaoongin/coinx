from coinx.collector.okx.series import (
    clear_supported_symbols_cache,
    get_supported_symbols,
    is_symbol_supported,
    parse_klines,
    parse_open_interest_hist,
    parse_taker_buy_sell_vol,
    to_exchange_symbol,
    to_internal_symbol,
)


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
