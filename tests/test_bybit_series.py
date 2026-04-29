from coinx.collector.bybit import series as bybit_series
from coinx.collector.exchange_adapters import get_exchange_adapter
from coinx.collector.exchange_repair import repair_history_symbols
from coinx.models import MarketKline, MarketOpenInterestHist, MarketTakerBuySellVol


def test_parse_bybit_klines_maps_kline_array():
    payload = {
        'list': [
            [
                '1711526400000',
                '68000.10',
                '68100.20',
                '67950.30',
                '68020.40',
                '123.45',
                '8398765.43',
            ]
        ]
    }

    records = bybit_series.parse_klines(payload, symbol='BTCUSDT', period='5m')

    assert records[0]['symbol'] == 'BTCUSDT'
    assert records[0]['open_time'] == 1711526400000
    assert records[0]['close_time'] == 1711526699999
    assert records[0]['open_price'] == 68000.10
    assert records[0]['close_price'] == 68020.40
    assert records[0]['volume'] == 123.45
    assert records[0]['quote_volume'] == 8398765.43


def test_parse_bybit_open_interest_maps_history_rows():
    payload = {
        'list': [
            {
                'openInterest': '12345.67',
                'timestamp': '1711526400000',
            }
        ]
    }

    records = bybit_series.parse_open_interest_hist(payload, symbol='BTCUSDT', period='5m')

    assert records[0]['symbol'] == 'BTCUSDT'
    assert records[0]['event_time'] == 1711526400000
    assert records[0]['sum_open_interest'] == 12345.67
    assert records[0]['sum_open_interest_value'] is None


def test_bybit_fetchers_use_homepage_request_params(monkeypatch):
    calls = []

    def fake_request(path, params, session=None, timeout=10):
        calls.append((path, params))
        return {'list': []}

    monkeypatch.setattr(bybit_series, '_request_bybit', fake_request)

    bybit_series.fetch_klines('BTCUSDT', '5m', 9999, start_time=1000, end_time=2000)
    bybit_series.fetch_open_interest_hist('BTCUSDT', '5m', 9999, start_time=1000, end_time=2000)

    assert calls[0] == (
        '/v5/market/kline',
        {
            'category': 'linear',
            'symbol': 'BTCUSDT',
            'interval': '5',
            'limit': '1000',
            'start': '1000',
            'end': '2000',
        },
    )
    assert calls[1] == (
        '/v5/market/open-interest',
        {
            'category': 'linear',
            'symbol': 'BTCUSDT',
            'intervalTime': '5min',
            'limit': '200',
            'startTime': '1000',
            'endTime': '2000',
        },
    )


def test_bybit_adapter_supports_only_homepage_rest_series():
    adapter = get_exchange_adapter('bybit')

    assert adapter.exchange_id == 'bybit'
    assert adapter.supported_series_types == ('klines', 'open_interest_hist')
    assert adapter.supports_time_window('klines') is True
    assert adapter.supports_time_window('open_interest_hist') is True
    assert adapter.page_limit('open_interest_hist') == 200
    assert 'taker_buy_sell_vol' not in adapter.supported_series_types


def test_exchange_repair_bybit_skips_taker_buy_sell_vol(db_session, monkeypatch):
    monkeypatch.setattr(bybit_series, 'is_symbol_supported', lambda *args, **kwargs: True)

    calls = []

    def fake_fetch(series_type, symbol, period, limit, session=None, start_time=None, end_time=None):
        calls.append((series_type, limit, start_time, end_time))
        if series_type == 'klines':
            return {
                'list': [
                    [
                        str(start_time),
                        '1.0',
                        '2.0',
                        '0.5',
                        '1.5',
                        '10.0',
                        '15.0',
                    ]
                ]
            }
        return {
            'list': [
                {
                    'openInterest': '100.0',
                    'timestamp': str(start_time),
                }
            ]
        }

    monkeypatch.setattr(bybit_series, 'fetch_series_payload', fake_fetch)

    summary = repair_history_symbols(
        symbols=['BTCUSDT'],
        series_types=['klines', 'open_interest_hist', 'taker_buy_sell_vol'],
        exchanges=['bybit'],
        now_ms=600000,
        coverage_hours=1,
        full_scan=True,
        max_workers=1,
        db_session=db_session,
    )

    assert summary['status'] == 'success'
    assert summary['exchanges'] == ['bybit']
    assert [item['series_type'] for item in summary['results']] == ['klines', 'open_interest_hist']
    assert calls == [
        ('klines', 1000, 0, 300000),
        ('open_interest_hist', 200, 0, 300000),
    ]
    assert db_session.query(MarketKline).filter_by(exchange='bybit').count() == 1
    assert db_session.query(MarketOpenInterestHist).filter_by(exchange='bybit').count() == 1
    assert db_session.query(MarketTakerBuySellVol).filter_by(exchange='bybit').count() == 0
