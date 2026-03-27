from coinx.collector.binance.series import collect_series_batch


def test_collect_series_batch_loops_over_symbols_periods_and_series_types(monkeypatch):
    calls = []

    def fake_collect(series_type, symbol, period, limit, http_session=None, db_session=None):
        calls.append((series_type, symbol, period, limit))
        return {
            'series_type': series_type,
            'symbol': symbol,
            'period': period,
            'limit': limit,
            'affected': 1,
            'records': [],
        }

    monkeypatch.setattr('coinx.collector.binance.series.collect_and_store_series', fake_collect)

    result = collect_series_batch(
        symbols=['BTCUSDT', 'ETHUSDT'],
        periods=['5m', '1h'],
        series_types=['klines', 'open_interest_hist'],
        limit=10,
    )

    assert result['success_count'] == 8
    assert result['failure_count'] == 0
    assert len(calls) == 8
    assert calls[0] == ('klines', 'BTCUSDT', '5m', 10)


def test_collect_series_batch_continues_after_single_failure(monkeypatch):
    calls = []

    def fake_collect(series_type, symbol, period, limit, http_session=None, db_session=None):
        calls.append((series_type, symbol, period, limit))
        if symbol == 'ETHUSDT':
            raise RuntimeError('boom')
        return {
            'series_type': series_type,
            'symbol': symbol,
            'period': period,
            'limit': limit,
            'affected': 1,
            'records': [],
        }

    monkeypatch.setattr('coinx.collector.binance.series.collect_and_store_series', fake_collect)

    result = collect_series_batch(
        symbols=['BTCUSDT', 'ETHUSDT'],
        periods=['5m'],
        series_types=['klines'],
        limit=5,
    )

    assert result['success_count'] == 1
    assert result['failure_count'] == 1
    assert result['results'][1]['status'] == 'error'
