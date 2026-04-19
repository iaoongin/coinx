from coinx.scheduler import scheduled_update


def test_scheduled_update_repairs_homepage_series_and_updates_market_tickers(monkeypatch):
    calls = {'repair': None, 'market_tickers': 0}

    def fake_repair(symbols=None, series_types=None):
        calls['repair'] = {
            'symbols': symbols,
            'series_types': series_types,
        }
        return {'status': 'success', 'results': []}

    def fake_update_market_tickers(force_update=False):
        calls['market_tickers'] += 1

    monkeypatch.setattr('coinx.scheduler.get_active_coins', lambda: ['BTCUSDT'])
    monkeypatch.setattr('coinx.scheduler.should_refresh_homepage_series', lambda symbols: True)
    monkeypatch.setattr('coinx.scheduler.repair_tracked_symbols', fake_repair)
    monkeypatch.setattr('coinx.scheduler.update_market_tickers', fake_update_market_tickers)

    scheduled_update()

    assert calls['repair'] == {
        'symbols': ['BTCUSDT'],
        'series_types': ['klines', 'open_interest_hist'],
    }
    assert calls['market_tickers'] == 1


def test_scheduled_update_skips_repair_when_homepage_series_is_fresh(monkeypatch):
    calls = {'repair': 0, 'market_tickers': 0}

    def fake_update_market_tickers(force_update=False):
        calls['market_tickers'] += 1

    monkeypatch.setattr('coinx.scheduler.get_active_coins', lambda: ['BTCUSDT'])
    monkeypatch.setattr('coinx.scheduler.should_refresh_homepage_series', lambda symbols: False)
    monkeypatch.setattr(
        'coinx.scheduler.repair_tracked_symbols',
        lambda **kwargs: calls.__setitem__('repair', calls['repair'] + 1),
    )
    monkeypatch.setattr('coinx.scheduler.update_market_tickers', fake_update_market_tickers)

    scheduled_update()

    assert calls['repair'] == 0
    assert calls['market_tickers'] == 1