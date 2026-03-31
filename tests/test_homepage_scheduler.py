from coinx.scheduler import scheduled_update


def test_scheduled_update_repairs_homepage_series_and_updates_drop_list(monkeypatch):
    calls = {'repair': None, 'drop_list': 0}

    def fail_old_path(*args, **kwargs):
        raise AssertionError('old scheduler path should not be used')

    def fake_repair(symbols=None, series_types=None):
        calls['repair'] = {
            'symbols': symbols,
            'series_types': series_types,
        }
        return {'status': 'success', 'results': []}

    monkeypatch.setattr('coinx.scheduler.update_all_data', fail_old_path, raising=False)
    monkeypatch.setattr('coinx.scheduler.should_update_cache', fail_old_path, raising=False)
    monkeypatch.setattr('coinx.scheduler.get_active_coins', lambda: ['BTCUSDT'])
    monkeypatch.setattr('coinx.scheduler.should_refresh_homepage_series', lambda symbols: True, raising=False)
    monkeypatch.setattr('coinx.scheduler.repair_tracked_symbols', fake_repair, raising=False)
    monkeypatch.setattr(
        'coinx.scheduler.update_drop_list_data',
        lambda: calls.__setitem__('drop_list', calls['drop_list'] + 1),
    )

    scheduled_update()

    assert calls['repair'] == {
        'symbols': ['BTCUSDT'],
        'series_types': ['klines', 'open_interest_hist'],
    }
    assert calls['drop_list'] == 1


def test_scheduled_update_skips_repair_when_homepage_series_is_fresh(monkeypatch):
    calls = {'repair': 0, 'drop_list': 0}

    def fail_old_path(*args, **kwargs):
        raise AssertionError('old scheduler path should not be used')

    monkeypatch.setattr('coinx.scheduler.update_all_data', fail_old_path, raising=False)
    monkeypatch.setattr('coinx.scheduler.should_update_cache', fail_old_path, raising=False)
    monkeypatch.setattr('coinx.scheduler.get_active_coins', lambda: ['BTCUSDT'])
    monkeypatch.setattr('coinx.scheduler.should_refresh_homepage_series', lambda symbols: False, raising=False)
    monkeypatch.setattr(
        'coinx.scheduler.repair_tracked_symbols',
        lambda **kwargs: calls.__setitem__('repair', calls['repair'] + 1),
        raising=False,
    )
    monkeypatch.setattr(
        'coinx.scheduler.update_drop_list_data',
        lambda: calls.__setitem__('drop_list', calls['drop_list'] + 1),
    )

    scheduled_update()

    assert calls['repair'] == 0
    assert calls['drop_list'] == 1
