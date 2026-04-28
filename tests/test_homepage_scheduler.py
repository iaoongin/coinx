from coinx.repositories.homepage_series import HOMEPAGE_REQUIRED_SERIES_TYPES
from coinx.scheduler import scheduled_repair_tracked


def test_scheduled_repair_tracked_repairs_active_coins(monkeypatch):
    calls = {'repair': None}

    def fake_repair(symbols=None, series_types=None):
        calls['repair'] = {
            'symbols': symbols,
            'series_types': series_types,
        }
        return {'status': 'success', 'results': []}

    monkeypatch.setattr('coinx.scheduler.get_active_coins', lambda: ['BTCUSDT'])
    monkeypatch.setattr('coinx.scheduler.repair_latest_tracked_symbols', fake_repair)

    scheduled_repair_tracked()

    assert calls['repair'] == {
        'symbols': ['BTCUSDT'],
        'series_types': list(HOMEPAGE_REQUIRED_SERIES_TYPES),
    }


def test_scheduled_repair_tracked_skips_when_no_active_coins(monkeypatch):
    calls = {'repair': 0}

    monkeypatch.setattr('coinx.scheduler.get_active_coins', lambda: [])
    monkeypatch.setattr(
        'coinx.scheduler.repair_latest_tracked_symbols',
        lambda **kwargs: calls.__setitem__('repair', calls['repair'] + 1),
    )

    scheduled_repair_tracked()

    assert calls['repair'] == 0
