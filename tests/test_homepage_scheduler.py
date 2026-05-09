from coinx.repositories.homepage_series import HOMEPAGE_REQUIRED_SERIES_TYPES
from coinx.scheduler import scheduled_repair_market_rolling


def test_scheduled_repair_market_rolling_repairs_score_symbols(monkeypatch):
    calls = {'repair': None}

    def fake_repair(symbols=None, series_types=None, **kwargs):
        calls['repair'] = {
            'symbols': symbols,
            'series_types': series_types,
        }
        return {
            'status': 'success',
            'results': [],
            'success_count': 2,
            'failure_count': 0,
            'skipped_count': 3,
            'precheck_skipped_count': 1,
            'duration_ms': 12.5,
        }

    monkeypatch.setattr('coinx.scheduler.get_market_structure_score_symbols', lambda: ['BTCUSDT', 'ETHUSDT'])
    monkeypatch.setattr('coinx.scheduler.repair_rolling_tracked_symbols', fake_repair)

    scheduled_repair_market_rolling()

    assert calls['repair'] == {
        'symbols': ['BTCUSDT', 'ETHUSDT'],
        'series_types': list(HOMEPAGE_REQUIRED_SERIES_TYPES),
    }


def test_scheduled_repair_market_rolling_skips_when_no_market_symbols(monkeypatch):
    calls = {'repair': 0}

    monkeypatch.setattr('coinx.scheduler.get_market_structure_score_symbols', lambda: [])
    monkeypatch.setattr(
        'coinx.scheduler.repair_rolling_tracked_symbols',
        lambda **kwargs: calls.__setitem__('repair', calls['repair'] + 1),
    )

    scheduled_repair_market_rolling()

    assert calls['repair'] == 0
