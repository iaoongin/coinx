import logging

import coinx.scheduler as scheduler_module
from coinx.repositories.homepage_series import HOMEPAGE_REQUIRED_SERIES_TYPES
from coinx.scheduler import scheduled_repair_market_history, scheduled_repair_market_rolling


def test_scheduled_repair_market_rolling_repairs_tracked_symbols_before_top_symbols(monkeypatch, caplog):
    calls = []

    def fake_repair(symbols=None, series_types=None, **kwargs):
        calls.append({
            'symbols': symbols,
            'series_types': series_types,
            'max_workers': kwargs.get('max_workers'),
        })
        return {
            'status': 'success',
            'results': [],
            'success_count': len(symbols or []),
            'failure_count': 0,
            'skipped_count': 0,
            'precheck_skipped_count': 1,
            'duration_ms': 12.5,
        }

    monkeypatch.setattr('coinx.scheduler.get_active_coins', lambda: ['BTCUSDT', 'ETHUSDT'])
    score_scope = {}

    def fake_score_symbols(**kwargs):
        score_scope.update(kwargs)
        return ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT']

    monkeypatch.setattr('coinx.scheduler.get_market_structure_score_symbols', fake_score_symbols)
    monkeypatch.setattr('coinx.scheduler.ENABLED_EXCHANGES', ['binance', 'okx', 'bybit'])
    monkeypatch.setattr('coinx.scheduler.repair_rolling_tracked_symbols', fake_repair)

    caplog.set_level(logging.INFO)
    scheduled_repair_market_rolling()

    assert calls == [
        {
            'symbols': ['BTCUSDT', 'ETHUSDT'],
            'series_types': list(HOMEPAGE_REQUIRED_SERIES_TYPES),
            'max_workers': 3,
        },
        {
            'symbols': ['SOLUSDT', 'BNBUSDT'],
            'series_types': list(HOMEPAGE_REQUIRED_SERIES_TYPES),
            'max_workers': 3,
        },
    ]
    assert score_scope == {
        'top_volume_limit': scheduler_module.FETCH_COINS_TOP_VOLUME_COUNT,
        'top_gainers_limit': scheduler_module.FETCH_COINS_TOP_GAINERS_COUNT,
        'top_losers_limit': scheduler_module.FETCH_COINS_TOP_LOSERS_COUNT,
    }
    messages = [record.getMessage() for record in caplog.records]
    tracked_start_index = next(
        index for index, message in enumerate(messages)
        if '阶段 1/2' in message and '跟踪币种' in message
    )
    top_start_index = next(
        index for index, message in enumerate(messages)
        if '阶段 2/2' in message and 'top 榜币种' in message
    )
    assert tracked_start_index < top_start_index


def test_scheduled_repair_market_rolling_skips_when_no_market_symbols(monkeypatch):
    calls = {'repair': 0}

    monkeypatch.setattr('coinx.scheduler.get_active_coins', lambda: [])
    monkeypatch.setattr('coinx.scheduler.get_market_structure_score_symbols', lambda **kwargs: [])
    monkeypatch.setattr(
        'coinx.scheduler.repair_rolling_tracked_symbols',
        lambda **kwargs: calls.__setitem__('repair', calls['repair'] + 1),
    )

    scheduled_repair_market_rolling()

    assert calls['repair'] == 0


def test_scheduled_repair_market_history_repairs_tracked_symbols_before_top_symbols(monkeypatch, caplog):
    calls = []

    def fake_history(symbols=None, series_types=None, **kwargs):
        calls.append({
            'symbols': symbols,
            'series_types': series_types,
            'max_workers': kwargs.get('max_workers'),
        })
        return {
            'status': 'success',
            'results': [],
            'success_count': len(symbols or []),
            'failure_count': 0,
            'skipped_count': 0,
            'duration_ms': 25.0,
        }

    monkeypatch.setattr('coinx.scheduler.get_active_coins', lambda: ['BTCUSDT', 'QUSDT'])
    monkeypatch.setattr('coinx.scheduler.FETCH_COINS_ENABLED', True)
    monkeypatch.setattr(
        'coinx.scheduler.get_all_24hr_tickers',
        lambda: [
            {'symbol': 'BTCUSDT', 'priceChangePercent': 10, 'quoteVolume': 300},
            {'symbol': 'SOLUSDT', 'priceChangePercent': 9, 'quoteVolume': 200},
            {'symbol': 'BNBUSDT', 'priceChangePercent': -10, 'quoteVolume': 100},
            {'symbol': 'XRPUSDT', 'priceChangePercent': -9, 'quoteVolume': 50},
        ],
    )
    monkeypatch.setattr('coinx.scheduler.FETCH_COINS_TOP_VOLUME_COUNT', 3)
    monkeypatch.setattr('coinx.scheduler.FETCH_COINS_TOP_GAINERS_COUNT', 2)
    monkeypatch.setattr('coinx.scheduler.FETCH_COINS_TOP_LOSERS_COUNT', 2)
    monkeypatch.setattr('coinx.scheduler.ENABLED_EXCHANGES', ['binance', 'okx'])
    monkeypatch.setattr('coinx.scheduler.run_history_repair_job', fake_history)

    caplog.set_level(logging.INFO)
    scheduled_repair_market_history()

    assert calls == [
        {
            'symbols': ['BTCUSDT', 'QUSDT'],
            'series_types': list(HOMEPAGE_REQUIRED_SERIES_TYPES),
            'max_workers': 2,
        },
        {
            'symbols': ['SOLUSDT', 'BNBUSDT', 'XRPUSDT'],
            'series_types': list(HOMEPAGE_REQUIRED_SERIES_TYPES),
            'max_workers': 2,
        },
    ]
    messages = [record.getMessage() for record in caplog.records]
    tracked_start_index = next(
        index for index, message in enumerate(messages)
        if '历史修补阶段 1/2' in message and '跟踪币种' in message
    )
    top_start_index = next(
        index for index, message in enumerate(messages)
        if '历史修补阶段 2/2' in message and 'top 榜币种' in message
    )
    assert tracked_start_index < top_start_index
