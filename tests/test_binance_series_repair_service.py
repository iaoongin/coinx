from coinx.collector.binance.repair import (
    repair_single_series,
    repair_tracked_symbols,
    run_series_repair_job,
)
from coinx.models import BinanceKline


def _make_kline_payload(open_time):
    return [
        open_time,
        '1.0',
        '2.0',
        '0.9',
        '1.5',
        '100.0',
        open_time + 299999,
        '150.0',
        12,
        '60.0',
        '90.0',
        '0',
    ]


def test_repair_single_series_pages_and_upserts_records(db_session, monkeypatch):
    monkeypatch.setattr(
        'coinx.collector.binance.repair.build_repair_window',
        lambda *args, **kwargs: {
            'start_time': 0,
            'end_time': 600000,
            'has_gap': True,
        },
    )
    monkeypatch.setattr(
        'coinx.collector.binance.repair.BINANCE_SERIES_REPAIR_KLINES_PAGE_LIMIT',
        2,
    )
    monkeypatch.setattr('coinx.collector.binance.repair.BINANCE_SERIES_REPAIR_SLEEP_MS', 0)

    calls = []

    def fake_fetch(series_type, symbol, period, limit, session=None, start_time=None, end_time=None):
        calls.append((series_type, symbol, period, limit, start_time, end_time))
        if start_time == 0:
            return [_make_kline_payload(0), _make_kline_payload(300000)]
        if start_time == 600000:
            return [_make_kline_payload(600000)]
        return []

    monkeypatch.setattr('coinx.collector.binance.repair.fetch_series_payload', fake_fetch)

    summary = repair_single_series(
        symbol='BTCUSDT',
        series_type='klines',
        now_ms=600000,
        db_session=db_session,
    )

    rows = db_session.query(BinanceKline).order_by(BinanceKline.open_time.asc()).all()

    assert summary['status'] == 'success'
    assert summary['pages'] == 2
    assert summary['affected'] == 3
    assert len(rows) == 3
    assert rows[-1].open_time == 600000
    assert calls == [
        ('klines', 'BTCUSDT', '5m', 2, 0, 600000),
        ('klines', 'BTCUSDT', '5m', 2, 600000, 600000),
    ]


def test_repair_single_series_skips_when_no_gap(monkeypatch):
    monkeypatch.setattr(
        'coinx.collector.binance.repair.build_repair_window',
        lambda *args, **kwargs: {
            'start_time': 1200000,
            'end_time': 900000,
            'has_gap': False,
        },
    )

    called = {'fetch': False}

    def fake_fetch(*args, **kwargs):
        called['fetch'] = True
        return []

    monkeypatch.setattr('coinx.collector.binance.repair.fetch_series_payload', fake_fetch)

    summary = repair_single_series(
        symbol='BTCUSDT',
        series_type='open_interest_hist',
        now_ms=900000,
    )

    assert summary['status'] == 'skipped'
    assert summary['affected'] == 0
    assert called['fetch'] is False


def test_repair_tracked_symbols_uses_active_coins_and_continues_after_errors(monkeypatch):
    monkeypatch.setattr('coinx.collector.binance.repair.get_active_coins', lambda: ['BTCUSDT', 'ETHUSDT'])

    calls = []

    def fake_repair(symbol, series_type, now_ms=None, http_session=None, db_session=None):
        calls.append((symbol, series_type))
        if symbol == 'ETHUSDT' and series_type == 'open_interest_hist':
            raise RuntimeError('boom')
        return {
            'symbol': symbol,
            'series_type': series_type,
            'status': 'success',
            'affected': 1,
        }

    monkeypatch.setattr('coinx.collector.binance.repair.repair_single_series', fake_repair)

    summary = repair_tracked_symbols(
        series_types=['klines', 'open_interest_hist'],
        now_ms=900000,
    )

    assert calls == [
        ('BTCUSDT', 'klines'),
        ('BTCUSDT', 'open_interest_hist'),
        ('ETHUSDT', 'klines'),
        ('ETHUSDT', 'open_interest_hist'),
    ]
    assert summary['success_count'] == 3
    assert summary['failure_count'] == 1
    assert len(summary['results']) == 4
    assert summary['results'][-1]['status'] == 'error'


def test_run_series_repair_job_respects_enabled_flag(monkeypatch):
    monkeypatch.setattr('coinx.collector.binance.repair.BINANCE_SERIES_REPAIR_ENABLED', False)

    skipped = run_series_repair_job()

    assert skipped['status'] == 'skipped'

    monkeypatch.setattr('coinx.collector.binance.repair.BINANCE_SERIES_REPAIR_ENABLED', True)
    monkeypatch.setattr(
        'coinx.collector.binance.repair.repair_tracked_symbols',
        lambda: {'status': 'success', 'success_count': 2, 'failure_count': 0, 'results': []},
    )

    executed = run_series_repair_job()

    assert executed['status'] == 'success'
    assert executed['success_count'] == 2
