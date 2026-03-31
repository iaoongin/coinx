from coinx.collector.binance.repair import (
    repair_single_series,
    repair_tracked_symbols,
    run_series_repair_job,
)
from coinx.models import BinanceKline, BinanceOpenInterestHist


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
        ('klines', 'BTCUSDT', '5m', 2, 0, 300000),
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


def test_repair_single_series_can_overlap_for_coverage_backfill(db_session, monkeypatch):
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
        10,
    )
    monkeypatch.setattr('coinx.collector.binance.repair.BINANCE_SERIES_REPAIR_SLEEP_MS', 0)

    existing = {
        'symbol': 'BTCUSDT',
        'period': '5m',
        'open_time': 300000,
        'close_time': 599999,
        'open_price': 1,
        'high_price': 2,
        'low_price': 1,
        'close_price': 2,
        'volume': 10,
        'quote_volume': 20,
        'trade_count': 3,
        'taker_buy_base_volume': 4,
        'taker_buy_quote_volume': 5,
        'raw_json': [],
    }
    from coinx.repositories.binance_series import upsert_series_records
    upsert_series_records('klines', [existing], session=db_session)

    calls = []

    def fake_fetch(series_type, symbol, period, limit, session=None, start_time=None, end_time=None):
        calls.append((start_time, end_time))
        return [
            _make_kline_payload(0),
            _make_kline_payload(300000),
            _make_kline_payload(600000),
        ]

    monkeypatch.setattr('coinx.collector.binance.repair.fetch_series_payload', fake_fetch)

    summary = repair_single_series(
        symbol='BTCUSDT',
        series_type='klines',
        now_ms=600000,
        db_session=db_session,
    )

    rows = db_session.query(BinanceKline).order_by(BinanceKline.open_time.asc()).all()

    assert summary['status'] == 'success'
    assert summary['affected'] == 3
    assert len(rows) == 3
    assert [row.open_time for row in rows] == [0, 300000, 600000]
    assert calls == [(0, 600000)]


def test_repair_single_series_pages_futures_history_by_time_windows(db_session, monkeypatch):
    monkeypatch.setattr(
        'coinx.collector.binance.repair.build_repair_window',
        lambda *args, **kwargs: {
            'start_time': 0,
            'end_time': 900000,
            'has_gap': True,
            'earliest_local_timestamp': None,
            'latest_local_timestamp': None,
        },
    )
    monkeypatch.setattr(
        'coinx.collector.binance.repair.BINANCE_SERIES_REPAIR_FUTURES_PAGE_LIMIT',
        2,
    )
    monkeypatch.setattr('coinx.collector.binance.repair.BINANCE_SERIES_REPAIR_SLEEP_MS', 0)
    monkeypatch.setattr(
        'coinx.collector.binance.repair.parse_series_payload',
        lambda *args, **kwargs: kwargs['payload'],
    )

    calls = []
    all_records = [
        {
            'symbol': 'BTCUSDT',
            'period': '5m',
            'event_time': event_time,
            'sum_open_interest': 100 + index,
            'sum_open_interest_value': 200 + index,
            'cmc_circulating_supply': None,
            'raw_json': {},
        }
        for index, event_time in enumerate([0, 300000, 600000, 900000])
    ]

    def fake_fetch(series_type, symbol, period, limit, session=None, start_time=None, end_time=None):
        calls.append((start_time, end_time))
        in_window = [record for record in all_records if start_time <= record['event_time'] <= end_time]
        return in_window[-limit:]

    monkeypatch.setattr('coinx.collector.binance.repair.fetch_series_payload', fake_fetch)

    summary = repair_single_series(
        symbol='BTCUSDT',
        series_type='open_interest_hist',
        now_ms=900000,
        db_session=db_session,
    )

    rows = (
        db_session.query(BinanceOpenInterestHist)
        .order_by(BinanceOpenInterestHist.event_time.asc())
        .all()
    )

    assert summary['status'] == 'success'
    assert summary['affected'] == 4
    assert summary['records'] == 4
    assert [row.event_time for row in rows] == [0, 300000, 600000, 900000]
    assert calls == [
        (0, 300000),
        (600000, 900000),
    ]


def test_repair_single_series_continues_after_empty_head_page(db_session, monkeypatch):
    monkeypatch.setattr(
        'coinx.collector.binance.repair.build_repair_window',
        lambda *args, **kwargs: {
            'start_time': 0,
            'end_time': 900000,
            'has_gap': True,
            'earliest_local_timestamp': None,
            'latest_local_timestamp': None,
        },
    )
    monkeypatch.setattr(
        'coinx.collector.binance.repair.BINANCE_SERIES_REPAIR_FUTURES_PAGE_LIMIT',
        2,
    )
    monkeypatch.setattr('coinx.collector.binance.repair.BINANCE_SERIES_REPAIR_SLEEP_MS', 0)
    monkeypatch.setattr(
        'coinx.collector.binance.repair.parse_series_payload',
        lambda *args, **kwargs: kwargs['payload'],
    )

    calls = []

    def fake_fetch(series_type, symbol, period, limit, session=None, start_time=None, end_time=None):
        calls.append((start_time, end_time))
        if start_time == 0:
            return []
        return [
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'event_time': 600000,
                'sum_open_interest': 101,
                'sum_open_interest_value': 201,
                'cmc_circulating_supply': None,
                'raw_json': {},
            },
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'event_time': 900000,
                'sum_open_interest': 102,
                'sum_open_interest_value': 202,
                'cmc_circulating_supply': None,
                'raw_json': {},
            },
        ]

    monkeypatch.setattr('coinx.collector.binance.repair.fetch_series_payload', fake_fetch)

    summary = repair_single_series(
        symbol='BTCUSDT',
        series_type='open_interest_hist',
        now_ms=900000,
        db_session=db_session,
    )

    rows = (
        db_session.query(BinanceOpenInterestHist)
        .order_by(BinanceOpenInterestHist.event_time.asc())
        .all()
    )

    assert summary['status'] == 'success'
    assert summary['affected'] == 2
    assert [row.event_time for row in rows] == [600000, 900000]
    assert calls == [
        (0, 300000),
        (600000, 900000),
    ]


def test_repair_single_series_logs_progress(db_session, monkeypatch):
    monkeypatch.setattr(
        'coinx.collector.binance.repair.build_repair_window',
        lambda *args, **kwargs: {
            'start_time': 0,
            'end_time': 300000,
            'has_gap': True,
            'earliest_local_timestamp': None,
            'latest_local_timestamp': None,
        },
    )
    monkeypatch.setattr(
        'coinx.collector.binance.repair.BINANCE_SERIES_REPAIR_KLINES_PAGE_LIMIT',
        10,
    )
    monkeypatch.setattr('coinx.collector.binance.repair.BINANCE_SERIES_REPAIR_SLEEP_MS', 0)
    monkeypatch.setattr(
        'coinx.collector.binance.repair.fetch_series_payload',
        lambda *args, **kwargs: [_make_kline_payload(0), _make_kline_payload(300000)],
    )

    info_logs = []
    monkeypatch.setattr('coinx.collector.binance.repair.logger.info', info_logs.append)

    repair_single_series(
        symbol='BTCUSDT',
        series_type='klines',
        now_ms=300000,
        db_session=db_session,
    )

    assert any('开始修补历史序列: 币种=BTCUSDT, 类型=klines' in message for message in info_logs)
    assert any('修补分页请求: 币种=BTCUSDT, 类型=klines, 页码=1' in message for message in info_logs)
    assert any('修补分页完成: 币种=BTCUSDT, 类型=klines, 页码=1' in message for message in info_logs)
    assert any('历史序列修补完成: 币种=BTCUSDT, 类型=klines, 状态=成功' in message for message in info_logs)


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


def test_repair_tracked_symbols_logs_batch_progress(monkeypatch):
    monkeypatch.setattr('coinx.collector.binance.repair.get_active_coins', lambda: ['BTCUSDT', 'ETHUSDT'])
    monkeypatch.setattr(
        'coinx.collector.binance.repair.repair_single_series',
        lambda symbol, series_type, **kwargs: {
            'symbol': symbol,
            'series_type': series_type,
            'status': 'success',
            'affected': 1,
            'pages': 1,
        },
    )

    info_logs = []
    monkeypatch.setattr('coinx.collector.binance.repair.logger.info', info_logs.append)

    repair_tracked_symbols(
        series_types=['klines', 'open_interest_hist'],
        now_ms=900000,
    )

    assert any('开始修补已跟踪币种历史序列: 币种数量=2' in message for message in info_logs)
    assert any('已跟踪币种修补进度: 任务=1/4, 币种=BTCUSDT, 类型=klines' in message for message in info_logs)
    assert any('已跟踪币种修补结果: 任务=4/4, 币种=ETHUSDT, 类型=open_interest_hist, 状态=success' in message for message in info_logs)
    assert any('已跟踪币种历史序列修补完成: 总任务数=4, 成功=4, 失败=0, 跳过=0' in message for message in info_logs)


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
