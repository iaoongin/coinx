from coinx.collector.binance.repair import (
    repair_rolling_tracked_symbols,
    repair_single_series,
    repair_tracked_symbols,
    run_history_repair_job,
    run_series_repair_job,
)
from coinx.models import BinanceKline, BinanceOpenInterestHist, BinanceTakerBuySellVol
from coinx.repositories.binance_series import upsert_series_records


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
    assert summary['pages'] == 1
    assert summary['affected'] == 2
    assert len(rows) == 2
    assert rows[-1].open_time == 300000
    assert calls == [
        ('klines', 'BTCUSDT', '5m', 2, 0, 300000),
        ('klines', 'BTCUSDT', '5m', 2, 600000, 600000),
    ]


def test_repair_single_series_trims_unclosed_kline_records(db_session, monkeypatch):
    upsert_series_records(
        'klines',
        [
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'open_time': 600000,
                'close_time': 899999,
                'open_price': 1,
                'high_price': 2,
                'low_price': 1,
                'close_price': 2,
                'volume': 10,
                'quote_volume': 20,
                'trade_count': 3,
                'taker_buy_base_volume': 4,
                'taker_buy_quote_volume': 5,
            }
        ],
        session=db_session,
    )

    monkeypatch.setattr('coinx.collector.binance.repair.BINANCE_SERIES_REPAIR_SLEEP_MS', 0)
    monkeypatch.setattr('coinx.collector.binance.repair.BINANCE_SERIES_REPAIR_KLINES_PAGE_LIMIT', 2)
    monkeypatch.setattr('coinx.collector.binance.repair.fetch_series_payload', lambda *args, **kwargs: [])

    summary = repair_single_series(
        symbol='BTCUSDT',
        series_type='klines',
        now_ms=600000,
        db_session=db_session,
    )

    rows = db_session.query(BinanceKline).all()

    assert summary['status'] == 'success'
    assert len(rows) == 0


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
    assert summary['affected'] == 2
    assert len(rows) == 2
    assert [row.open_time for row in rows] == [0, 300000]
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
        lambda *args, **kwargs: kwargs.get('payload', args[1] if len(args) > 1 else None),
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
    assert summary['affected'] == 3
    assert summary['records'] == 3
    assert [row.event_time for row in rows] == [0, 300000, 600000]
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
        lambda *args, **kwargs: kwargs.get('payload', args[1] if len(args) > 1 else None),
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
            },
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'event_time': 900000,
                'sum_open_interest': 102,
                'sum_open_interest_value': 202,
                'cmc_circulating_supply': None,
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
    assert summary['affected'] == 1
    assert [row.event_time for row in rows] == [600000]
    assert calls == [
        (0, 300000),
        (600000, 900000),
    ]


def test_repair_single_series_trims_unclosed_taker_buy_sell_vol_records(db_session, monkeypatch):
    monkeypatch.setattr(
        'coinx.collector.binance.repair.build_repair_window',
        lambda *args, **kwargs: {
            'start_time': 0,
            'end_time': 600000,
            'has_gap': True,
            'earliest_local_timestamp': None,
            'latest_local_timestamp': None,
        },
    )
    monkeypatch.setattr('coinx.collector.binance.repair.BINANCE_SERIES_REPAIR_SLEEP_MS', 0)
    monkeypatch.setattr('coinx.collector.binance.repair.BINANCE_SERIES_REPAIR_FUTURES_PAGE_LIMIT', 10)

    calls = []

    def fake_fetch(*args, **kwargs):
        calls.append(kwargs.get('end_time'))
        if len(calls) == 1:
            return [
                {
                    'symbol': 'BTCUSDT',
                    'period': '5m',
                    'event_time': 300000,
                    'buy_sell_ratio': 1.0,
                    'buy_vol': 10.0,
                    'sell_vol': 5.0,
                },
                {
                    'symbol': 'BTCUSDT',
                    'period': '5m',
                    'event_time': 600000,
                    'buy_sell_ratio': 1.0,
                    'buy_vol': 11.0,
                    'sell_vol': 6.0,
                },
            ]
        return []

    monkeypatch.setattr('coinx.collector.binance.repair.fetch_series_payload', fake_fetch)
    monkeypatch.setattr(
        'coinx.collector.binance.repair.parse_series_payload',
        lambda *args, **kwargs: kwargs.get('payload', args[1] if len(args) > 1 else None),
    )

    summary = repair_single_series(
        symbol='BTCUSDT',
        series_type='taker_buy_sell_vol',
        now_ms=600000,
        db_session=db_session,
    )

    rows = db_session.query(BinanceTakerBuySellVol).order_by(BinanceTakerBuySellVol.event_time.asc()).all()

    assert summary['status'] == 'success'
    assert summary['affected'] == 1
    assert [row.event_time for row in rows] == [300000]
    assert calls == [600000, 299999, 599999]


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

    assert any('BTCUSDT' in message and 'klines' in message for message in info_logs)
    assert any('1' in message for message in info_logs)
    assert any('BTCUSDT' in message and 'klines' in message for message in info_logs)
    assert any('BTCUSDT' in message and 'klines' in message for message in info_logs)


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

    assert any('寮€濮嬩慨琛ュ巻鍙插簭鍒? 甯佺鏁伴噺=2' in message for message in info_logs)
    assert any('淇ˉ杩涘害: 浠诲姟=1/4, 甯佺=BTCUSDT, 绫诲瀷=klines' in message for message in info_logs)
    assert any('淇ˉ缁撴灉: 浠诲姟=4/4, 甯佺=ETHUSDT, 绫诲瀷=open_interest_hist, 鐘舵€?success' in message for message in info_logs)
    assert any('鍘嗗彶搴忓垪淇ˉ瀹屾垚: 鎬讳换鍔℃暟=4, 鎴愬姛=4, 澶辫触=0, 璺宠繃=0' in message for message in info_logs)


def test_run_series_repair_job_respects_enabled_flag(monkeypatch):
    monkeypatch.setattr('coinx.collector.binance.repair.BINANCE_SERIES_REPAIR_ENABLED', False)

    skipped = run_series_repair_job()

    assert skipped['status'] == 'skipped'

    monkeypatch.setattr('coinx.collector.binance.repair.BINANCE_SERIES_REPAIR_ENABLED', True)
    monkeypatch.setattr(
        'coinx.collector.binance.repair.repair_tracked_symbols',
        lambda **kwargs: {'status': 'success', 'success_count': 2, 'failure_count': 0, 'results': []},
    )

    executed = run_series_repair_job()

    assert executed['status'] == 'success'
    assert executed['success_count'] == 2


def test_repair_rolling_tracked_symbols_skips_existing_points(db_session, monkeypatch):
    upsert_series_records(
        'klines',
        [
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'open_time': 600000,
                'close_time': 899999,
                'open_price': 1,
                'high_price': 2,
                'low_price': 1,
                'close_price': 2,
                'volume': 10,
                'quote_volume': 20,
                'trade_count': 3,
                'taker_buy_base_volume': 4,
                'taker_buy_quote_volume': 5,
            },
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'open_time': 1200000,
                'close_time': 1499999,
                'open_price': 1,
                'high_price': 2,
                'low_price': 1,
                'close_price': 2,
                'volume': 10,
                'quote_volume': 20,
                'trade_count': 3,
                'taker_buy_base_volume': 4,
                'taker_buy_quote_volume': 5,
            },
        ],
        session=db_session,
    )

    calls = []

    def fake_fetch(series_type, symbol, period, limit, session=None, start_time=None, end_time=None):
        calls.append((start_time, end_time))
        return [_make_kline_payload(900000)]

    monkeypatch.setattr('coinx.collector.binance.repair.fetch_series_payload', fake_fetch)

    summary = repair_rolling_tracked_symbols(
        symbols=['BTCUSDT'],
        series_types=['klines'],
        now_ms=1500000,
        points=3,
        max_workers=1,
        db_session=db_session,
    )

    rows = db_session.query(BinanceKline).order_by(BinanceKline.open_time).all()

    assert summary['mode'] == 'rolling'
    assert summary['precheck_skipped_count'] == 0
    assert calls == [(900000, 900000)]
    assert [row.open_time for row in rows] == [600000, 900000, 1200000]


def test_run_history_repair_job_uses_configured_batch(monkeypatch):
    captured = {}

    def fake_repair(**kwargs):
        captured.update(kwargs)
        return {'status': 'success', 'success_count': 1, 'failure_count': 0, 'results': []}

    monkeypatch.setattr('coinx.collector.binance.repair.repair_tracked_symbols', fake_repair)
    monkeypatch.setattr('coinx.collector.binance.repair.REPAIR_HISTORY_SYMBOL_BATCH_SIZE', 3)

    run_history_repair_job(symbols=['A', 'B', 'C', 'D'], series_types=['klines'])

    assert captured['symbols'] == ['A', 'B', 'C']
    assert captured['symbol_batch_size'] is None
    assert captured['coverage_hours'] == 168
    assert captured['series_types'] == ['klines']
