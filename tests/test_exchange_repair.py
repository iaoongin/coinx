from dataclasses import dataclass
import threading

from coinx.collector.exchange_repair import repair_history_symbols, repair_rolling_symbols
from coinx.models import MarketKline, MarketOpenInterestHist, MarketTakerBuySellVol
from coinx.repositories.series import upsert_series_records


@dataclass(frozen=True)
class FakeAdapter:
    exchange_id: str
    supported_series_types: tuple
    precise_series_types: tuple
    calls: list
    supported_symbols: tuple = None
    series_period_map: dict = None

    def supports_time_window(self, series_type):
        return series_type in self.precise_series_types

    def supports_symbol(self, symbol, series_type=None, session=None):
        if self.supported_symbols is None:
            return True
        return symbol in self.supported_symbols

    def periods_for_series(self, series_type):
        if not self.series_period_map:
            return ('5m',)
        return self.series_period_map.get(series_type, ('5m',))

    def fetch_series_payload(self, series_type, symbol, period, limit, session=None, start_time=None, end_time=None):
        self.calls.append((self.exchange_id, series_type, symbol, limit, start_time, end_time))
        if series_type == 'klines':
            return [
                {
                    'symbol': symbol,
                    'period': period,
                    'open_time': start_time or 900000,
                    'close_time': (start_time or 900000) + 299999,
                    'open_price': 1,
                    'high_price': 2,
                    'low_price': 1,
                    'close_price': 1.5,
                    'ignored_extra': 'drop me',
                }
            ]
        if series_type == 'open_interest_hist':
            return [
                {
                    'symbol': symbol,
                    'period': period,
                    'event_time': start_time or 900000,
                    'sum_open_interest': 10,
                    'sum_open_interest_value': 15,
                }
            ]
        return [
            {
                'symbol': symbol,
                'period': period,
                'event_time': start_time or 900000,
                'buy_vol': 10,
                'sell_vol': 4,
            }
        ]

    def parse_series_payload(self, series_type, payload, symbol, period):
        return payload


def test_exchange_rolling_repair_uses_adapter_and_skips_existing_points(db_session, monkeypatch):
    upsert_series_records(
        'binance',
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
                'close_price': 1,
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
            },
        ],
        session=db_session,
    )
    calls = []
    adapter = FakeAdapter('binance', ('klines',), ('klines',), calls)
    monkeypatch.setattr('coinx.collector.exchange_repair.get_exchange_adapters', lambda exchanges: [adapter])

    summary = repair_rolling_symbols(
        symbols=['BTCUSDT'],
        series_types=['klines'],
        exchanges=['binance'],
        now_ms=1500000,
        points=3,
        max_workers=1,
        db_session=db_session,
    )

    rows = db_session.query(MarketKline).order_by(MarketKline.open_time).all()

    assert summary['mode'] == 'rolling'
    assert summary['success_count'] == 1
    assert calls == [('binance', 'klines', 'BTCUSDT', 2, 900000, 900000)]
    assert [row.open_time for row in rows] == [600000, 900000, 1200000]


def test_exchange_history_repair_marks_non_precise_windows(db_session, monkeypatch):
    calls = []
    adapter = FakeAdapter('okx', ('open_interest_hist',), (), calls)
    monkeypatch.setattr('coinx.collector.exchange_repair.get_exchange_adapters', lambda exchanges: [adapter])

    summary = repair_history_symbols(
        symbols=['BTCUSDT'],
        series_types=['open_interest_hist'],
        exchanges=['okx'],
        now_ms=1500000,
        full_scan=True,
        max_workers=1,
        coverage_hours=1,
        db_session=db_session,
    )

    row = db_session.query(MarketOpenInterestHist).one()

    assert summary['mode'] == 'history'
    assert summary['exchange_summaries']['okx']['results'][0]['window_precise'] is False
    assert calls == [('okx', 'open_interest_hist', 'BTCUSDT', 500, None, None)]
    assert row.exchange == 'okx'
    assert row.event_time == 900000


def test_exchange_rolling_repair_skips_unsupported_symbols(db_session, monkeypatch):
    calls = []
    info_logs = []
    adapter = FakeAdapter('okx', ('klines',), ('klines',), calls, supported_symbols=('BTCUSDT',))
    monkeypatch.setattr('coinx.collector.exchange_repair.get_exchange_adapters', lambda exchanges: [adapter])
    monkeypatch.setattr('coinx.collector.exchange_repair.logger.info', info_logs.append)

    summary = repair_rolling_symbols(
        symbols=['BTCUSDT', 'PROMUSDT'],
        series_types=['klines'],
        exchanges=['okx'],
        now_ms=1500000,
        points=1,
        max_workers=1,
        db_session=db_session,
    )

    skipped = [item for item in summary['results'] if item.get('status') == 'skipped']

    assert summary['failure_count'] == 0
    assert summary['success_count'] == 1
    assert summary['skipped_count'] == 1
    assert skipped[0]['symbol'] == 'PROMUSDT'
    assert skipped[0]['reason'] == 'unsupported_symbol'
    assert calls == [('okx', 'klines', 'BTCUSDT', 2, 1200000, 1200000)]
    assert any('rolling' in message for message in info_logs)
    assert any('PROMUSDT' in message for message in info_logs)


def test_exchange_rolling_repair_falls_back_to_latest_page_when_precise_window_misses(db_session, monkeypatch):
    calls = []

    @dataclass(frozen=True)
    class FallbackAdapter:
        exchange_id: str = 'okx'
        supported_series_types: tuple = ('klines',)
        precise_series_types: tuple = ('klines',)

        def supports_time_window(self, series_type):
            return True

        def supports_symbol(self, symbol, series_type=None, session=None):
            return True

        def periods_for_series(self, series_type):
            return ('5m',)

        def fetch_series_payload(self, series_type, symbol, period, limit, session=None, start_time=None, end_time=None):
            calls.append((symbol, start_time, end_time))
            if start_time is not None or end_time is not None:
                return [
                    {
                        'symbol': symbol,
                        'period': period,
                        'open_time': 900000,
                        'close_time': 1199999,
                        'open_price': 1,
                        'high_price': 2,
                        'low_price': 1,
                        'close_price': 1.5,
                    }
                ]
            return [
                {
                    'symbol': symbol,
                    'period': period,
                    'open_time': 1200000,
                    'close_time': 1499999,
                    'open_price': 1,
                    'high_price': 2,
                    'low_price': 1,
                    'close_price': 1.8,
                }
            ]

        def parse_series_payload(self, series_type, payload, symbol, period):
            return payload

    monkeypatch.setattr('coinx.collector.exchange_repair.get_exchange_adapters', lambda exchanges: [FallbackAdapter()])

    summary = repair_rolling_symbols(
        symbols=['BTCUSDT'],
        series_types=['klines'],
        exchanges=['okx'],
        now_ms=1500000,
        points=1,
        max_workers=1,
        db_session=db_session,
    )

    rows = db_session.query(MarketKline).filter(MarketKline.exchange == 'okx').all()

    assert summary['success_count'] == 1
    assert calls == [('BTCUSDT', 1200000, 1200000), ('BTCUSDT', None, None)]
    assert len(rows) == 1
    assert rows[0].open_time == 1200000


def test_exchange_history_repair_collects_adapter_series_periods(db_session, monkeypatch):
    calls = []
    adapter = FakeAdapter(
        'okx',
        ('taker_buy_sell_vol',),
        ('taker_buy_sell_vol',),
        calls,
        series_period_map={'taker_buy_sell_vol': ('5m', '1H')},
    )
    monkeypatch.setattr('coinx.collector.exchange_repair.get_exchange_adapters', lambda exchanges: [adapter])

    summary = repair_history_symbols(
        symbols=['BTCUSDT'],
        series_types=['taker_buy_sell_vol'],
        exchanges=['okx'],
        now_ms=7_500_000,
        full_scan=True,
        max_workers=1,
        coverage_hours=1,
        db_session=db_session,
    )

    rows = db_session.query(MarketTakerBuySellVol).order_by(MarketTakerBuySellVol.period).all()

    assert summary['mode'] == 'history'
    assert summary['success_count'] == 2
    assert [call[3] for call in calls] == [500, 500]
    assert [call[4] for call in calls] == [3_600_000, 0]
    assert [call[5] for call in calls] == [7_200_000, 3_600_000]
    assert [(row.period, row.event_time) for row in rows] == [('1H', 900000), ('5m', 3600000)]


def test_exchange_rolling_repair_groups_parallelism_by_exchange(db_session, monkeypatch):
    starts = []
    releases = {}
    overlaps = {'seen': False}
    state_lock = threading.Lock()
    started_event = threading.Event()

    @dataclass(frozen=True)
    class ParallelAdapter:
        exchange_id: str

        @property
        def supported_series_types(self):
            return ('klines',)

        def supports_time_window(self, series_type):
            return True

        def supports_symbol(self, symbol, series_type=None, session=None):
            return True

        def periods_for_series(self, series_type):
            return ('5m',)

        def fetch_series_payload(self, series_type, symbol, period, limit, session=None, start_time=None, end_time=None):
            event = releases.setdefault(self.exchange_id, threading.Event())
            with state_lock:
                starts.append(self.exchange_id)
                if len(set(starts)) > 1:
                    overlaps['seen'] = True
                if len(starts) >= 2:
                    started_event.set()
            event.wait(timeout=2)
            return [
                {
                    'symbol': symbol,
                    'period': period,
                    'open_time': start_time or 900000,
                    'close_time': (start_time or 900000) + 299999,
                    'open_price': 1,
                    'high_price': 2,
                    'low_price': 1,
                    'close_price': 1.5,
                }
            ]

        def parse_series_payload(self, series_type, payload, symbol, period):
            return payload

    monkeypatch.setattr(
        'coinx.collector.exchange_repair.get_exchange_adapters',
        lambda exchanges: [ParallelAdapter('binance'), ParallelAdapter('okx')],
    )
    monkeypatch.setattr(
        'coinx.collector.exchange_repair.get_existing_series_timestamps',
        lambda **kwargs: {},
    )
    monkeypatch.setattr(
        'coinx.collector.exchange_repair.upsert_series_records',
        lambda exchange, series_type, records, session=None: len(records),
    )

    result_holder = {}

    worker_thread = threading.Thread(
        target=lambda: result_holder.setdefault(
            'summary',
            repair_rolling_symbols(
                symbols=['BTCUSDT'],
                series_types=['klines'],
                exchanges=['binance', 'okx'],
                now_ms=1500000,
                points=1,
                max_workers=2,
                db_session=None,
            ),
        )
    )
    worker_thread.start()

    assert started_event.wait(timeout=2), 'expected both exchange workers to start'

    releases['binance'].set()
    releases['okx'].set()
    worker_thread.join(timeout=2)

    assert not worker_thread.is_alive()
    assert overlaps['seen'] is True
    assert result_holder['summary']['success_count'] == 2
