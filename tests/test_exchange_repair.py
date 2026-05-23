from dataclasses import dataclass
import threading

from coinx.collector.binance.client import (
    BinanceRateLimitUnavailable,
    clear_binance_rate_limit_state,
)
from coinx.collector.bybit.series import BybitRateLimitUnavailable, clear_bybit_rate_limit_state
from coinx.collector.gate import series as gate_series
from coinx.collector.gate.series import GateUnsupportedContract
from coinx.collector.exchange_repair import (
    _flush_group_records,
    repair_history_symbols,
    repair_rolling_symbols,
)
from coinx.collector.okx.series import OKXRateLimitUnavailable, clear_okx_rate_limit_state
from coinx.collector.timing import format_duration_breakdown
from coinx.models import MarketKline, MarketOpenInterestHist, MarketTakerBuySellVol
from coinx.repositories.series import upsert_series_records


FIVE_MINUTES_MS = 5 * 60 * 1000


def _clear_rate_limit_states():
    clear_binance_rate_limit_state()
    clear_bybit_rate_limit_state()
    clear_okx_rate_limit_state()
    gate_series.clear_gate_rate_limit_state()


def _assert_duration_breakdown(summary):
    breakdown = summary['duration_breakdown_ms']
    for key in ('api_ms', 'rate_limit_wait_ms', 'db_read_ms', 'db_write_ms', 'parse_ms', 'other_ms'):
        assert key in breakdown
        assert breakdown[key] >= 0


def test_format_duration_breakdown_uses_friendly_units():
    text = format_duration_breakdown(
        {
            'api_ms': 66164.68,
            'rate_limit_wait_ms': 178734.75,
            'db_read_ms': 4586.63,
            'db_write_ms': 277092.41,
            'parse_ms': 26.58,
            'precheck_ms': 5516.23,
            'other_ms': 0,
        }
    )

    assert 'API=1m6.2s' in text
    assert '限流等待=2m58.7s' in text
    assert '读库=4.59s' in text
    assert '解析=27ms' in text


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
    _clear_rate_limit_states()
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
    _assert_duration_breakdown(summary)
    assert 'binance' in summary['duration_breakdown_by_exchange']
    assert 'klines' in summary['duration_breakdown_by_series_type']
    assert calls == [('binance', 'klines', 'BTCUSDT', 2, 900000, 900000)]
    assert [row.open_time for row in rows] == [600000, 900000, 1200000]


def test_exchange_history_repair_marks_non_precise_windows(db_session, monkeypatch):
    _clear_rate_limit_states()
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
    _assert_duration_breakdown(summary)
    assert 'okx' in summary['duration_breakdown_by_exchange']
    assert 'open_interest_hist' in summary['duration_breakdown_by_series_type']
    assert summary['exchange_summaries']['okx']['results'][0]['window_precise'] is False
    assert calls == [('okx', 'open_interest_hist', 'BTCUSDT', 500, None, None)]
    assert row.exchange == 'okx'
    assert row.event_time == 900000


def test_exchange_rolling_repair_skips_unsupported_symbols(db_session, monkeypatch):
    _clear_rate_limit_states()
    calls = []
    info_logs = []
    adapter = FakeAdapter('okx', ('klines',), ('klines',), calls, supported_symbols=('BTCUSDT',))
    monkeypatch.setattr('coinx.collector.exchange_repair.get_exchange_adapters', lambda exchanges: [adapter])
    monkeypatch.setattr(
        'coinx.collector.exchange_repair.logger.info',
        lambda *args: info_logs.append(' '.join(str(arg) for arg in args)),
    )

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
    assert any('开始修补: 模式=rolling' in message for message in info_logs)
    assert any('预检完成: 模式=rolling' in message for message in info_logs)
    assert any('原因=不支持币种' in message and 'PROMUSDT' in message for message in info_logs)


def test_gate_contract_not_found_is_skipped_as_unsupported(db_session, monkeypatch):
    _clear_rate_limit_states()
    calls = []

    @dataclass(frozen=True)
    class GateNotFoundAdapter:
        exchange_id: str = 'gate'
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
            raise GateUnsupportedContract(f'gate contract not found: {symbol}')

        def parse_series_payload(self, series_type, payload, symbol, period):
            return payload

    monkeypatch.setattr('coinx.collector.exchange_repair.get_exchange_adapters', lambda exchanges: [GateNotFoundAdapter()])

    summary = repair_rolling_symbols(
        symbols=['MSTRUSDT'],
        series_types=['klines'],
        exchanges=['gate'],
        now_ms=1500000,
        points=1,
        max_workers=1,
        db_session=db_session,
    )

    assert summary['failure_count'] == 0
    assert summary['success_count'] == 0
    assert summary['skipped_count'] == 1
    assert summary['results'][0]['reason'] == 'unsupported_symbol'
    assert summary['results'][0]['symbol'] == 'MSTRUSDT'
    assert calls == [('MSTRUSDT', 1200000, 1200000)]


def test_gate_rolling_repair_skips_when_supported_symbol_lookup_fails(db_session, monkeypatch):
    _clear_rate_limit_states()
    calls = []

    @dataclass(frozen=True)
    class GateLookupFailAdapter:
        exchange_id: str = 'gate'
        supported_series_types: tuple = ('klines',)
        precise_series_types: tuple = ('klines',)

        def supports_time_window(self, series_type):
            return True

        def supports_symbol(self, symbol, series_type=None, session=None):
            raise RuntimeError('contracts unavailable')

        def periods_for_series(self, series_type):
            return ('5m',)

        def fetch_series_payload(self, series_type, symbol, period, limit, session=None, start_time=None, end_time=None):
            calls.append((symbol, start_time, end_time))
            return []

        def parse_series_payload(self, series_type, payload, symbol, period):
            return payload

    monkeypatch.setattr('coinx.collector.exchange_repair.get_exchange_adapters', lambda exchanges: [GateLookupFailAdapter()])

    summary = repair_rolling_symbols(
        symbols=['BTCUSDT'],
        series_types=['klines'],
        exchanges=['gate'],
        now_ms=1500000,
        points=1,
        max_workers=1,
        db_session=db_session,
    )

    assert summary['failure_count'] == 0
    assert summary['success_count'] == 0
    assert summary['skipped_count'] == 1
    assert summary['results'][0]['reason'] == 'supported_symbol_lookup_failed'
    assert summary['results'][0]['symbol'] == 'BTCUSDT'
    assert summary['results'][0]['error'] == 'contracts unavailable'
    assert calls == []


def test_gate_history_repair_skips_when_supported_symbol_lookup_fails(db_session, monkeypatch):
    _clear_rate_limit_states()
    calls = []

    @dataclass(frozen=True)
    class GateLookupFailAdapter:
        exchange_id: str = 'gate'
        supported_series_types: tuple = ('klines',)
        precise_series_types: tuple = ('klines',)

        def supports_time_window(self, series_type):
            return True

        def supports_symbol(self, symbol, series_type=None, session=None):
            raise RuntimeError('contracts unavailable')

        def periods_for_series(self, series_type):
            return ('5m',)

        def fetch_series_payload(self, series_type, symbol, period, limit, session=None, start_time=None, end_time=None):
            calls.append((symbol, start_time, end_time))
            return []

        def parse_series_payload(self, series_type, payload, symbol, period):
            return payload

    monkeypatch.setattr('coinx.collector.exchange_repair.get_exchange_adapters', lambda exchanges: [GateLookupFailAdapter()])

    summary = repair_history_symbols(
        symbols=['BTCUSDT'],
        series_types=['klines'],
        exchanges=['gate'],
        now_ms=1500000,
        full_scan=True,
        max_workers=1,
        coverage_hours=1,
        db_session=db_session,
    )

    assert summary['failure_count'] == 0
    assert summary['success_count'] == 0
    assert summary['skipped_count'] == 1
    assert summary['results'][0]['reason'] == 'supported_symbol_lookup_failed'
    assert summary['results'][0]['symbol'] == 'BTCUSDT'
    assert summary['results'][0]['error'] == 'contracts unavailable'
    assert calls == []


def test_exchange_rolling_repair_falls_back_to_latest_page_when_precise_window_misses(db_session, monkeypatch):
    _clear_rate_limit_states()
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
    _clear_rate_limit_states()
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
    _clear_rate_limit_states()
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
    monkeypatch.setattr('coinx.collector.exchange_repair.get_existing_series_timestamps', lambda **kwargs: {})
    monkeypatch.setattr(
        'coinx.collector.exchange_repair.get_session',
        lambda: type('Session', (), {'close': lambda self: None})(),
    )
    monkeypatch.setattr(
        'coinx.collector.exchange_repair.upsert_series_records_in_batches',
        lambda exchange, series_type, records, batch_size, session=None: len(records),
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


def test_exchange_rolling_repair_skips_okx_when_budget_unavailable(db_session, monkeypatch):
    _clear_rate_limit_states()
    @dataclass(frozen=True)
    class LimitedAdapter:
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
            raise OKXRateLimitUnavailable('rubik', 5)

        def parse_series_payload(self, series_type, payload, symbol, period):
            return payload

    monkeypatch.setattr('coinx.collector.exchange_repair.get_exchange_adapters', lambda exchanges: [LimitedAdapter()])

    summary = repair_rolling_symbols(
        symbols=['BTCUSDT'],
        series_types=['klines'],
        exchanges=['okx'],
        now_ms=1500000,
        points=1,
        max_workers=1,
        db_session=db_session,
    )

    assert summary['failure_count'] == 0
    assert summary['skipped_count'] == 1
    assert summary['results'][0]['reason'] == 'okx_budget_unavailable'
    assert summary['results'][0]['duration_breakdown_ms']['cooldown_skip_ms'] == 5000
    assert summary['duration_breakdown_ms']['cooldown_skip_ms'] == 5000


def test_exchange_history_repair_skips_bybit_when_budget_unavailable(db_session, monkeypatch):
    _clear_rate_limit_states()
    @dataclass(frozen=True)
    class LimitedAdapter:
        exchange_id: str = 'bybit'
        supported_series_types: tuple = ('klines',)
        precise_series_types: tuple = ('klines',)

        def supports_time_window(self, series_type):
            return True

        def supports_symbol(self, symbol, series_type=None, session=None):
            return True

        def periods_for_series(self, series_type):
            return ('5m',)

        def fetch_series_payload(self, series_type, symbol, period, limit, session=None, start_time=None, end_time=None):
            raise BybitRateLimitUnavailable('market', 5)

        def parse_series_payload(self, series_type, payload, symbol, period):
            return payload

    monkeypatch.setattr('coinx.collector.exchange_repair.get_exchange_adapters', lambda exchanges: [LimitedAdapter()])

    summary = repair_history_symbols(
        symbols=['BTCUSDT'],
        series_types=['klines'],
        exchanges=['bybit'],
        now_ms=1500000,
        full_scan=True,
        max_workers=1,
        coverage_hours=1,
        db_session=db_session,
    )

    assert summary['failure_count'] == 0
    assert summary['skipped_count'] == 1
    assert summary['results'][0]['reason'] == 'bybit_budget_unavailable'
    assert summary['results'][0]['duration_breakdown_ms']['cooldown_skip_ms'] == 5000
    assert summary['duration_breakdown_ms']['cooldown_skip_ms'] == 5000


def test_exchange_history_repair_skips_binance_when_budget_unavailable(db_session, monkeypatch):
    _clear_rate_limit_states()
    @dataclass(frozen=True)
    class LimitedAdapter:
        exchange_id: str = 'binance'
        supported_series_types: tuple = ('klines',)
        precise_series_types: tuple = ('klines',)

        def supports_time_window(self, series_type):
            return True

        def supports_symbol(self, symbol, series_type=None, session=None):
            return True

        def periods_for_series(self, series_type):
            return ('5m',)

        def fetch_series_payload(self, series_type, symbol, period, limit, session=None, start_time=None, end_time=None):
            raise BinanceRateLimitUnavailable('default', 2)

        def parse_series_payload(self, series_type, payload, symbol, period):
            return payload

    monkeypatch.setattr('coinx.collector.exchange_repair.get_exchange_adapters', lambda exchanges: [LimitedAdapter()])

    summary = repair_history_symbols(
        symbols=['BTCUSDT'],
        series_types=['klines'],
        exchanges=['binance'],
        now_ms=1500000,
        full_scan=True,
        max_workers=1,
        coverage_hours=1,
        db_session=db_session,
    )

    assert summary['failure_count'] == 0
    assert summary['skipped_count'] == 1
    assert summary['results'][0]['reason'] == 'binance_budget_unavailable'


def test_exchange_rolling_repair_logs_budget_unavailable_in_chinese(db_session, monkeypatch):
    _clear_rate_limit_states()
    info_logs = []

    @dataclass(frozen=True)
    class LimitedAdapter:
        exchange_id: str = 'gate'
        supported_series_types: tuple = ('klines',)
        precise_series_types: tuple = ('klines',)

        def supports_time_window(self, series_type):
            return True

        def supports_symbol(self, symbol, series_type=None, session=None):
            return True

        def periods_for_series(self, series_type):
            return ('5m',)

        def fetch_series_payload(self, series_type, symbol, period, limit, session=None, start_time=None, end_time=None):
            raise gate_series.GateRateLimitUnavailable(8)

        def parse_series_payload(self, series_type, payload, symbol, period):
            return payload

    monkeypatch.setattr('coinx.collector.exchange_repair.get_exchange_adapters', lambda exchanges: [LimitedAdapter()])
    monkeypatch.setattr(
        'coinx.collector.exchange_repair.logger.info',
        lambda *args: info_logs.append(' '.join(str(arg) for arg in args)),
    )

    summary = repair_rolling_symbols(
        symbols=['BTCUSDT'],
        series_types=['klines'],
        exchanges=['gate'],
        now_ms=1500000,
        points=1,
        max_workers=1,
        db_session=db_session,
    )

    assert summary['skipped_count'] == 1
    assert any('修补完成: 模式=rolling' in message and '跳过原因=限流冷却中=1' in message for message in info_logs)


def test_exchange_history_repair_logs_fixed_chinese_stage_messages(db_session, monkeypatch):
    _clear_rate_limit_states()
    info_logs = []
    calls = []
    adapter = FakeAdapter('okx', ('open_interest_hist',), (), calls)
    monkeypatch.setattr('coinx.collector.exchange_repair.get_exchange_adapters', lambda exchanges: [adapter])
    monkeypatch.setattr(
        'coinx.collector.exchange_repair.logger.info',
        lambda *args: info_logs.append(' '.join(str(arg) for arg in args)),
    )

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

    assert summary['success_count'] == 1
    assert any('开始修补: 模式=history' in message for message in info_logs)
    assert any('交易所执行开始: 模式=history' in message for message in info_logs)
    assert any('交易所执行完成: 模式=history' in message for message in info_logs)
    assert any('修补完成: 模式=history' in message for message in info_logs)


def test_exchange_group_logs_duration_breakdown(db_session, monkeypatch):
    _clear_rate_limit_states()
    info_logs = []
    calls = []
    adapter = FakeAdapter('binance', ('klines',), ('klines',), calls)
    monkeypatch.setattr('coinx.collector.exchange_repair.get_exchange_adapters', lambda exchanges: [adapter])
    monkeypatch.setattr(
        'coinx.collector.exchange_repair.logger.info',
        lambda *args: info_logs.append((args[0] % args[1:]) if len(args) > 1 else args[0]),
    )

    summary = repair_rolling_symbols(
        symbols=['BTCUSDT'],
        series_types=['klines'],
        exchanges=['binance'],
        now_ms=1500000,
        points=1,
        max_workers=1,
        db_session=db_session,
    )

    assert summary['success_count'] == 1
    assert any(
        '交易所执行完成: 模式=rolling' in message
        and '累计耗时分类=' in message
        and 'API=' in message
        for message in info_logs
    )


def test_exchange_rolling_repair_batches_group_writes(monkeypatch):
    _clear_rate_limit_states()
    calls = []
    upsert_calls = []
    adapter = FakeAdapter('binance', ('klines',), ('klines',), calls)

    monkeypatch.setattr('coinx.collector.exchange_repair.get_exchange_adapters', lambda exchanges: [adapter])
    monkeypatch.setattr('coinx.collector.exchange_repair.get_existing_series_timestamps', lambda **kwargs: {})
    monkeypatch.setattr(
        'coinx.collector.exchange_repair.upsert_series_records_in_batches',
        lambda exchange, series_type, records, batch_size, session=None: upsert_calls.append(
            (exchange, series_type, [record['open_time'] for record in records], batch_size)
        ) or len(records),
    )

    summary = repair_rolling_symbols(
        symbols=['BTCUSDT', 'ETHUSDT'],
        series_types=['klines'],
        exchanges=['binance'],
        now_ms=1500000,
        points=1,
        max_workers=1,
        db_session=None,
    )

    assert summary['success_count'] == 2
    assert len(calls) == 2
    assert upsert_calls == [('binance', 'klines', [1200000, 1200000], 500)]
    assert [item['affected'] for item in summary['results']] == [1, 1]


def test_exchange_history_repair_batches_group_writes(monkeypatch):
    _clear_rate_limit_states()
    calls = []
    upsert_calls = []
    adapter = FakeAdapter('binance', ('klines',), ('klines',), calls)

    monkeypatch.setattr('coinx.collector.exchange_repair.get_exchange_adapters', lambda exchanges: [adapter])
    monkeypatch.setattr(
        'coinx.collector.exchange_repair.upsert_series_records_in_batches',
        lambda exchange, series_type, records, batch_size, session=None: upsert_calls.append(
            (exchange, series_type, [record['open_time'] for record in records], batch_size)
        ) or len(records),
    )

    summary = repair_history_symbols(
        symbols=['BTCUSDT', 'ETHUSDT'],
        series_types=['klines'],
        exchanges=['binance'],
        now_ms=1500000,
        full_scan=True,
        max_workers=1,
        coverage_hours=1,
        db_session=None,
    )

    assert summary['success_count'] == 2
    assert len(calls) == 2
    assert upsert_calls == [('binance', 'klines', [900000, 900000], 2000)]
    assert [item['affected'] for item in summary['results']] == [1, 1]


def test_exchange_repair_grouped_flush_handles_multiple_series(monkeypatch):
    _clear_rate_limit_states()
    upsert_calls = []

    monkeypatch.setattr(
        'coinx.collector.exchange_repair.upsert_series_records_in_batches',
        lambda exchange, series_type, records, batch_size, session=None: upsert_calls.append(
            (exchange, series_type, len(records), batch_size)
        ) or len(records),
    )

    group_results = [
        {
            'series_type': 'klines',
            'pending_records': [{'open_time': 1}],
            'duration_breakdown_ms': {},
        },
        {
            'series_type': 'open_interest_hist',
            'pending_records': [{'event_time': 2}],
            'duration_breakdown_ms': {},
        },
    ]

    flushed = _flush_group_records('binance', group_results, db_session=None)

    assert upsert_calls == [('binance', 'klines', 1, 500), ('binance', 'open_interest_hist', 1, 500)]
    assert all('pending_records' not in item for item in flushed)


def test_exchange_repair_grouped_flush_uses_history_batch_size_and_logs(monkeypatch):
    _clear_rate_limit_states()
    upsert_calls = []
    info_logs = []

    monkeypatch.setattr('coinx.collector.exchange_repair.REPAIR_HISTORY_WRITE_BATCH_SIZE', 2)
    monkeypatch.setattr(
        'coinx.collector.exchange_repair.upsert_series_records_in_batches',
        lambda exchange, series_type, records, batch_size, session=None: upsert_calls.append(
            (exchange, series_type, len(records), batch_size)
        ) or len(records),
    )
    monkeypatch.setattr(
        'coinx.collector.exchange_repair.logger.info',
        lambda *args: info_logs.append(args[0] % args[1:]),
    )

    group_results = [
        {
            'exchange': 'gate',
            'symbol': 'BTCUSDT',
            'series_type': 'klines',
            'pending_records': [{'open_time': 1}, {'open_time': 2}, {'open_time': 3}],
            'duration_breakdown_ms': {},
        },
    ]

    flushed = _flush_group_records('gate', group_results, db_session=None, mode='history')

    assert upsert_calls == [('gate', 'klines', 3, 2)]
    assert any('批量写入开始: 模式=history 交易所=gate 序列类型=klines 记录数=3 batch_size=2' in line for line in info_logs)
    assert all('pending_records' not in item for item in flushed)


def test_gate_history_repair_paginates_open_interest_without_end_time(db_session, monkeypatch):
    _clear_rate_limit_states()

    class GatePagingAdapter:
        exchange_id = 'gate'
        supported_series_types = ('open_interest_hist',)

        def supports_time_window(self, series_type):
            return True

        def supports_symbol(self, symbol, series_type=None, session=None):
            return True

        def periods_for_series(self, series_type):
            return ('5m',)

        def fetch_series_payload(self, series_type, symbol, period, limit, session=None, start_time=None, end_time=None):
            assert series_type == 'open_interest_hist'
            assert end_time is None
            if start_time == 0:
                return [
                    {
                        'symbol': symbol,
                        'period': period,
                        'event_time': index * FIVE_MINUTES_MS,
                        'sum_open_interest': 10 + index,
                        'sum_open_interest_value': 100 + index,
                    }
                    for index in range(1000)
                ]
            if start_time == 1000 * FIVE_MINUTES_MS:
                return [
                    {
                        'symbol': symbol,
                        'period': period,
                        'event_time': index * FIVE_MINUTES_MS,
                        'sum_open_interest': 10 + index,
                        'sum_open_interest_value': 100 + index,
                    }
                    for index in range(1000, 1500)
                ]
            return []

        def parse_series_payload(self, series_type, payload, symbol, period):
            return payload

    monkeypatch.setattr('coinx.collector.exchange_repair.get_exchange_adapters', lambda exchanges: [GatePagingAdapter()])

    summary = repair_history_symbols(
        symbols=['BTCUSDT'],
        series_types=['open_interest_hist'],
        exchanges=['gate'],
        now_ms=1500 * FIVE_MINUTES_MS,
        full_scan=True,
        max_workers=1,
        coverage_hours=(1500 * 5) // 60,
        db_session=db_session,
    )

    rows = (
        db_session.query(MarketOpenInterestHist)
        .filter(
            MarketOpenInterestHist.exchange == 'gate',
            MarketOpenInterestHist.symbol == 'BTCUSDT',
            MarketOpenInterestHist.period == '5m',
        )
        .order_by(MarketOpenInterestHist.event_time)
        .all()
    )

    assert summary['success_count'] == 1
    assert len(rows) == 1500
    assert rows[0].event_time == 0
    assert rows[-1].event_time == 1499 * FIVE_MINUTES_MS


def test_okx_history_repair_paginates_open_interest_backward_from_end_time(db_session, monkeypatch):
    _clear_rate_limit_states()

    class OkxBackwardPagingAdapter:
        exchange_id = 'okx'
        supported_series_types = ('open_interest_hist',)

        def supports_time_window(self, series_type):
            return True

        def supports_symbol(self, symbol, series_type=None, session=None):
            return True

        def periods_for_series(self, series_type):
            return ('5m',)

        def page_limit(self, series_type):
            return 100

        def fetch_series_payload(self, series_type, symbol, period, limit, session=None, start_time=None, end_time=None):
            assert series_type == 'open_interest_hist'
            if (start_time, end_time) == (500 * FIVE_MINUTES_MS, 599 * FIVE_MINUTES_MS):
                return [
                    {
                        'symbol': symbol,
                        'period': period,
                        'event_time': index * FIVE_MINUTES_MS,
                        'sum_open_interest': 10 + index,
                        'sum_open_interest_value': 100 + index,
                    }
                    for index in range(599, 499, -1)
                ]
            if (start_time, end_time) == (400 * FIVE_MINUTES_MS, 499 * FIVE_MINUTES_MS):
                return [
                    {
                        'symbol': symbol,
                        'period': period,
                        'event_time': index * FIVE_MINUTES_MS,
                        'sum_open_interest': 10 + index,
                        'sum_open_interest_value': 100 + index,
                    }
                    for index in range(499, 399, -1)
                ]
            if (start_time, end_time) == (300 * FIVE_MINUTES_MS, 399 * FIVE_MINUTES_MS):
                return [
                    {
                        'symbol': symbol,
                        'period': period,
                        'event_time': index * FIVE_MINUTES_MS,
                        'sum_open_interest': 10 + index,
                        'sum_open_interest_value': 100 + index,
                    }
                    for index in range(399, 299, -1)
                ]
            if (start_time, end_time) == (200 * FIVE_MINUTES_MS, 299 * FIVE_MINUTES_MS):
                return [
                    {
                        'symbol': symbol,
                        'period': period,
                        'event_time': index * FIVE_MINUTES_MS,
                        'sum_open_interest': 10 + index,
                        'sum_open_interest_value': 100 + index,
                    }
                    for index in range(299, 199, -1)
                ]
            if (start_time, end_time) == (100 * FIVE_MINUTES_MS, 199 * FIVE_MINUTES_MS):
                return [
                    {
                        'symbol': symbol,
                        'period': period,
                        'event_time': index * FIVE_MINUTES_MS,
                        'sum_open_interest': 10 + index,
                        'sum_open_interest_value': 100 + index,
                    }
                    for index in range(199, 99, -1)
                ]
            if (start_time, end_time) == (0, 99 * FIVE_MINUTES_MS):
                return [
                    {
                        'symbol': symbol,
                        'period': period,
                        'event_time': index * FIVE_MINUTES_MS,
                        'sum_open_interest': 10 + index,
                        'sum_open_interest_value': 100 + index,
                    }
                    for index in range(99, -1, -1)
                ]
            return []

        def parse_series_payload(self, series_type, payload, symbol, period):
            return payload

    monkeypatch.setattr('coinx.collector.exchange_repair.get_exchange_adapters', lambda exchanges: [OkxBackwardPagingAdapter()])

    summary = repair_history_symbols(
        symbols=['BTCUSDT'],
        series_types=['open_interest_hist'],
        exchanges=['okx'],
        now_ms=600 * FIVE_MINUTES_MS,
        full_scan=True,
        max_workers=1,
        coverage_hours=(600 * 5) // 60,
        db_session=db_session,
    )

    rows = (
        db_session.query(MarketOpenInterestHist)
        .filter(
            MarketOpenInterestHist.exchange == 'okx',
            MarketOpenInterestHist.symbol == 'BTCUSDT',
            MarketOpenInterestHist.period == '5m',
        )
        .order_by(MarketOpenInterestHist.event_time)
        .all()
    )

    assert summary['success_count'] == 1
    assert len(rows) == 600
    assert rows[0].event_time == 0
    assert rows[-1].event_time == 599 * FIVE_MINUTES_MS
