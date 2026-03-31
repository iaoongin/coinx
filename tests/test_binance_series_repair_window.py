from coinx.collector.binance.repair import (
    build_repair_window,
    floor_to_completed_5m,
    get_latest_series_timestamp,
)
from coinx.repositories.binance_series import upsert_series_records


def test_floor_to_completed_5m_aligns_timestamp():
    assert floor_to_completed_5m(601234) == 600000
    assert floor_to_completed_5m(600000) == 600000


def test_get_latest_series_timestamp_returns_none_for_empty_table(db_session):
    assert get_latest_series_timestamp('BTCUSDT', 'klines', session=db_session) is None


def test_get_latest_series_timestamp_uses_kline_open_time(db_session):
    upsert_series_records(
        'klines',
        [
            {
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
            },
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'open_time': 600000,
                'close_time': 899999,
                'open_price': 2,
                'high_price': 3,
                'low_price': 2,
                'close_price': 3,
                'volume': 10,
                'quote_volume': 20,
                'trade_count': 3,
                'taker_buy_base_volume': 4,
                'taker_buy_quote_volume': 5,
                'raw_json': [],
            },
        ],
        session=db_session,
    )

    assert get_latest_series_timestamp('BTCUSDT', 'klines', session=db_session) == 600000


def test_get_latest_series_timestamp_uses_event_time_for_ratio_series(db_session):
    upsert_series_records(
        'top_long_short_position_ratio',
        [
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'event_time': 300000,
                'long_short_ratio': 1.1,
                'long_account': 0.6,
                'short_account': 0.4,
                'raw_json': {},
            },
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'event_time': 600000,
                'long_short_ratio': 1.2,
                'long_account': 0.62,
                'short_account': 0.38,
                'raw_json': {},
            },
        ],
        session=db_session,
    )

    assert (
        get_latest_series_timestamp(
            'BTCUSDT',
            'top_long_short_position_ratio',
            session=db_session,
        )
        == 600000
    )


def test_build_repair_window_bootstraps_recent_seven_days_when_empty(db_session, monkeypatch):
    monkeypatch.setattr(
        'coinx.collector.binance.repair.BINANCE_SERIES_REPAIR_BOOTSTRAP_DAYS',
        7,
    )

    window = build_repair_window(
        symbol='BTCUSDT',
        series_type='klines',
        now_ms=600000,
        session=db_session,
    )

    assert window['has_gap'] is True
    assert window['end_time'] == 600000
    assert window['start_time'] == 600000 - 7 * 24 * 60 * 60 * 1000


def test_build_repair_window_starts_after_latest_local_record(db_session):
    upsert_series_records(
        'open_interest_hist',
        [
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'event_time': 300000,
                'sum_open_interest': 100,
                'sum_open_interest_value': 200,
                'cmc_circulating_supply': 300,
                'raw_json': {},
            }
        ],
        session=db_session,
    )

    window = build_repair_window(
        symbol='BTCUSDT',
        series_type='open_interest_hist',
        now_ms=900000,
        session=db_session,
    )

    assert window['has_gap'] is True
    assert window['start_time'] == 600000
    assert window['end_time'] == 900000


def test_build_repair_window_returns_no_gap_when_series_is_caught_up(db_session):
    upsert_series_records(
        'global_long_short_account_ratio',
        [
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'event_time': 900000,
                'long_short_ratio': 1.3,
                'long_account': 0.58,
                'short_account': 0.42,
                'raw_json': {},
            }
        ],
        session=db_session,
    )

    window = build_repair_window(
        symbol='BTCUSDT',
        series_type='global_long_short_account_ratio',
        now_ms=900000,
        session=db_session,
    )

    assert window['has_gap'] is False
    assert window['start_time'] == 1200000
    assert window['end_time'] == 900000
