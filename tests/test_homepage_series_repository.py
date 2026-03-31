from coinx.repositories.homepage_series import (
    FIVE_MINUTES_MS,
    get_homepage_series_snapshot,
    get_homepage_series_data,
    get_homepage_series_update_time,
    should_refresh_homepage_series,
)
from coinx.models import BinanceKline, BinanceOpenInterestHist


def seed_series(
    session,
    symbol,
    start_time_ms,
    periods,
    oi_base=1000.0,
    oi_step=10.0,
    price_base=100.0,
    price_step=1.0,
    quote_volume_base=1000.0,
    taker_buy_quote_base=600.0,
):
    for index in range(periods):
        event_time = start_time_ms + index * FIVE_MINUTES_MS
        open_price = price_base + index * price_step - 0.5
        close_price = price_base + index * price_step
        session.add(
            BinanceOpenInterestHist(
                symbol=symbol,
                period='5m',
                event_time=event_time,
                sum_open_interest=oi_base + index * oi_step,
                sum_open_interest_value=(oi_base + index * oi_step) * close_price,
                cmc_circulating_supply=None,
                raw_json={},
            )
        )
        session.add(
            BinanceKline(
                symbol=symbol,
                period='5m',
                open_time=event_time,
                close_time=event_time + FIVE_MINUTES_MS - 1,
                open_price=open_price,
                high_price=close_price + 1,
                low_price=open_price - 1,
                close_price=close_price,
                volume=100 + index,
                quote_volume=quote_volume_base + index,
                trade_count=10 + index,
                taker_buy_base_volume=50 + index,
                taker_buy_quote_volume=taker_buy_quote_base + index,
                raw_json=[],
            )
        )
    session.commit()


def test_get_homepage_series_data_builds_coin_payload_from_5m_series(db_session):
    start_time = 1_700_000_000_000
    seed_series(db_session, 'BTCUSDT', start_time, 289)

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    assert len(coins) == 1
    coin = coins[0]

    assert coin['symbol'] == 'BTCUSDT'
    assert coin['current_open_interest'] == 3880.0
    assert coin['current_open_interest_value'] == 1505440.0
    assert coin['current_price'] == 388.0
    assert coin['price_change'] == 288.0
    assert coin['price_change_percent'] == 288.0

    change_15m = coin['changes']['15m']
    assert change_15m['open_interest'] == 3850.0
    assert round(change_15m['ratio'], 2) == round((3880.0 - 3850.0) / 3850.0 * 100, 2)
    assert change_15m['current_price'] == 385.0
    assert change_15m['price_change'] == 3.0
    assert round(change_15m['price_change_percent'], 2) == round(3.0 / 385.0 * 100, 2)

    assert coin['net_inflow']['5m'] == 488.0
    assert coin['net_inflow']['15m'] == 1461.0


def test_get_homepage_series_data_returns_none_for_missing_interval_points(db_session):
    start_time = 1_700_000_000_000
    seed_series(db_session, 'BTCUSDT', start_time, 12)

    missing_target_time = start_time + 8 * FIVE_MINUTES_MS
    missing_window_time = start_time + 9 * FIVE_MINUTES_MS
    db_session.query(BinanceKline).filter(
        BinanceKline.symbol == 'BTCUSDT',
        BinanceKline.period == '5m',
        BinanceKline.open_time.in_([missing_target_time, missing_window_time]),
    ).delete()
    db_session.commit()

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    assert len(coins) == 1
    coin = coins[0]

    assert coin['changes']['15m']['current_price'] is None
    assert coin['changes']['15m']['price_change_percent'] is None
    assert coin['net_inflow']['15m'] is None


def test_get_homepage_series_update_time_uses_min_common_time_across_symbols(db_session):
    start_time = 1_700_000_000_000
    seed_series(db_session, 'BTCUSDT', start_time, 20)
    seed_series(db_session, 'ETHUSDT', start_time, 19)

    update_time = get_homepage_series_update_time(
        symbols=['BTCUSDT', 'ETHUSDT'],
        session=db_session,
    )

    assert update_time == start_time + 18 * FIVE_MINUTES_MS


def test_get_homepage_series_snapshot_returns_data_and_update_time_together(db_session):
    start_time = 1_700_000_000_000
    seed_series(db_session, 'BTCUSDT', start_time, 20)
    seed_series(db_session, 'ETHUSDT', start_time, 19)

    snapshot = get_homepage_series_snapshot(
        symbols=['BTCUSDT', 'ETHUSDT'],
        session=db_session,
    )

    assert len(snapshot['data']) == 2
    assert snapshot['cache_update_time'] == start_time + 18 * FIVE_MINUTES_MS


def test_should_refresh_homepage_series_when_latest_is_current_but_long_history_is_missing(db_session):
    start_time = 1_700_000_000_000
    seed_series(db_session, 'BTCUSDT', start_time, 2017)
    db_session.query(BinanceOpenInterestHist).filter(
        BinanceOpenInterestHist.symbol == 'BTCUSDT',
        BinanceOpenInterestHist.period == '5m',
        BinanceOpenInterestHist.event_time < start_time + 1441 * FIVE_MINUTES_MS,
    ).delete()
    db_session.commit()

    now_ms = start_time + 2016 * FIVE_MINUTES_MS

    assert should_refresh_homepage_series(
        symbols=['BTCUSDT'],
        now_ms=now_ms,
        session=db_session,
    ) is True


def test_should_refresh_homepage_series_skips_when_latest_is_current_and_coverage_is_complete(db_session):
    start_time = 1_700_000_000_000
    seed_series(db_session, 'BTCUSDT', start_time, 2017)

    now_ms = start_time + 2016 * FIVE_MINUTES_MS

    assert should_refresh_homepage_series(
        symbols=['BTCUSDT'],
        now_ms=now_ms,
        session=db_session,
    ) is False
