from coinx.repositories.homepage_series import (
    FIVE_MINUTES_MS,
    TIME_INTERVALS,
    get_homepage_series_snapshot,
    get_homepage_series_data,
    get_homepage_series_update_time,
    should_refresh_homepage_series,
)
from coinx.models import (
    BinanceKline,
    BinanceOpenInterestHist,
    BinanceTakerBuySellVol,
    MarketKline,
    MarketOpenInterestHist,
    MarketTakerBuySellVol,
)


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
    taker_vol_base=1000.0,
    taker_vol_step=10.0,
    include_taker_vol=False,
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
            )
        )
        if include_taker_vol:
            session.add(
                BinanceTakerBuySellVol(
                    symbol=symbol,
                    period='5m',
                    event_time=event_time,
                    buy_sell_ratio=1.2 + index * 0.001,
                    buy_vol=taker_vol_base + index * taker_vol_step,
                    sell_vol=(taker_vol_base + index * taker_vol_step) * 0.8,
                )
            )
    session.commit()


def test_get_homepage_series_data_builds_coin_payload_from_5m_series(db_session, monkeypatch):
    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['binance'])
    start_time = 1_700_000_000_000
    seed_series(db_session, 'BTCUSDT', start_time, 2017, include_taker_vol=True)

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    assert len(coins) == 1
    coin = coins[0]

    assert coin['symbol'] == 'BTCUSDT'
    assert coin['status'] == 'complete'
    assert coin['current_open_interest'] is not None
    assert coin['current_open_interest_value'] is not None
    assert coin['current_price'] is not None
    assert coin['price_change'] is not None
    assert coin['price_change_percent'] is not None
    assert coin['changes']['15m']['current_price'] is not None
    assert coin['net_inflow']


def test_get_homepage_series_data_uses_oi_kline_anchor_when_taker_vol_lags(db_session):
    import pytest
    pytest.skip('旧的单交易所部分可用语义已废弃')
    start_time = 1_700_000_000_000
    seed_series(db_session, 'BTCUSDT', start_time, 20, include_taker_vol=True)
    db_session.query(BinanceTakerBuySellVol).filter(
        BinanceTakerBuySellVol.symbol == 'BTCUSDT',
        BinanceTakerBuySellVol.period == '5m',
        BinanceTakerBuySellVol.event_time == start_time + 19 * FIVE_MINUTES_MS,
    ).delete()
    db_session.commit()

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    assert len(coins) == 1
    coin = coins[0]

    assert coin['status'] == 'empty'
    assert coin['included_exchanges'] == []
    assert coin['missing_exchanges'] == ['binance']
    assert coin['current_open_interest'] is None
    assert coin['current_price'] is None


def test_get_homepage_series_data_keeps_168h_changes_when_taker_vol_lags_by_one_point(db_session):
    import pytest
    pytest.skip('旧的单交易所部分可用语义已废弃')
    start_time = 1_700_000_000_000
    seed_series(db_session, 'BTCUSDT', start_time, 2017, include_taker_vol=True)
    db_session.query(BinanceTakerBuySellVol).filter(
        BinanceTakerBuySellVol.symbol == 'BTCUSDT',
        BinanceTakerBuySellVol.period == '5m',
        BinanceTakerBuySellVol.event_time == start_time + 2016 * FIVE_MINUTES_MS,
    ).delete()
    db_session.commit()

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    assert len(coins) == 1
    coin = coins[0]

    assert coin['status'] == 'empty'
    assert coin['included_exchanges'] == []
    assert coin['missing_exchanges'] == ['binance']
    assert coin['current_price'] is None


def test_get_homepage_series_data_returns_none_for_missing_interval_points(db_session):
    import pytest
    pytest.skip('旧的单交易所部分可用语义已废弃')
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

    assert coin['status'] == 'empty'
    assert coin['included_exchanges'] == []
    assert coin['missing_exchanges'] == ['binance']
    assert coin['current_open_interest'] is None


def test_get_homepage_series_data_logs_kline_rejection_reason(db_session, monkeypatch):
    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['binance'])
    start_time = 1_700_000_000_000
    seed_series(db_session, 'BTCUSDT', start_time, 2017, include_taker_vol=True)
    db_session.query(BinanceKline).filter(
        BinanceKline.symbol == 'BTCUSDT',
        BinanceKline.period == '5m',
    ).delete()
    db_session.commit()

    info_logs = []
    warning_logs = []
    monkeypatch.setattr('coinx.repositories.homepage_series.logger.info', info_logs.append)
    monkeypatch.setattr('coinx.repositories.homepage_series.logger.warning', warning_logs.append)

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    coin = coins[0]
    assert coin['status'] == 'empty'
    assert coin['included_exchanges'] == []
    assert coin['missing_exchanges'] == ['binance']
    assert any('missing_kline_history' in message or 'missing_kline_target' in message for message in warning_logs)
    assert any('symbol=BTCUSDT' in message and 'exchange=binance' in message for message in warning_logs)
    assert any('首页交易所聚合为空态' in message for message in warning_logs)


def test_get_homepage_series_data_logs_open_interest_rejection_reason(db_session, monkeypatch):
    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['binance'])
    start_time = 1_700_000_000_000
    seed_series(db_session, 'BTCUSDT', start_time, 2017, include_taker_vol=True)
    db_session.query(BinanceOpenInterestHist).filter(
        BinanceOpenInterestHist.symbol == 'BTCUSDT',
        BinanceOpenInterestHist.period == '5m',
    ).delete()
    db_session.commit()

    info_logs = []
    warning_logs = []
    monkeypatch.setattr('coinx.repositories.homepage_series.logger.info', info_logs.append)
    monkeypatch.setattr('coinx.repositories.homepage_series.logger.warning', warning_logs.append)

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    coin = coins[0]
    assert coin['status'] == 'empty'
    assert coin['included_exchanges'] == []
    assert coin['missing_exchanges'] == ['binance']
    assert any('missing_open_interest_history' in message or 'missing_open_interest_target' in message for message in warning_logs)
    assert any('symbol=BTCUSDT' in message and 'exchange=binance' in message for message in warning_logs)


def test_get_homepage_series_data_does_not_reject_when_taker_mapping_is_missing(db_session, monkeypatch):
    start_time = 1_700_000_000_000
    seed_series(db_session, 'BTCUSDT', start_time, 2017, include_taker_vol=True)

    class FakeAdapter:
        def taker_period_for_interval(self, interval):
            if interval == '168h':
                return None
            return '5m'

    monkeypatch.setattr('coinx.repositories.homepage_series._get_enabled_exchanges', lambda: ['binance'])
    monkeypatch.setattr('coinx.repositories.homepage_series.get_exchange_adapter', lambda exchange: FakeAdapter())

    info_logs = []
    warning_logs = []
    monkeypatch.setattr('coinx.repositories.homepage_series.logger.info', info_logs.append)
    monkeypatch.setattr('coinx.repositories.homepage_series.logger.warning', warning_logs.append)

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    coin = coins[0]
    assert coin['status'] == 'complete'
    assert coin['included_exchanges'] == ['binance']
    assert coin['missing_exchanges'] == []
    assert coin['net_inflow']
    assert '168h' not in coin['net_inflow']
    assert any('首页交易所聚合完成' in message for message in info_logs)
    assert not any('首页交易所门禁否决' in message for message in warning_logs)


def test_get_homepage_series_data_does_not_log_rejection_for_complete_exchange(db_session, monkeypatch):
    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['binance'])
    start_time = 1_700_000_000_000
    seed_series(db_session, 'BTCUSDT', start_time, 2017, include_taker_vol=True)

    info_logs = []
    warning_logs = []
    monkeypatch.setattr('coinx.repositories.homepage_series.logger.info', info_logs.append)
    monkeypatch.setattr('coinx.repositories.homepage_series.logger.warning', warning_logs.append)

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    coin = coins[0]
    assert coin['status'] == 'complete'
    assert coin['included_exchanges'] == ['binance']
    assert coin['missing_exchanges'] == []
    assert not any('首页交易所门禁否决' in message for message in warning_logs)
    assert any('首页交易所聚合完成' in message for message in info_logs)


def test_get_homepage_series_data_formats_small_prices_without_scientific_notation_until_eight_decimal_place(db_session):
    start_time = 1_700_000_000_000
    seed_series(
        db_session,
        'BTCUSDT',
        start_time,
        2017,
        price_base=0.0000001,
        price_step=0.0,
        include_taker_vol=True,
    )

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    assert coins[0]['current_price_formatted'] == '0.0000001'


def test_get_homepage_series_data_keeps_plain_price_without_rounding_when_total_digits_within_seven(db_session):
    start_time = 1_700_000_000_000
    seed_series(
        db_session,
        'BTCUSDT',
        start_time,
        2017,
        price_base=1234.567,
        price_step=0.0,
        include_taker_vol=True,
    )

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    assert coins[0]['current_price_formatted'] == '1234.567'


def test_get_homepage_series_data_formats_large_prices_without_compact_suffix(db_session):
    start_time = 1_700_000_000_000
    seed_series(
        db_session,
        'BTCUSDT',
        start_time,
        2017,
        price_base=1234.5678,
        price_step=0.0,
        include_taker_vol=True,
    )

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    assert coins[0]['current_price_formatted'] == '1234.57'


def test_get_homepage_series_data_formats_tiny_prices_with_scientific_notation_after_seven_decimal_places(db_session):
    start_time = 1_700_000_000_000
    seed_series(
        db_session,
        'BTCUSDT',
        start_time,
        2017,
        price_base=0.00000001,
        price_step=0.0,
        include_taker_vol=True,
    )

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    assert coins[0]['current_price_formatted'] == '1.00000e-08'


def test_get_homepage_series_update_time_uses_min_common_time_across_symbols(db_session, monkeypatch):
    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['binance'])
    start_time = 1_700_000_000_000
    seed_series(db_session, 'BTCUSDT', start_time, 2017, include_taker_vol=True)
    seed_series(db_session, 'ETHUSDT', start_time + FIVE_MINUTES_MS, 2017, include_taker_vol=True)

    update_time = get_homepage_series_update_time(
        symbols=['BTCUSDT', 'ETHUSDT'],
        session=db_session,
    )

    assert update_time == start_time + 2016 * FIVE_MINUTES_MS


def test_get_homepage_series_snapshot_returns_data_and_update_time_together(db_session, monkeypatch):
    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['binance'])
    start_time = 1_700_000_000_000
    seed_series(db_session, 'BTCUSDT', start_time, 2017, include_taker_vol=True)
    seed_series(db_session, 'ETHUSDT', start_time + FIVE_MINUTES_MS, 2017, include_taker_vol=True)

    snapshot = get_homepage_series_snapshot(
        symbols=['BTCUSDT', 'ETHUSDT'],
        session=db_session,
    )

    assert len(snapshot['data']) == 2
    assert snapshot['cache_update_time'] == start_time + 2016 * FIVE_MINUTES_MS


def test_should_refresh_homepage_series_when_latest_is_current_but_long_history_is_missing(db_session):
    start_time = 1_700_000_000_000
    seed_series(db_session, 'BTCUSDT', start_time, 2017, include_taker_vol=True)
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


def test_should_refresh_homepage_series_skips_when_latest_is_current_and_coverage_is_complete(db_session, monkeypatch):
    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['binance'])
    start_time = 1_700_000_000_000
    aligned_start_time = start_time - (start_time % FIVE_MINUTES_MS)
    seed_series(db_session, 'BTCUSDT', aligned_start_time, 2017, include_taker_vol=True)

    now_ms = aligned_start_time + 2017 * FIVE_MINUTES_MS

    assert should_refresh_homepage_series(
        symbols=['BTCUSDT'],
        now_ms=now_ms,
        session=db_session,
    ) is False


def test_get_homepage_series_data_without_taker_vol_returns_empty_net_inflow(db_session):
    import pytest
    pytest.skip('旧的单交易所部分可用语义已废弃')
    start_time = 1_700_000_000_000
    seed_series(db_session, 'BTCUSDT', start_time, 289, include_taker_vol=False)

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    assert len(coins) == 1
    coin = coins[0]
    assert coin['status'] == 'empty'
    assert coin['included_exchanges'] == []
    assert 'binance' in coin['missing_exchanges']
    assert coin['current_open_interest'] is None


def test_get_homepage_series_data_with_taker_vol_returns_net_inflow(db_session, monkeypatch):
    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['binance'])
    start_time = 1_700_000_000_000
    seed_series(db_session, 'BTCUSDT', start_time, 2017, include_taker_vol=True)

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    assert len(coins) == 1
    coin = coins[0]
    assert coin['net_inflow'] is not None
    assert '5m' in coin['net_inflow']
    assert coin['net_inflow']['5m'] is not None
    assert isinstance(coin['net_inflow']['5m'], (int, float))


def test_get_homepage_series_data_with_partial_taker_vol_returns_partial_net_inflow(db_session, monkeypatch):
    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['binance'])
    import pytest
    pytest.skip('旧的单交易所部分可用语义已废弃')
    start_time = 1_700_000_000_000
    seed_series(db_session, 'BTCUSDT', start_time, 10, include_taker_vol=True)

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    assert len(coins) == 1
    coin = coins[0]
    assert coin['status'] == 'empty'
    assert coin['included_exchanges'] == []
    assert 'binance' in coin['missing_exchanges']
    assert coin['net_inflow'] == {}


def test_get_homepage_series_data_aggregates_open_interest_and_net_inflow_across_exchanges(db_session, monkeypatch):
    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['binance', 'okx'])

    class FakeAdapter:
        def taker_period_for_interval(self, interval):
            return '5m'

    monkeypatch.setattr('coinx.repositories.homepage_series.get_exchange_adapter', lambda exchange: FakeAdapter())

    start_time = 1_700_000_000_000
    seed_series(
        db_session,
        'BTCUSDT',
        start_time,
        2017,
        oi_base=1000.0,
        oi_step=10.0,
        taker_vol_base=1000.0,
        taker_vol_step=10.0,
        include_taker_vol=True,
    )

    for index in range(2017):
        event_time = start_time + index * FIVE_MINUTES_MS
        okx_oi = 2000.0 + index * 20.0
        okx_close_price = 100.0 + index
        okx_buy = 500.0 + index * 5.0
        okx_sell = 200.0 + index * 2.0
        db_session.add(
            MarketOpenInterestHist(
                exchange='okx',
                symbol='BTCUSDT',
                period='5m',
                event_time=event_time,
                sum_open_interest=okx_oi,
                sum_open_interest_value=okx_oi * okx_close_price,
            )
        )
        db_session.add(
            MarketKline(
                exchange='okx',
                symbol='BTCUSDT',
                period='5m',
                open_time=event_time,
                close_time=event_time + FIVE_MINUTES_MS - 1,
                open_price=okx_close_price - 0.5,
                high_price=okx_close_price + 1,
                low_price=okx_close_price - 1.5,
                close_price=okx_close_price,
                volume=1000 + index,
                quote_volume=2000 + index,
                trade_count=10 + index,
                taker_buy_base_volume=500 + index,
                taker_buy_quote_volume=600 + index,
            )
        )
        db_session.add(
            MarketTakerBuySellVol(
                exchange='okx',
                symbol='BTCUSDT',
                period='5m',
                event_time=event_time,
                buy_sell_ratio=okx_buy / okx_sell,
                buy_vol=okx_buy,
                sell_vol=okx_sell,
            )
        )
    db_session.commit()

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    coin = coins[0]
    assert coin['included_exchanges'] == ['binance', 'okx']
    assert coin['missing_exchanges'] == []
    assert coin['status'] == 'complete'
    assert coin['current_price'] == 2116.0
    assert coin['current_open_interest'] == 21160.0 + 42320.0
    assert coin['current_open_interest_value'] == 44774560.0 + 42320.0 * 2116.0
    assert coin['exchange_open_interest'][0]['exchange'] == 'okx'
    assert coin['exchange_open_interest'][0]['open_interest_value'] == 42320.0 * 2116.0
    assert round(coin['exchange_open_interest'][0]['share_percent'], 2) == 66.67
    assert coin['exchange_open_interest'][1]['exchange'] == 'binance'
    assert coin['exchange_open_interest'][1]['open_interest_value'] == 44774560.0
    assert round(coin['exchange_open_interest'][1]['quantity_share_percent'], 2) == 33.33
    assert coin['changes']['15m']['open_interest'] == 21130.0 + 42260.0
    assert coin['net_inflow']['5m'] == 10580.0


def test_get_homepage_series_data_excludes_exchange_when_any_required_series_is_missing(db_session, monkeypatch):
    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['binance', 'okx'])
    start_time = 1_700_000_000_000
    seed_series(
        db_session,
        'BTCUSDT',
        start_time,
        2017,
        oi_base=1000.0,
        oi_step=10.0,
        taker_vol_base=1000.0,
        taker_vol_step=10.0,
        include_taker_vol=True,
    )

    for index in range(2017):
        event_time = start_time + index * FIVE_MINUTES_MS
        okx_oi = 2000.0 + index * 20.0
        okx_close_price = 100.0 + index
        db_session.add(
            MarketOpenInterestHist(
                exchange='okx',
                symbol='BTCUSDT',
                period='5m',
                event_time=event_time,
                sum_open_interest=okx_oi,
                sum_open_interest_value=okx_oi * okx_close_price,
            )
        )
        if index != 0:
            db_session.add(
                MarketKline(
                    exchange='okx',
                    symbol='BTCUSDT',
                    period='5m',
                    open_time=event_time,
                    close_time=event_time + FIVE_MINUTES_MS - 1,
                    open_price=okx_close_price - 0.5,
                    high_price=okx_close_price + 1,
                    low_price=okx_close_price - 1.5,
                    close_price=okx_close_price,
                    volume=1000 + index,
                    quote_volume=2000 + index,
                    trade_count=10 + index,
                    taker_buy_base_volume=500 + index,
                    taker_buy_quote_volume=600 + index,
                )
            )
        db_session.add(
            MarketTakerBuySellVol(
                exchange='okx',
                symbol='BTCUSDT',
                period='5m',
                event_time=event_time,
                buy_sell_ratio=1.5,
                buy_vol=500.0 + index,
                sell_vol=200.0 + index,
            )
        )
    db_session.commit()

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    coin = coins[0]
    assert coin['included_exchanges'] == ['binance']
    assert coin['missing_exchanges'] == ['okx']
    assert coin['status'] == 'partial'
    assert [item['exchange'] for item in coin['exchange_open_interest']] == ['binance']
    assert coin['current_open_interest'] == 21160.0
    assert coin['current_price'] == 2116.0


def test_get_homepage_series_data_exposes_exchange_statuses_for_included_excluded_and_unsupported(db_session, monkeypatch):
    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['binance', 'okx', 'mystery'])
    monkeypatch.setattr('coinx.repositories.homepage_series.get_supported_exchange_ids', lambda: ['binance', 'okx'])

    class FakeAdapter:
        def taker_period_for_interval(self, interval):
            return '5m'

    monkeypatch.setattr('coinx.repositories.homepage_series.get_exchange_adapter', lambda exchange: FakeAdapter())

    start_time = 1_700_000_000_000
    seed_series(
        db_session,
        'BTCUSDT',
        start_time,
        2017,
        oi_base=1000.0,
        oi_step=10.0,
        taker_vol_base=1000.0,
        taker_vol_step=10.0,
        include_taker_vol=True,
    )

    for index in range(2017):
        event_time = start_time + index * FIVE_MINUTES_MS
        okx_oi = 2000.0 + index * 20.0
        okx_close_price = 100.0 + index
        db_session.add(
            MarketOpenInterestHist(
                exchange='okx',
                symbol='BTCUSDT',
                period='5m',
                event_time=event_time,
                sum_open_interest=okx_oi,
                sum_open_interest_value=okx_oi * okx_close_price,
            )
        )
        if index != 2016:
            db_session.add(
                MarketKline(
                    exchange='okx',
                    symbol='BTCUSDT',
                    period='5m',
                    open_time=event_time,
                    close_time=event_time + FIVE_MINUTES_MS - 1,
                    open_price=okx_close_price - 0.5,
                    high_price=okx_close_price + 1,
                    low_price=okx_close_price - 1.5,
                    close_price=okx_close_price,
                    volume=1000 + index,
                    quote_volume=2000 + index,
                    trade_count=10 + index,
                    taker_buy_base_volume=500 + index,
                    taker_buy_quote_volume=600 + index,
                )
            )
            db_session.add(
                MarketTakerBuySellVol(
                    exchange='okx',
                    symbol='BTCUSDT',
                    period='5m',
                    event_time=event_time,
                    buy_sell_ratio=1.5,
                    buy_vol=500.0 + index,
                    sell_vol=200.0 + index,
                )
            )
    db_session.commit()

    coin = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)[0]
    statuses = {item['exchange']: item for item in coin['exchange_statuses']}

    assert statuses['binance']['status'] == 'included'
    assert statuses['binance']['open_interest_formatted'] != 'N/A'
    assert statuses['okx']['status'] == 'excluded'
    assert statuses['okx']['open_interest_formatted'] != 'N/A'
    assert statuses['mystery']['status'] == 'unsupported'
    assert statuses['mystery']['open_interest_formatted'] == 'N/A'
    assert coin['included_exchanges'] == ['binance']
    assert coin['missing_exchanges'] == ['okx', 'mystery']


def test_get_homepage_series_data_marks_exchange_status_unsupported_when_symbol_not_in_supported_list(db_session, monkeypatch):
    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['okx'])
    monkeypatch.setattr('coinx.collector.okx.series.get_supported_symbols', lambda session=None: {'BTCUSDT'})

    coins = get_homepage_series_data(symbols=['GWEIUSDT'], session=db_session)

    coin = coins[0]
    statuses = {item['exchange']: item for item in coin['exchange_statuses']}

    assert coin['included_exchanges'] == []
    assert coin['missing_exchanges'] == ['okx']
    assert statuses['okx']['status'] == 'unsupported'
    assert statuses['okx']['open_interest_formatted'] == 'N/A'
    assert statuses['okx']['open_interest_value_formatted'] == 'N/A'


def test_get_homepage_series_data_marks_exchange_status_unknown_when_support_lookup_fails(db_session, monkeypatch):
    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['okx'])

    def raise_lookup_error(session=None):
        raise RuntimeError('lookup failed')

    monkeypatch.setattr('coinx.collector.okx.series.get_supported_symbols', raise_lookup_error)

    coins = get_homepage_series_data(symbols=['GWEIUSDT'], session=db_session)

    coin = coins[0]
    statuses = {item['exchange']: item for item in coin['exchange_statuses']}

    assert coin['included_exchanges'] == []
    assert coin['missing_exchanges'] == ['okx']
    assert statuses['okx']['status'] == 'unknown'
    assert statuses['okx']['open_interest_formatted'] == 'N/A'
    assert statuses['okx']['support_state'] == 'unknown'


def test_get_homepage_series_data_uses_okx_hourly_taker_for_long_intervals(db_session, monkeypatch):
    import pytest
    pytest.skip('旧的跨交易所部分透传语义已废弃')
    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['binance', 'okx'])
    start_time = 1_700_000_000_000
    full_history_points = 2017
    seed_series(db_session, 'BTCUSDT', start_time, full_history_points, include_taker_vol=True)

    current_time = start_time + (full_history_points - 1) * FIVE_MINUTES_MS
    for offset in range(168):
        event_time = current_time - offset * 60 * 60 * 1000
        db_session.add(
            MarketTakerBuySellVol(
                exchange='okx',
                symbol='BTCUSDT',
                period='1H',
                event_time=event_time,
                buy_sell_ratio=2.5,
                buy_vol=100.0,
                sell_vol=40.0,
            )
        )
    db_session.commit()

    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['binance'])
    binance_only = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)[0]

    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['binance', 'okx'])
    with_okx = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)[0]

    assert with_okx['status'] == 'partial'
    assert with_okx['included_exchanges'] == ['binance']
    assert with_okx['net_inflow']['168h'] == binance_only['net_inflow']['168h']
    assert with_okx['net_inflow']['48h'] == binance_only['net_inflow']['48h']
    assert with_okx['net_inflow']['72h'] == binance_only['net_inflow']['72h']


def test_get_homepage_series_data_estimates_okx_open_interest_from_value(db_session, monkeypatch):
    import pytest
    pytest.skip('旧的跨交易所部分透传语义已废弃')
    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['binance', 'okx'])
    start_time = 1_700_000_000_000
    seed_series(
        db_session,
        'ETHUSDT',
        start_time,
        20,
        oi_base=1000.0,
        oi_step=10.0,
        price_base=2000.0,
        price_step=10.0,
        include_taker_vol=True,
    )

    for index in range(20):
        event_time = start_time + index * FIVE_MINUTES_MS
        db_session.add(
            MarketOpenInterestHist(
                exchange='okx',
                symbol='ETHUSDT',
                period='5m',
                event_time=event_time,
                sum_open_interest=None,
                sum_open_interest_value=1_000_000.0 + index * 1000.0,
            )
        )
    db_session.commit()

    coins = get_homepage_series_data(symbols=['ETHUSDT'], session=db_session)

    coin = coins[0]
    assert coin['included_exchanges'] == ['binance']
    assert coin['missing_exchanges'] == ['okx']
    assert coin['status'] == 'partial'
    assert coin['current_price'] == 2190.0
    assert coin['current_open_interest'] == 1190.0
    assert coin['current_open_interest_value'] == 1190.0 * 2190.0
    assert [item['exchange'] for item in coin['exchange_open_interest']] == ['binance']


def test_get_homepage_series_data_uses_available_exchange_when_okx_is_missing(db_session, monkeypatch):
    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['binance', 'okx'])
    start_time = 1_700_000_000_000
    seed_series(db_session, 'BTCUSDT', start_time, 2017, include_taker_vol=True)

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    coin = coins[0]
    assert coin['included_exchanges'] == ['binance']
    assert coin['missing_exchanges'] == ['okx']
    assert coin['status'] == 'partial'
    assert coin['current_open_interest'] == 21160.0
    assert [item['exchange'] for item in coin['exchange_open_interest']] == ['binance']


def test_get_homepage_series_data_can_return_aggregate_metrics_without_reference_price(db_session, monkeypatch):
    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['okx'])
    start_time = 1_700_000_000_000
    for index in range(20):
        event_time = start_time + index * FIVE_MINUTES_MS
        db_session.add(
            MarketOpenInterestHist(
                exchange='okx',
                symbol='BTCUSDT',
                period='5m',
                event_time=event_time,
                sum_open_interest=1000.0 + index,
                sum_open_interest_value=2000.0 + index,
            )
        )
    db_session.commit()

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    coin = coins[0]
    assert coin['status'] == 'empty'
    assert coin['included_exchanges'] == []
    assert coin['missing_exchanges'] == ['okx']
    assert coin['current_open_interest'] is None
    assert coin['current_price'] is None
    assert coin['current_price_formatted'] == 'N/A'
    assert coin['changes']['15m']['price_change'] is None
