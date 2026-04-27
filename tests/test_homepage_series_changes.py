import pytest

from coinx.models import BinanceKline, BinanceOpenInterestHist, BinanceTakerBuySellVol
from coinx.repositories.homepage_series import FIVE_MINUTES_MS, get_homepage_series_data
from homepage_contracts import (
    EXPECTED_INTERVALS,
    START_TIME_MS,
    assert_complete_interval_contract,
    seed_complete_homepage_series,
)


def test_homepage_series_returns_full_interval_contract(db_session):
    seed_complete_homepage_series(db_session)

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    assert len(coins) == 1
    assert_complete_interval_contract(coins[0])


def test_homepage_series_contract_fails_when_168h_source_window_is_broken(db_session):
    seed_complete_homepage_series(db_session)

    damaged_times = [START_TIME_MS + index * FIVE_MINUTES_MS for index in range(11)]
    db_session.query(BinanceOpenInterestHist).filter(
        BinanceOpenInterestHist.symbol == 'BTCUSDT',
        BinanceOpenInterestHist.period == '5m',
        BinanceOpenInterestHist.event_time.in_(damaged_times),
    ).delete(synchronize_session=False)
    db_session.query(BinanceKline).filter(
        BinanceKline.symbol == 'BTCUSDT',
        BinanceKline.period == '5m',
        BinanceKline.open_time.in_(damaged_times),
    ).delete(synchronize_session=False)
    db_session.query(BinanceTakerBuySellVol).filter(
        BinanceTakerBuySellVol.symbol == 'BTCUSDT',
        BinanceTakerBuySellVol.period == '5m',
        BinanceTakerBuySellVol.event_time.in_(damaged_times),
    ).delete(synchronize_session=False)
    db_session.commit()

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    assert len(coins) == 1
    with pytest.raises(AssertionError):
        assert_complete_interval_contract(coins[0])
