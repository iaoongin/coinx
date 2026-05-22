import pytest

from coinx.models import MarketKline, MarketOpenInterestHist, MarketTakerBuySellVol
from coinx.repositories.homepage_series import FIVE_MINUTES_MS, get_homepage_series_data
from homepage_contracts import (
    EXPECTED_INTERVALS,
    START_TIME_MS,
    assert_complete_interval_contract,
    seed_complete_homepage_series,
)


def test_homepage_series_returns_full_interval_contract(db_session):
    from pytest import MonkeyPatch
    monkeypatch = MonkeyPatch()
    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['binance'])
    seed_complete_homepage_series(db_session)

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    assert len(coins) == 1
    assert_complete_interval_contract(coins[0])
    monkeypatch.undo()


def test_homepage_series_contract_fails_when_168h_source_window_is_broken(db_session):
    seed_complete_homepage_series(db_session)

    damaged_times = [START_TIME_MS + index * FIVE_MINUTES_MS for index in range(11)]
    db_session.query(MarketOpenInterestHist).filter(
        MarketOpenInterestHist.symbol == 'BTCUSDT',
        MarketOpenInterestHist.period == '5m',
        MarketOpenInterestHist.event_time.in_(damaged_times),
    ).delete(synchronize_session=False)
    db_session.query(MarketKline).filter(
        MarketKline.symbol == 'BTCUSDT',
        MarketKline.period == '5m',
        MarketKline.open_time.in_(damaged_times),
    ).delete(synchronize_session=False)
    db_session.query(MarketTakerBuySellVol).filter(
        MarketTakerBuySellVol.symbol == 'BTCUSDT',
        MarketTakerBuySellVol.period == '5m',
        MarketTakerBuySellVol.event_time.in_(damaged_times),
    ).delete(synchronize_session=False)
    db_session.commit()

    coins = get_homepage_series_data(symbols=['BTCUSDT'], session=db_session)

    assert len(coins) == 1
    with pytest.raises(AssertionError):
        assert_complete_interval_contract(coins[0])
