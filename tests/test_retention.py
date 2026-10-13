import time

from coinx.models import MarketFundingRate, MarketFuturesSmartMoneySignal
from coinx.repositories import retention


def test_cleanup_mysql_removes_records_older_than_retention_window(db_session, monkeypatch):
    now_ms = int(time.time() * 1000)
    cutoff_ms = retention.retention_cutoff_ms(30, now_ms=now_ms)
    old_event_time = cutoff_ms - 1
    recent_event_time = cutoff_ms + 60 * 1000

    db_session.add_all([
        MarketFundingRate(
            symbol='OLDUSDT',
            period='5m',
            event_time=old_event_time,
            exchange='binance',
        ),
        MarketFundingRate(
            symbol='NEWUSDT',
            period='5m',
            event_time=recent_event_time,
            exchange='binance',
        ),
    ])
    db_session.commit()

    monkeypatch.setattr(
        retention,
        'MYSQL_RETENTION_TARGETS',
        (
            (MarketFundingRate, MarketFundingRate.event_time),
        ),
    )

    deleted = retention._cleanup_mysql_market_data(30, session=db_session)

    assert deleted['market_funding_rate'] == 1
    assert db_session.query(MarketFundingRate).count() == 1


def test_cleanup_smart_money_removes_expired_list_snapshots(db_session):
    now_ms = int(time.time() * 1000)
    cutoff_ms = retention.retention_cutoff_ms(30, now_ms=now_ms)
    db_session.add_all([
        MarketFuturesSmartMoneySignal(
            exchange='binance',
            time_range='30m',
            symbol='OLDUSDT',
            period='15m',
            snapshot_time=cutoff_ms - 1,
        ),
        MarketFuturesSmartMoneySignal(
            exchange='binance',
            time_range='30m',
            symbol='NEWUSDT',
            period='15m',
            snapshot_time=cutoff_ms + 60 * 1000,
        ),
    ])
    db_session.commit()

    deleted = retention.cleanup_expired_smart_money_data(30, session=db_session)

    assert deleted['market_futures_smart_money_signal'] == 1
    assert db_session.query(MarketFuturesSmartMoneySignal).count() == 1
