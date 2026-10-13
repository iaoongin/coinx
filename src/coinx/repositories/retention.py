"""Retention cleanup for historical market records."""

import time

from coinx.database import get_session
from coinx.models import (
    MarketFundingRate,
    MarketFuturesSmartMoneySignal,
    MarketKline,
    MarketOpenInterestHist,
    MarketSnapshot,
    MarketTakerBuySellVol,
    MarketTickers,
)


MYSQL_RETENTION_TARGETS = (
    (MarketSnapshot, MarketSnapshot.snapshot_time),
    (MarketTickers, MarketTickers.close_time),
    (MarketOpenInterestHist, MarketOpenInterestHist.event_time),
    (MarketKline, MarketKline.close_time),
    (MarketTakerBuySellVol, MarketTakerBuySellVol.event_time),
    (MarketFundingRate, MarketFundingRate.event_time),
    (MarketFuturesSmartMoneySignal, MarketFuturesSmartMoneySignal.snapshot_time),
)

CLICKHOUSE_RETENTION_TARGETS = (
    ('market_snapshots', 'snapshot_time'),
    ('market_tickers', 'close_time'),
    ('market_open_interest_hist', 'event_time'),
    ('market_klines', 'close_time'),
    ('market_taker_buy_sell_vol', 'event_time'),
    ('market_funding_rate', 'event_time'),
    ('market_futures_smart_money_signal', 'snapshot_time'),
)

DELETE_BATCH_SIZE = 10_000


def retention_cutoff_ms(retention_days, now_ms=None):
    """Return the inclusive millisecond cutoff for a retention window."""
    current = int(time.time() * 1000) if now_ms is None else int(now_ms)
    return current - max(0, int(retention_days)) * 24 * 60 * 60 * 1000


def _delete_mysql_model_rows(session, model, time_column, cutoff_ms):
    deleted = 0
    while True:
        ids = [row[0] for row in (
            session.query(model.id)
            .filter(time_column < cutoff_ms)
            .order_by(model.id)
            .limit(DELETE_BATCH_SIZE)
            .all()
        )]
        if not ids:
            return deleted
        deleted += session.query(model).filter(model.id.in_(ids)).delete(
            synchronize_session=False,
        )
        session.commit()


def _cleanup_mysql_market_data(retention_days, session=None):
    own_session = session is None
    db = session or get_session()
    cutoff_ms = retention_cutoff_ms(retention_days)
    deleted = {}
    try:
        for model, time_column in MYSQL_RETENTION_TARGETS:
            deleted[model.__tablename__] = _delete_mysql_model_rows(
                db, model, time_column, cutoff_ms,
            )

        return deleted
    except Exception:
        db.rollback()
        raise
    finally:
        if own_session:
            db.close()


def _cleanup_clickhouse(retention_days):
    from coinx.read_backend import get_clickhouse_repository
    from coinx.write_backend import (
        get_clickhouse_write_repository,
        market_write_lock,
    )

    cutoff_ms = retention_cutoff_ms(retention_days)
    read_repository = get_clickhouse_repository()
    write_repository = get_clickhouse_write_repository()
    deleted = {}
    for table_name, time_column in CLICKHOUSE_RETENTION_TARGETS:
        table = read_repository._table(table_name, final=False)
        with market_write_lock(table_name, 'retention_cleanup'):
            write_repository.client.execute(
                f'ALTER TABLE {table} DELETE WHERE {time_column} < {cutoff_ms}'
            )
        deleted[table_name] = 'mutation_submitted'
    return deleted


def cleanup_expired_market_data(retention_days, session=None):
    """Delete market records older than the configured retention window."""
    from coinx.write_backend import is_clickhouse_write

    if is_clickhouse_write():
        return _cleanup_clickhouse(retention_days)
    return _cleanup_mysql_market_data(retention_days, session=session)


def cleanup_expired_smart_money_data(retention_days, session=None):
    """Remove expired Smart Money list snapshots without touching other market data."""
    from coinx.write_backend import is_clickhouse_write

    cutoff_ms = retention_cutoff_ms(retention_days)
    if is_clickhouse_write():
        from coinx.read_backend import get_clickhouse_repository
        from coinx.write_backend import get_clickhouse_write_repository, market_write_lock

        table = get_clickhouse_repository()._table(
            'market_futures_smart_money_signal', final=False,
        )
        with market_write_lock('market_futures_smart_money_signal', 'retention_cleanup'):
            get_clickhouse_write_repository().client.execute(
                f'ALTER TABLE {table} DELETE WHERE snapshot_time < {cutoff_ms}'
            )
        return {'market_futures_smart_money_signal': 'mutation_submitted'}

    own_session = session is None
    db = session or get_session()
    try:
        deleted = _delete_mysql_model_rows(
            db,
            MarketFuturesSmartMoneySignal,
            MarketFuturesSmartMoneySignal.snapshot_time,
            cutoff_ms,
        )
        return {'market_futures_smart_money_signal': deleted}
    except Exception:
        db.rollback()
        raise
    finally:
        if own_session:
            db.close()
