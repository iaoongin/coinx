from typing import List, Optional

from sqlalchemy import func, desc, asc

from coinx.config import (
    DB_TYPE,
    FETCH_COINS_TOP_GAINERS_COUNT,
    FETCH_COINS_TOP_LOSERS_COUNT,
    FETCH_COINS_TOP_VOLUME_COUNT,
)
from coinx.database import get_session
from coinx.models import MarketTickers


def save_market_tickers(records: List[dict], collect_time: int = None, session=None) -> int:
    """批量保存行情快照数据"""
    if not records:
        return 0

    own_session = session is None
    db = session or get_session()

    try:
        timestamp = collect_time if collect_time else int(__import__('time').time() * 1000)

        for record in records:
            record['close_time'] = timestamp

        dialect = db.bind.dialect.name
        if DB_TYPE == 'starrocks' and dialect == 'mysql':
            insert_cols = [c.name for c in MarketTickers.__table__.columns]
            values = [{k: v for k, v in r.items() if k in insert_cols} for r in records]
            db.execute(MarketTickers.__table__.insert().values(values))
        else:
            db.add_all([MarketTickers(**record) for record in records])
        db.commit()
        return len(records)
    except Exception:
        db.rollback()
        raise
    finally:
        if own_session:
            db.close()


def get_market_tickers(
    rank_type: str = 'price_change',
    direction: str = 'down',
    limit: int = 100,
    close_time: Optional[int] = None,
    session=None,
) -> List:
    """获取行情快照数据（按指定维度排序）"""
    own_session = session is None
    db = session or get_session()

    try:
        if close_time is None:
            close_time = db.query(func.max(MarketTickers.close_time)).scalar()

        if close_time is None:
            return []

        query = db.query(
            MarketTickers.symbol,
            MarketTickers.price_change,
            MarketTickers.price_change_percent,
            MarketTickers.weighted_avg_price,
            MarketTickers.last_price,
            MarketTickers.last_qty,
            MarketTickers.open_price,
            MarketTickers.high_price,
            MarketTickers.low_price,
            MarketTickers.volume,
            MarketTickers.quote_volume,
            MarketTickers.open_time,
            MarketTickers.close_time,
            MarketTickers.first_id,
            MarketTickers.last_id,
            MarketTickers.count,
            MarketTickers.created_at,
        ).filter(MarketTickers.close_time == close_time)

        if rank_type == 'price_change':
            if direction == 'down':
                query = query.order_by(asc(MarketTickers.price_change_percent))
            else:
                query = query.order_by(desc(MarketTickers.price_change_percent))
        elif rank_type == 'volume':
            query = query.order_by(desc(MarketTickers.volume))
        elif rank_type == 'quote_volume':
            query = query.order_by(desc(MarketTickers.quote_volume))
        else:
            query = query.order_by(asc(MarketTickers.price_change_percent))

        query = query.limit(limit)

        return query.all()
    finally:
        if own_session:
            db.close()


def get_market_ticker_symbols(
    rank_type: str = 'price_change',
    direction: str = 'down',
    limit: int = 100,
    close_time: Optional[int] = None,
    session=None,
) -> List[str]:
    """获取行情快照中的币种列表，只读取 symbol 列。"""
    own_session = session is None
    db = session or get_session()

    try:
        if close_time is None:
            close_time = db.query(func.max(MarketTickers.close_time)).scalar()

        if close_time is None:
            return []

        query = db.query(MarketTickers.symbol).filter(MarketTickers.close_time == close_time)

        if rank_type == 'price_change':
            if direction == 'down':
                query = query.order_by(asc(MarketTickers.price_change_percent))
            else:
                query = query.order_by(desc(MarketTickers.price_change_percent))
        elif rank_type == 'volume':
            query = query.order_by(desc(MarketTickers.volume))
        elif rank_type == 'quote_volume':
            query = query.order_by(desc(MarketTickers.quote_volume))
        else:
            query = query.order_by(asc(MarketTickers.price_change_percent))

        rows = query.limit(limit).all()
        return [row[0] for row in rows if row and row[0]]
    finally:
        if own_session:
            db.close()


def _stable_unique_symbols(*symbol_groups) -> List[str]:
    seen = set()
    symbols = []
    for group in symbol_groups:
        for symbol in group or []:
            if symbol and symbol not in seen:
                seen.add(symbol)
                symbols.append(symbol)
    return symbols


def get_market_scope_symbols(
    tracked_symbols=None,
    top_gainers_limit: int = FETCH_COINS_TOP_GAINERS_COUNT,
    top_losers_limit: int = FETCH_COINS_TOP_LOSERS_COUNT,
    top_volume_limit: int = FETCH_COINS_TOP_VOLUME_COUNT,
    session=None,
) -> List[str]:
    """Build the tracked and ranked market scope from the latest ticker snapshot."""
    own_session = session is None
    db = session or get_session()

    try:
        close_time = db.query(func.max(MarketTickers.close_time)).scalar()
        if close_time is None:
            return _stable_unique_symbols(tracked_symbols)

        gainers = get_market_ticker_symbols(
            rank_type='price_change', direction='up', limit=top_gainers_limit,
            close_time=close_time, session=db,
        )
        losers = get_market_ticker_symbols(
            rank_type='price_change', direction='down', limit=top_losers_limit,
            close_time=close_time, session=db,
        )
        volumes = get_market_ticker_symbols(
            rank_type='quote_volume', limit=top_volume_limit, close_time=close_time, session=db,
        )
        return _stable_unique_symbols(tracked_symbols, gainers, losers, volumes)
    finally:
        if own_session:
            db.close()


def get_market_scope_symbols_from_tickers(
    tickers,
    tracked_symbols=None,
    top_gainers_limit: int = FETCH_COINS_TOP_GAINERS_COUNT,
    top_losers_limit: int = FETCH_COINS_TOP_LOSERS_COUNT,
    top_volume_limit: int = FETCH_COINS_TOP_VOLUME_COUNT,
) -> List[str]:
    """Build the market scope from live Binance 24-hour ticker records."""
    tickers = tickers or []

    def ranked_symbols(field, reverse, limit):
        def sort_value(ticker):
            try:
                return float(ticker.get(field, 0) or 0)
            except (TypeError, ValueError):
                return 0.0

        return [
            ticker.get('symbol')
            for ticker in sorted(tickers, key=sort_value, reverse=reverse)[:limit]
            if ticker.get('symbol')
        ]

    gainers = ranked_symbols('priceChangePercent', True, top_gainers_limit)
    losers = ranked_symbols('priceChangePercent', False, top_losers_limit)
    volumes = ranked_symbols('quoteVolume', True, top_volume_limit)
    return _stable_unique_symbols(tracked_symbols, gainers, losers, volumes)


def get_latest_close_time(session=None) -> Optional[int]:
    """获取最新的快照时间"""
    own_session = session is None
    db = session or get_session()

    try:
        return db.query(func.max(MarketTickers.close_time)).scalar()
    finally:
        if own_session:
            db.close()


def delete_old_records(days: int = 7, session=None) -> int:
    """删除指定天数之前的旧数据"""
    import time
    own_session = session is None
    db = session or get_session()

    try:
        cutoff_time = int(time.time() * 1000) - (days * 24 * 60 * 60 * 1000)
        if DB_TYPE == 'starrocks':
            result = db.execute(
                MarketTickers.__table__.delete().where(MarketTickers.close_time < cutoff_time)
            )
            deleted = result.rowcount
        else:
            deleted = db.query(MarketTickers).filter(MarketTickers.close_time < cutoff_time).delete()
        db.commit()
        return deleted
    except Exception:
        db.rollback()
        raise
    finally:
        if own_session:
            db.close()
