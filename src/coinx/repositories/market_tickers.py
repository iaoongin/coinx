from typing import List, Optional

from sqlalchemy import and_, func, desc, asc

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
) -> List[MarketTickers]:
    """获取行情快照数据（按指定维度排序）"""
    own_session = session is None
    db = session or get_session()

    try:
        if close_time is None:
            close_time = db.query(func.max(MarketTickers.close_time)).scalar()

        if close_time is None:
            return []

        query = db.query(MarketTickers).filter(MarketTickers.close_time == close_time)

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
        deleted = db.query(MarketTickers).filter(MarketTickers.close_time < cutoff_time).delete()
        db.commit()
        return deleted
    except Exception:
        db.rollback()
        raise
    finally:
        if own_session:
            db.close()
