import logging
from datetime import datetime
from types import SimpleNamespace
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
from coinx.read_backend import get_clickhouse_repository, is_clickhouse_read


logger = logging.getLogger(__name__)


def save_market_tickers(records: List[dict], collect_time: int = None, session=None) -> int:
    """批量保存行情快照数据"""
    if not records:
        return 0

    from coinx.write_backend import (
        get_clickhouse_write_repository,
        is_clickhouse_write,
        market_write_lock,
    )
    if is_clickhouse_write():
        timestamp = collect_time if collect_time else int(__import__('time').time() * 1000)
        version = datetime.now()
        rows = []
        for source in records:
            row = dict(source)
            row['close_time'] = timestamp
            row['created_at'] = row.get('created_at') or version
            row['updated_at'] = version
            rows.append(row)
        columns = [
            'close_time', 'symbol', 'price_change', 'price_change_percent',
            'weighted_avg_price', 'last_price', 'last_qty', 'open_price',
            'high_price', 'low_price', 'volume', 'quote_volume', 'open_time',
            'first_id', 'last_id', 'count', 'created_at', 'updated_at',
        ]
        with market_write_lock('market_tickers', 'global'):
            return get_clickhouse_write_repository().insert_rows(
                'market_tickers',
                columns,
                rows,
                batch_id=f'tickers_{timestamp}',
            )

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
    as_of_ms: Optional[int] = None,
    session=None,
) -> List:
    if session is None and is_clickhouse_read():
        rows = get_clickhouse_repository().latest_tickers(
            rank_type=rank_type,
            direction=direction,
            limit=limit,
            close_time=close_time,
            as_of_ms=as_of_ms,
        )
        return [SimpleNamespace(**row) for row in rows]

    """获取行情快照数据（按指定维度排序）"""
    own_session = session is None
    db = session or get_session()

    try:
        if close_time is None:
            latest_query = db.query(func.max(MarketTickers.close_time))
            if as_of_ms is not None:
                latest_query = latest_query.filter(MarketTickers.close_time <= int(as_of_ms))
            close_time = latest_query.scalar()

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
                query = query.order_by(asc(MarketTickers.price_change_percent), asc(MarketTickers.symbol))
            else:
                query = query.order_by(desc(MarketTickers.price_change_percent), asc(MarketTickers.symbol))
        elif rank_type == 'volume':
            query = query.order_by(desc(MarketTickers.volume), asc(MarketTickers.symbol))
        elif rank_type == 'quote_volume':
            query = query.order_by(desc(MarketTickers.quote_volume), asc(MarketTickers.symbol))
        else:
            query = query.order_by(asc(MarketTickers.price_change_percent), asc(MarketTickers.symbol))

        query = query.limit(limit)
        rows = query.all()

        try:
            from coinx.repositories.market_shadow import shadow_latest_tickers

            shadow_latest_tickers(
                rows,
                rank_type=rank_type,
                direction=direction,
                limit=limit,
                close_time=close_time,
            )
        except Exception:
            # Shadow diagnostics must never break the MySQL response path.
            logger.exception('提交 ClickHouse 行情 shadow 对比失败')

        return rows
    finally:
        if own_session:
            db.close()


def get_market_ticker_symbols(
    rank_type: str = 'price_change',
    direction: str = 'down',
    limit: int = 100,
    close_time: Optional[int] = None,
    as_of_ms: Optional[int] = None,
    session=None,
) -> List[str]:
    if session is None and is_clickhouse_read():
        rows = get_clickhouse_repository().latest_tickers(
            rank_type=rank_type,
            direction=direction,
            limit=limit,
            close_time=close_time,
            as_of_ms=as_of_ms,
        )
        return [str(row.get('symbol')) for row in rows if row.get('symbol')]
    """获取行情快照中的币种列表，只读取 symbol 列。"""
    own_session = session is None
    db = session or get_session()

    try:
        if close_time is None:
            latest_query = db.query(func.max(MarketTickers.close_time))
            if as_of_ms is not None:
                latest_query = latest_query.filter(MarketTickers.close_time <= int(as_of_ms))
            close_time = latest_query.scalar()

        if close_time is None:
            return []

        query = db.query(MarketTickers.symbol).filter(MarketTickers.close_time == close_time)

        if rank_type == 'price_change':
            if direction == 'down':
                query = query.order_by(asc(MarketTickers.price_change_percent), asc(MarketTickers.symbol))
            else:
                query = query.order_by(desc(MarketTickers.price_change_percent), asc(MarketTickers.symbol))
        elif rank_type == 'volume':
            query = query.order_by(desc(MarketTickers.volume), asc(MarketTickers.symbol))
        elif rank_type == 'quote_volume':
            query = query.order_by(desc(MarketTickers.quote_volume), asc(MarketTickers.symbol))
        else:
            query = query.order_by(asc(MarketTickers.price_change_percent), asc(MarketTickers.symbol))

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
    if session is None and is_clickhouse_read():
        gainers = get_market_ticker_symbols(
            rank_type='price_change', direction='up', limit=top_gainers_limit,
        )
        losers = get_market_ticker_symbols(
            rank_type='price_change', direction='down', limit=top_losers_limit,
        )
        volumes = get_market_ticker_symbols(
            rank_type='quote_volume', direction='up', limit=top_volume_limit,
        )
        return _stable_unique_symbols(tracked_symbols, gainers, losers, volumes)

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


def get_latest_close_time(as_of_ms: Optional[int] = None, session=None) -> Optional[int]:
    if session is None and is_clickhouse_read():
        repository = get_clickhouse_repository()
        # close_time is the snapshot key; duplicates do not affect max().
        # Avoid FINAL here because it merges every ticker part just to find a
        # scalar watermark.
        sql = f"SELECT max(close_time) FROM {repository._table('market_tickers', final=False)}"
        if as_of_ms is not None:
            sql += f" WHERE close_time <= {int(as_of_ms)}"
        value = repository.client.query_scalar(sql)
        return int(value) if value is not None else None

    """获取最新的快照时间"""
    own_session = session is None
    db = session or get_session()

    try:
        query = db.query(func.max(MarketTickers.close_time))
        if as_of_ms is not None:
            query = query.filter(MarketTickers.close_time <= int(as_of_ms))
        return query.scalar()
    finally:
        if own_session:
            db.close()


def delete_old_records(days: int = 7, session=None) -> int:
    from coinx.write_backend import get_clickhouse_write_repository, is_clickhouse_write
    if is_clickhouse_write():
        from coinx.read_backend import get_clickhouse_repository

        cutoff_time = int(__import__('time').time() * 1000) - (days * 24 * 60 * 60 * 1000)
        table = get_clickhouse_repository()._table('market_tickers').replace(' FINAL', '')
        from coinx.write_backend import market_write_lock
        with market_write_lock('market_tickers', 'cleanup'):
            get_clickhouse_write_repository().client.execute(
                f'ALTER TABLE {table} DELETE WHERE close_time < {int(cutoff_time)}'
            )
        return 0
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
