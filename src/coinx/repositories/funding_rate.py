"""资金费率数据存储和查询模块"""
import time
from datetime import datetime

from sqlalchemy import func, text
from sqlalchemy.dialects.mysql import insert as mysql_insert

from coinx.collector.binance.funding_rate import fetch_all_premium_index
from coinx.collector.binance.client import get_session as get_http_session
from coinx.config import DB_TYPE
from coinx.database import get_session
from coinx.models import MarketFundingRate
from coinx.read_backend import get_clickhouse_repository, is_clickhouse_read
from coinx.utils import logger


def _history_cutoff_time_ms(hours, now_ms=None):
    anchor = int(now_ms) if now_ms is not None else int(time.time() * 1000)
    return anchor - hours * 60 * 60 * 1000


def save_funding_rates(records, session=None):
    """
    批量保存资金费率数据

    MySQL: INSERT ON DUPLICATE KEY UPDATE（单条 SQL）
    SQLite: 逐条 upsert（测试用）

    Args:
        records: 记录列表
        session: 数据库 session（可选）

    Returns:
        int: 成功保存的记录数
    """
    if not records:
        return 0

    from coinx.write_backend import (
        get_clickhouse_write_repository,
        is_clickhouse_write,
        market_write_lock,
    )
    if is_clickhouse_write():
        version = datetime.now()
        rows = []
        for source in records:
            row = dict(source)
            row.setdefault('exchange', 'binance')
            row.setdefault('period', '5m')
            row['created_at'] = row.get('created_at') or version
            row['updated_at'] = version
            rows.append(row)
        columns = [
            'exchange', 'symbol', 'period', 'event_time', 'funding_rate',
            'predicted_rate', 'next_funding_time', 'mark_price', 'created_at',
            'updated_at',
        ]
        with market_write_lock('market_funding_rate', 'global'):
            return get_clickhouse_write_repository().insert_rows(
                'market_funding_rate',
                columns,
                rows,
                batch_id=f'funding_{time.time_ns()}',
            )

    own_session = session is None
    db = session or get_session()

    try:
        dialect = db.bind.dialect.name
        if DB_TYPE == 'starrocks' and dialect == 'mysql':
            insert_cols = [c.name for c in MarketFundingRate.__table__.columns]
            values = [{k: v for k, v in r.items() if k in insert_cols} for r in records]
            db.execute(MarketFundingRate.__table__.insert().values(values))
        elif dialect == 'mysql':
            stmt = mysql_insert(MarketFundingRate).values(records)
            stmt = stmt.on_duplicate_key_update(
                funding_rate=stmt.inserted.funding_rate,
                predicted_rate=stmt.inserted.predicted_rate,
                next_funding_time=stmt.inserted.next_funding_time,
                mark_price=stmt.inserted.mark_price,
            )
            db.execute(stmt)
        else:
            for record in records:
                existing = db.query(MarketFundingRate).filter(
                    MarketFundingRate.symbol == record['symbol'],
                    MarketFundingRate.period == record['period'],
                    MarketFundingRate.event_time == record['event_time']
                ).first()
                if existing:
                    existing.funding_rate = record.get('funding_rate')
                    existing.predicted_rate = record.get('predicted_rate')
                    existing.next_funding_time = record.get('next_funding_time')
                    existing.mark_price = record.get('mark_price')
                else:
                    db.add(MarketFundingRate(**record))

        db.commit()
        logger.info('资金费率数据保存成功: %d 条记录', len(records))
        return len(records)

    except Exception as e:
        db.rollback()
        logger.error('资金费率数据保存失败: %s', e)
        raise
    finally:
        if own_session:
            db.close()


def load_latest_funding_rates(symbols=None, session=None, as_of_ms=None, exchange='binance'):
    """
    加载指定币种的最新资金费率（批量查询优化）

    Args:
        symbols: 交易对列表；None 时加载全部币种
        session: 数据库 session（可选）

    Returns:
        dict: {symbol: {predicted_rate, funding_rate, next_funding_time, mark_price}}
    """
    if symbols == []:
        return {}

    if session is None and is_clickhouse_read():
        return get_clickhouse_repository().latest_funding_rates(
            symbols=symbols,
            period='5m',
            exchange=exchange,
            as_of_ms=as_of_ms,
        )

    own_session = session is None
    db = session or get_session()

    try:
        symbol_clause = ''
        params = {'period': '5m', 'exchange': exchange}
        if symbols is not None:
            symbol_values = [str(symbol) for symbol in symbols if symbol]
            if not symbol_values:
                return {}
            placeholders = []
            for index, symbol in enumerate(symbol_values):
                key = f'symbol_{index}'
                placeholders.append(f':{key}')
                params[key] = symbol
            symbol_clause = f" AND symbol IN ({', '.join(placeholders)})"
        outer_symbol_clause = symbol_clause.replace('symbol IN', 'm.symbol IN')
        as_of_clause = ''
        if as_of_ms is not None:
            as_of_clause = ' AND event_time <= :as_of_ms'
            params['as_of_ms'] = int(as_of_ms)
        latest_sql = text(f"""
            SELECT m.symbol, m.event_time, m.funding_rate, m.predicted_rate,
                   m.next_funding_time, m.mark_price
            FROM market_funding_rate m
            JOIN (
                SELECT symbol, MAX(event_time) AS event_time
                FROM market_funding_rate
                WHERE period = :period AND exchange = :exchange
                  {symbol_clause}{as_of_clause}
                GROUP BY symbol
            ) latest
              ON latest.symbol = m.symbol AND latest.event_time = m.event_time
            WHERE m.period = :period AND m.exchange = :exchange
              {outer_symbol_clause}
            ORDER BY m.symbol
        """)
        records = db.execute(latest_sql, params).mappings().all()

        result = {}
        for row in records:
            result[row['symbol']] = {
                'predicted_rate': float(row['predicted_rate']) if row['predicted_rate'] is not None else None,
                'funding_rate': float(row['funding_rate']) if row['funding_rate'] is not None else None,
                'next_funding_time': int(row['next_funding_time']) if row['next_funding_time'] is not None else None,
                'mark_price': float(row['mark_price']) if row['mark_price'] is not None else None,
                'event_time': int(row['event_time']),
            }

        try:
            from coinx.repositories.market_shadow import shadow_latest_funding_rates

            shadow_latest_funding_rates(
                result, symbols=symbols, period='5m', exchange=exchange, as_of_ms=as_of_ms
            )
        except Exception:
            logger.exception('提交 ClickHouse 资金费率 shadow 对比失败')

        return result

    finally:
        if own_session:
            db.close()


def load_latest_funding_rate_page(
    keyword='',
    show_abnormal_only=False,
    sort_by='funding_rate',
    sort_order='desc',
    page=1,
    page_size=50,
    threshold=0.001,
    period='5m',
    as_of_ms=None,
    exchange='binance',
    session=None,
):
    """Load latest funding-rate rows, stats, and paging with one SQL query."""
    if session is None and is_clickhouse_read():
        rows = get_clickhouse_repository().latest_funding_rate_page(
            keyword=keyword,
            show_abnormal_only=show_abnormal_only,
            sort_by=sort_by,
            sort_order=sort_order,
            page=page,
            page_size=page_size,
            threshold=threshold,
            period=period,
            as_of_ms=as_of_ms,
            exchange=exchange,
        )
        for row in rows.get('data', []):
            value = row.get('predicted_rate') if row.get('predicted_rate') is not None else row.get('funding_rate')
            row['is_abnormal'] = bool(value is not None and abs(float(value)) >= threshold)
        return rows

    own_session = session is None
    db = session or get_session()

    order_sql_map = {
        'predicted_rate': 'predicted_rate',
        'abs_predicted_rate': 'ABS(predicted_rate)',
        'funding_rate': 'funding_rate',
        'abs_funding_rate': 'ABS(funding_rate)',
    }
    order_sql = order_sql_map.get(sort_by, 'predicted_rate')
    order_dir = 'ASC' if sort_order == 'asc' else 'DESC'
    keyword = keyword or ''
    show_abnormal_only_flag = 1 if show_abnormal_only else 0
    offset_num = max(page - 1, 0) * page_size

    as_of_clause = ''
    if as_of_ms is not None:
        as_of_clause = ' AND event_time <= :as_of_ms'
    # The imported Binance table has a period/event-time access path that is
    # substantially faster without an additional exchange predicate. Keep the
    # default query identical to the established production path; non-default
    # exchanges still receive an explicit filter.
    exchange_clause = '' if exchange == 'binance' else ' AND exchange = :exchange'
    outer_exchange_clause = '' if exchange == 'binance' else ' AND m.exchange = :exchange'

    group_by_subq = f"""(
        SELECT symbol, period, MAX(event_time) AS event_time
        FROM market_funding_rate
        WHERE period = :period{exchange_clause}
          {as_of_clause}
        GROUP BY symbol, period
    ) t"""

    stats_sql = text(f"""
        SELECT
            COUNT(*) AS total_count,
            COALESCE(SUM(CASE WHEN ABS(COALESCE(m.predicted_rate, m.funding_rate, 0)) >= :threshold THEN 1 ELSE 0 END), 0) AS abnormal_count,
            COALESCE(SUM(CASE WHEN m.funding_rate > 0 THEN 1 ELSE 0 END), 0) AS positive_count,
            COALESCE(SUM(CASE WHEN m.funding_rate < 0 THEN 1 ELSE 0 END), 0) AS negative_count
        FROM market_funding_rate m
        JOIN {group_by_subq}
            ON m.symbol = t.symbol AND m.period = t.period AND m.event_time = t.event_time
        WHERE m.period = :period{outer_exchange_clause}
          AND (:keyword = '' OR UPPER(m.symbol) LIKE :keyword_like)
          AND (
                :show_abnormal_only = 0
                OR ABS(COALESCE(m.predicted_rate, m.funding_rate, 0)) >= :threshold
          )
    """)

    page_sql = text(f"""
        SELECT
            m.symbol,
            m.event_time,
            m.funding_rate,
            m.predicted_rate,
            m.next_funding_time,
            m.mark_price,
            CASE WHEN ABS(COALESCE(m.predicted_rate, m.funding_rate, 0)) >= :threshold THEN 1 ELSE 0 END AS is_abnormal
        FROM market_funding_rate m
        JOIN {group_by_subq}
            ON m.symbol = t.symbol AND m.period = t.period AND m.event_time = t.event_time
        WHERE m.period = :period{outer_exchange_clause}
          AND (:keyword = '' OR UPPER(m.symbol) LIKE :keyword_like)
          AND (
                :show_abnormal_only = 0
                OR ABS(COALESCE(m.predicted_rate, m.funding_rate, 0)) >= :threshold
          )
        ORDER BY {order_sql} {order_dir}, m.symbol ASC
        LIMIT :limit OFFSET :offset
    """)

    params = {
        'period': period,
        'exchange': exchange,
        'threshold': threshold,
        'keyword': keyword,
        'keyword_like': f"%{keyword.upper()}%",
        'show_abnormal_only': show_abnormal_only_flag,
        'limit': page_size,
        'offset': offset_num,
    }
    if as_of_ms is not None:
        params['as_of_ms'] = int(as_of_ms)

    try:
        stats_row = db.execute(stats_sql, params).mappings().first()
        total_count = int(stats_row['total_count']) if stats_row else 0
        abnormal_count = int(stats_row['abnormal_count']) if stats_row else 0
        positive_count = int(stats_row['positive_count']) if stats_row else 0
        negative_count = int(stats_row['negative_count']) if stats_row else 0

        if total_count == 0:
            return {
                'data': [],
                'total_count': 0,
                'stats': {
                    'total': 0,
                    'abnormal': 0,
                    'positive': 0,
                    'negative': 0,
                },
            }

        rows = db.execute(page_sql, params).mappings().all()

        data = []
        for row in rows:
            if not row['symbol']:
                continue
            data.append({
                'symbol': row['symbol'],
                'predicted_rate': float(row['predicted_rate']) if row['predicted_rate'] is not None else None,
                'funding_rate': float(row['funding_rate']) if row['funding_rate'] is not None else None,
                'next_funding_time': int(row['next_funding_time']) if row['next_funding_time'] is not None else None,
                'mark_price': float(row['mark_price']) if row['mark_price'] is not None else None,
                'event_time': int(row['event_time']) if row['event_time'] is not None else None,
                'is_abnormal': bool(row['is_abnormal']),
            })

        return {
            'data': data,
            'total_count': total_count,
            'stats': {
                'total': total_count,
                'abnormal': abnormal_count,
                'positive': positive_count,
                'negative': negative_count,
            },
        }
    finally:
        if own_session:
            db.close()


def load_funding_rate_history(symbol, hours=1, exchange='binance', session=None, as_of_ms=None):
    """
    加载单个币种的资金费率历史

    Args:
        symbol: 交易对
        hours: 历史小时数（默认 1 小时）
        exchange: 交易所
        session: 数据库 session（可选）

    Returns:
        list: 历史记录列表，按时间正序
    """
    if session is None and is_clickhouse_read():
        return get_clickhouse_repository().funding_rate_history(
            symbol=symbol,
            hours=hours,
            period='5m',
            exchange=exchange,
            now_ms=as_of_ms,
        )

    own_session = session is None
    db = session or get_session()

    try:
        cutoff_time = _history_cutoff_time_ms(hours, as_of_ms)

        records = db.query(
            MarketFundingRate.symbol,
            MarketFundingRate.event_time,
            MarketFundingRate.funding_rate,
            MarketFundingRate.predicted_rate,
            MarketFundingRate.next_funding_time,
            MarketFundingRate.mark_price,
        ).filter(
            MarketFundingRate.symbol == symbol,
            MarketFundingRate.period == '5m',
            MarketFundingRate.exchange == exchange,
            MarketFundingRate.event_time >= cutoff_time
        ).order_by(MarketFundingRate.event_time.asc()).all()
        if as_of_ms is not None:
            records = [record for record in records if int(record.event_time) <= int(as_of_ms)]

        return [
            {
                'symbol': r.symbol,
                'event_time': int(r.event_time),
                'funding_rate': float(r.funding_rate) if r.funding_rate is not None else None,
                'predicted_rate': float(r.predicted_rate) if r.predicted_rate is not None else None,
                'next_funding_time': int(r.next_funding_time) if r.next_funding_time is not None else None,
                'mark_price': float(r.mark_price) if r.mark_price is not None else None,
            }
            for r in records
        ]

    finally:
        if own_session:
            db.close()


def load_abnormal_funding_rates(threshold=0.001, exchange='binance', session=None, as_of_ms=None):
    """
    加载异常资金费率（绝对值超过阈值）

    Args:
        threshold: 异常阈值（默认 0.1%）
        exchange: 交易所
        session: 数据库 session（可选）

    Returns:
        list: 异常记录列表，按绝对值降序
    """
    if session is None and is_clickhouse_read():
        return get_clickhouse_repository().abnormal_funding_rates(
            threshold=threshold,
            exchange=exchange,
            as_of_ms=as_of_ms,
        )

    own_session = session is None
    db = session or get_session()
    try:
        # The migrated Binance table is keyed by (symbol, period, event_time)
        # and currently contains only Binance rows.  Leaving exchange out of
        # the grouped subquery enables MySQL's existing symbol/time index and
        # avoids a full scan of the 9M-row history table.  Keep the explicit
        # predicate for non-default exchanges and on the outer query.
        subquery_filters = [
            MarketFundingRate.period == '5m',
        ]
        if exchange != 'binance':
            subquery_filters.append(MarketFundingRate.exchange == exchange)
        if as_of_ms is not None:
            subquery_filters.append(MarketFundingRate.event_time <= int(as_of_ms))
        subquery = db.query(
            MarketFundingRate.symbol,
            func.max(MarketFundingRate.event_time).label('max_time')
        ).filter(*subquery_filters)
        subquery = subquery.group_by(MarketFundingRate.symbol).subquery()

        records = db.query(
            MarketFundingRate.symbol,
            MarketFundingRate.event_time,
            MarketFundingRate.funding_rate,
            MarketFundingRate.predicted_rate,
            MarketFundingRate.next_funding_time,
            MarketFundingRate.mark_price,
        ).join(
            subquery,
            (MarketFundingRate.symbol == subquery.c.symbol) &
            (MarketFundingRate.event_time == subquery.c.max_time) &
            (MarketFundingRate.period == '5m') &
            (MarketFundingRate.exchange == exchange)
        ).all()

        abnormal = []
        for row in records:
            predicted = float(row.predicted_rate) if row.predicted_rate is not None else None
            funding = float(row.funding_rate) if row.funding_rate is not None else None
            rate_for_check = predicted if predicted is not None else funding
            if rate_for_check is not None and abs(rate_for_check) >= threshold:
                abnormal.append({
                    'symbol': row.symbol,
                    'predicted_rate': predicted,
                    'funding_rate': funding,
                    'next_funding_time': int(row.next_funding_time) if row.next_funding_time is not None else None,
                    'mark_price': float(row.mark_price) if row.mark_price is not None else None,
                    'event_time': int(row.event_time),
                })
        abnormal.sort(key=lambda x: abs(x['predicted_rate'] or x['funding_rate'] or 0), reverse=True)
        return abnormal
    finally:
        if own_session:
            db.close()


def load_funding_rate_sparklines(symbols, hours=1, exchange='binance', session=None, as_of_ms=None):
    """
    批量加载所有币种的资金费率走势数据（用于缩略图）

    单次查询，按 symbol 分组返回近 N 小时的 predicted_rate 序列。

    Args:
        symbols: 交易对列表
        hours: 历史小时数（默认 1）
        exchange: 交易所
        session: 数据库 session（可选）

    Returns:
        dict: {symbol: [predicted_rate, ...]}  按 event_time 正序
    """
    if not symbols:
        return {}

    if session is None and is_clickhouse_read():
        return get_clickhouse_repository().funding_rate_sparklines(
            symbols=symbols,
            hours=hours,
            exchange=exchange,
            as_of_ms=as_of_ms,
        )

    own_session = session is None
    db = session or get_session()

    try:
        cutoff_time = _history_cutoff_time_ms(hours, as_of_ms)

        records = db.query(
            MarketFundingRate.symbol,
            MarketFundingRate.event_time,
            MarketFundingRate.funding_rate,
        ).filter(
            MarketFundingRate.symbol.in_(symbols),
            MarketFundingRate.period == '5m',
            MarketFundingRate.exchange == exchange,
            MarketFundingRate.event_time >= cutoff_time,
        ).order_by(
            MarketFundingRate.symbol.asc(),
            MarketFundingRate.event_time.asc(),
        ).all()
        if as_of_ms is not None:
            records = [record for record in records if int(record.event_time) <= int(as_of_ms)]

        result = {}
        for r in records:
            val = float(r.funding_rate) if r.funding_rate is not None else None
            result.setdefault(r.symbol, []).append(val)

        return result

    finally:
        if own_session:
            db.close()


def collect_funding_rates(symbols=None, max_workers=4, http_session=None, db_session=None):
    """
    采集资金费率数据（使用批量 API 一次性获取所有币种）

    Args:
        symbols: 币种列表（可选，用于过滤）
        max_workers: 未使用（保留兼容性）
        http_session: HTTP session（可选）
        db_session: 数据库 session（可选）

    Returns:
        int: 成功保存的记录数
    """
    from coinx.write_backend import is_clickhouse_write

    own_db = db_session is None
    db = None if is_clickhouse_write() else (db_session or get_session())

    try:
        sess = http_session or get_http_session()

        # 使用批量 API 一次性获取所有币种
        all_records = fetch_all_premium_index(session=sess)

        # 如果指定了 symbols，过滤出需要的
        if symbols:
            symbol_set = set(symbols)
            records = [r for r in all_records if r['symbol'] in symbol_set]
        else:
            records = all_records

        # 添加 exchange 字段
        for record in records:
            record['exchange'] = 'binance'

        if records:
            save_funding_rates(records, session=None if is_clickhouse_write() else db)
        return len(records)

    finally:
        if own_db and db is not None:
            db.close()
