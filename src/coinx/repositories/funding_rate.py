"""资金费率数据存储和查询模块"""
from datetime import datetime, timedelta

from sqlalchemy import func, text
from sqlalchemy.dialects.mysql import insert as mysql_insert

from coinx.collector.binance.funding_rate import fetch_all_premium_index
from coinx.collector.binance.client import get_session as get_http_session
from coinx.config import DB_TYPE
from coinx.database import get_session
from coinx.models import MarketFundingRate
from coinx.utils import logger


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


def load_latest_funding_rates(symbols, exchange='binance', session=None):
    """
    加载指定币种的最新资金费率（批量查询优化）

    Args:
        symbols: 交易对列表
        exchange: 交易所
        session: 数据库 session（可选）

    Returns:
        dict: {symbol: {predicted_rate, funding_rate, next_funding_time, mark_price}}
    """
    if not symbols:
        return {}

    own_session = session is None
    db = session or get_session()

    try:
        subquery = db.query(
            MarketFundingRate.symbol,
            func.max(MarketFundingRate.event_time).label('max_time')
        ).filter(
            MarketFundingRate.symbol.in_(symbols),
            MarketFundingRate.period == '5m',
            MarketFundingRate.exchange == exchange
        ).group_by(MarketFundingRate.symbol).subquery()

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
            (MarketFundingRate.event_time == subquery.c.max_time)
        ).all()

        result = {}
        for r in records:
            result[r.symbol] = {
                'predicted_rate': float(r.predicted_rate) if r.predicted_rate else None,
                'funding_rate': float(r.funding_rate) if r.funding_rate else None,
                'next_funding_time': int(r.next_funding_time) if r.next_funding_time else None,
                'mark_price': float(r.mark_price) if r.mark_price else None,
                'event_time': int(r.event_time),
            }

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
    session=None,
):
    """Load latest funding-rate rows, stats, and paging with one SQL query."""
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
    offset = max(page - 1, 0) * page_size
    keyword = keyword or ''

    sql = text(f"""
        WITH ranked AS (
            SELECT
                symbol,
                event_time,
                funding_rate,
                predicted_rate,
                next_funding_time,
                mark_price,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol
                    ORDER BY event_time DESC
                ) AS rn
            FROM market_funding_rate
            WHERE period = :period
        ),
        latest AS (
            SELECT
                symbol,
                event_time,
                funding_rate,
                predicted_rate,
                next_funding_time,
                mark_price,
                CASE
                    WHEN ABS(COALESCE(predicted_rate, funding_rate, 0)) >= :threshold THEN 1
                    ELSE 0
                END AS is_abnormal
            FROM ranked
            WHERE rn = 1
              AND (:keyword = '' OR UPPER(symbol) LIKE :keyword_like)
              AND (
                    :show_abnormal_only = 0
                    OR ABS(COALESCE(predicted_rate, funding_rate, 0)) >= :threshold
              )
        ),
        counted AS (
            SELECT
                COUNT(*) AS total_count,
                COALESCE(SUM(is_abnormal), 0) AS abnormal_count,
                COALESCE(SUM(CASE WHEN funding_rate > 0 THEN 1 ELSE 0 END), 0) AS positive_count,
                COALESCE(SUM(CASE WHEN funding_rate < 0 THEN 1 ELSE 0 END), 0) AS negative_count
            FROM latest
        ),
        numbered AS (
            SELECT
                symbol,
                event_time,
                funding_rate,
                predicted_rate,
                next_funding_time,
                mark_price,
                is_abnormal,
                ROW_NUMBER() OVER (
                    ORDER BY {order_sql} {order_dir}, symbol ASC
                ) AS seq
            FROM latest
        ),
        paged AS (
            SELECT *
            FROM numbered
            WHERE seq > :offset
              AND seq <= :offset + :limit
        )
        SELECT
            paged.symbol,
            paged.event_time,
            paged.funding_rate,
            paged.predicted_rate,
            paged.next_funding_time,
            paged.mark_price,
            paged.is_abnormal,
            counted.total_count,
            counted.abnormal_count,
            counted.positive_count,
            counted.negative_count
        FROM counted
        LEFT JOIN paged ON 1 = 1
        ORDER BY paged.seq
    """)

    try:
        rows = db.execute(
            sql,
            {
                'period': period,
                'threshold': threshold,
                'keyword': keyword,
                'keyword_like': f"%{keyword.upper()}%",
                'show_abnormal_only': 1 if show_abnormal_only else 0,
                'offset': offset,
                'limit': page_size,
            },
        ).mappings().all()

        if not rows:
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

        head = rows[0]
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
            'total_count': int(head['total_count'] or 0),
            'stats': {
                'total': int(head['total_count'] or 0),
                'abnormal': int(head['abnormal_count'] or 0),
                'positive': int(head['positive_count'] or 0),
                'negative': int(head['negative_count'] or 0),
            },
        }
    finally:
        if own_session:
            db.close()


def load_funding_rate_history(symbol, hours=1, exchange='binance', session=None):
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
    own_session = session is None
    db = session or get_session()

    try:
        cutoff_time = int((datetime.utcnow() - timedelta(hours=hours)).timestamp() * 1000)

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

        return [
            {
                'symbol': r.symbol,
                'event_time': int(r.event_time),
                'funding_rate': float(r.funding_rate) if r.funding_rate else None,
                'predicted_rate': float(r.predicted_rate) if r.predicted_rate else None,
                'next_funding_time': int(r.next_funding_time) if r.next_funding_time else None,
                'mark_price': float(r.mark_price) if r.mark_price else None,
            }
            for r in records
        ]

    finally:
        if own_session:
            db.close()


def load_abnormal_funding_rates(threshold=0.001, exchange='binance', session=None):
    """
    加载异常资金费率（绝对值超过阈值）

    Args:
        threshold: 异常阈值（默认 0.1%）
        exchange: 交易所
        session: 数据库 session（可选）

    Returns:
        list: 异常记录列表，按绝对值降序
    """
    own_session = session is None
    db = session or get_session()

    try:
        subquery = db.query(
            MarketFundingRate.symbol,
            func.max(MarketFundingRate.event_time).label('max_time')
        ).filter(
            MarketFundingRate.period == '5m',
            MarketFundingRate.exchange == exchange
        ).group_by(MarketFundingRate.symbol).subquery()

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
            (MarketFundingRate.event_time == subquery.c.max_time)
        ).all()

        abnormal = []
        for r in records:
            predicted = float(r.predicted_rate) if r.predicted_rate is not None else None
            funding = float(r.funding_rate) if r.funding_rate is not None else None
            rate_for_check = predicted if predicted is not None else funding
            if rate_for_check is not None and abs(rate_for_check) >= threshold:
                abnormal.append({
                    'symbol': r.symbol,
                    'predicted_rate': predicted,
                    'funding_rate': funding,
                    'next_funding_time': int(r.next_funding_time) if r.next_funding_time else None,
                    'mark_price': float(r.mark_price) if r.mark_price else None,
                    'event_time': int(r.event_time),
                })

        abnormal.sort(key=lambda x: abs(x['predicted_rate'] or x['funding_rate'] or 0), reverse=True)

        return abnormal

    finally:
        if own_session:
            db.close()


def load_funding_rate_sparklines(symbols, hours=1, exchange='binance', session=None):
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

    own_session = session is None
    db = session or get_session()

    try:
        cutoff_time = int((datetime.utcnow() - timedelta(hours=hours)).timestamp() * 1000)

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
    own_db = db_session is None
    db = db_session or get_session()

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
            save_funding_rates(records, session=db)
        return len(records)

    finally:
        if own_db:
            db.close()
