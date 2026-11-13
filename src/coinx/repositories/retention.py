"""Retention cleanup for historical market records."""

import time

from sqlalchemy import and_, inspect, or_

from coinx.collector.timing import format_duration_ms
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
from coinx.utils import logger


MYSQL_RETENTION_TARGETS = (
    (MarketSnapshot, MarketSnapshot.snapshot_time),
    (MarketTickers, MarketTickers.close_time),
    (MarketOpenInterestHist, MarketOpenInterestHist.event_time),
    (MarketKline, MarketKline.close_time),
    (MarketTakerBuySellVol, MarketTakerBuySellVol.event_time),
    (MarketFundingRate, MarketFundingRate.event_time),
)

CLICKHOUSE_RETENTION_TARGETS = (
    ('market_snapshots', 'snapshot_time'),
    ('market_tickers', 'close_time'),
    ('market_open_interest_hist', 'event_time'),
    ('market_klines', 'close_time'),
    ('market_taker_buy_sell_vol', 'event_time'),
    ('market_funding_rate', 'event_time'),
)

DELETE_BATCH_SIZE = 100_000


def retention_cutoff_ms(retention_days, now_ms=None):
    """Return the inclusive millisecond cutoff for a retention window."""
    current = int(time.time() * 1000) if now_ms is None else int(now_ms)
    return current - max(0, int(retention_days)) * 24 * 60 * 60 * 1000


def _report_cleanup_progress(progress_callback, table_name, batch_number, total_batches, deleted_count, **details):
    if progress_callback is None:
        return
    try:
        progress_callback(
            table_name,
            batch_number,
            total_batches,
            deleted_count,
            **details,
        )
    except Exception:
        logger.exception('[数据清理] 进度记录失败 | %s | 第 %s/%s 批', table_name, batch_number, total_batches or '--')


def _delete_mysql_model_rows(session, model, time_column, cutoff_ms, progress_callback=None):
    table_name = model.__tablename__
    table_started_at = time.perf_counter()
    batch_number = 0
    deleted = 0
    logger.info('')
    logger.info('[数据清理] 表开始 | %s | 正在统计过期记录', table_name)
    logger.info('')
    _report_cleanup_progress(progress_callback, table_name, 0, None, 0, stage='table_started')
    bind = session.get_bind()
    if bind.dialect.name == 'mysql':
        indexes = inspect(bind).get_indexes(table_name)
        if not any(index.get('column_names', [None])[0] == time_column.name for index in indexes if index.get('column_names')):
            raise RuntimeError(f'{table_name} 缺少以 {time_column.name} 为首列的清理索引')
    max_id = session.query(model.id).order_by(model.id.desc()).limit(1).scalar()
    total_rows = (
        session.query(model.id).filter(model.id <= max_id, time_column < cutoff_ms).count()
        if max_id is not None else 0
    )
    total_batches = (total_rows + DELETE_BATCH_SIZE - 1) // DELETE_BATCH_SIZE
    logger.info(
        '[数据清理] 表准备完成 | %s | 待清理 %d 条 | 预计 %d 批',
        table_name, total_rows, total_batches,
    )
    _report_cleanup_progress(
        progress_callback, table_name, 0, total_batches, 0, stage='counting_completed',
    )
    last_time = None
    last_id = None
    while max_id is not None:
        batch_started_at = time.perf_counter()
        query = session.query(model.id, time_column).filter(model.id <= max_id, time_column < cutoff_ms)
        if last_time is not None:
            query = query.filter(or_(
                time_column > last_time,
                and_(time_column == last_time, model.id > last_id),
            ))
        rows = query.order_by(time_column.asc(), model.id.asc()).limit(DELETE_BATCH_SIZE).all()
        query_duration_ms = (time.perf_counter() - batch_started_at) * 1000
        if not rows:
            break
        batch_number += 1
        last_id, last_time = rows[-1]
        delete_started_at = time.perf_counter()
        batch_deleted = session.query(model).filter(
            model.id.in_([row[0] for row in rows]),
            time_column < cutoff_ms,
        ).delete(
            synchronize_session=False,
        )
        delete_duration_ms = (time.perf_counter() - delete_started_at) * 1000
        commit_started_at = time.perf_counter()
        session.commit()
        commit_duration_ms = (time.perf_counter() - commit_started_at) * 1000
        deleted += batch_deleted
        batch_duration_ms = (time.perf_counter() - batch_started_at) * 1000
        table_duration_ms = (time.perf_counter() - table_started_at) * 1000
        remaining_ms = max(0, total_batches - batch_number) * (table_duration_ms / batch_number)
        _report_cleanup_progress(
            progress_callback,
            table_name,
            batch_number,
            total_batches,
            deleted,
            stage='batch_finished',
            batch_deleted_count=batch_deleted,
            batch_duration_ms=batch_duration_ms,
            query_duration_ms=query_duration_ms,
            delete_duration_ms=delete_duration_ms,
            commit_duration_ms=commit_duration_ms,
            total_duration_ms=table_duration_ms,
            remaining_duration_ms=remaining_ms,
        )
        logger.info(
            '[数据清理] 进行中 | %s | 第 %d/%d 批（预计） | 本批删除 %d 条 | 累计 %d 条 | 批次耗时 %s | 查询 %s | 删除 %s | 提交 %s | 总耗时 %s | 预计剩余 %s',
            table_name,
            batch_number,
            total_batches,
            batch_deleted,
            deleted,
            format_duration_ms(batch_duration_ms),
            format_duration_ms(query_duration_ms),
            format_duration_ms(delete_duration_ms),
            format_duration_ms(commit_duration_ms),
            format_duration_ms(table_duration_ms),
            format_duration_ms(remaining_ms),
        )
    _report_cleanup_progress(
        progress_callback, table_name, 0, total_batches, deleted,
        stage='table_finished', total_duration_ms=(time.perf_counter() - table_started_at) * 1000,
    )
    logger.info('')
    logger.info(
        '[数据清理] 表完成 | %s | 删除 %d 条 | 共 %d 批 | 耗时 %s',
        table_name,
        deleted,
        batch_number,
        format_duration_ms((time.perf_counter() - table_started_at) * 1000),
    )
    logger.info('')
    return deleted


def _cleanup_mysql_market_data(retention_days, session=None, progress_callback=None):
    own_session = session is None
    db = session or get_session()
    cutoff_ms = retention_cutoff_ms(retention_days)
    deleted = {}
    try:
        for model, time_column in MYSQL_RETENTION_TARGETS:
            deleted[model.__tablename__] = _delete_mysql_model_rows(
                db,
                model,
                time_column,
                cutoff_ms,
                progress_callback=progress_callback,
            )

        return deleted
    except Exception:
        db.rollback()
        raise
    finally:
        if own_session:
            db.close()


def _cleanup_clickhouse(retention_days, progress_callback=None):
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
        table_started_at = time.perf_counter()
        logger.info('')
        logger.info('[数据清理] 表开始 | %s', table_name)
        logger.info('')
        table = read_repository._table(table_name, final=False)
        with market_write_lock(table_name, 'retention_cleanup'):
            write_repository.client.execute(
                f'ALTER TABLE {table} DELETE WHERE {time_column} < {cutoff_ms}'
            )
        deleted[table_name] = 'mutation_submitted'
        _report_cleanup_progress(
            progress_callback,
            table_name,
            1,
            1,
            None,
            stage='mutation_submitted',
        )
        logger.info('')
        logger.info(
            '[数据清理] 表完成 | %s | 删除结果=mutation_submitted | 耗时 %s',
            table_name,
            format_duration_ms((time.perf_counter() - table_started_at) * 1000),
        )
        logger.info('')
    return deleted


def cleanup_expired_market_data(retention_days, session=None, progress_callback=None):
    """Delete market records older than the configured retention window."""
    from coinx.write_backend import is_clickhouse_write

    if is_clickhouse_write():
        return _cleanup_clickhouse(retention_days, progress_callback=progress_callback)
    return _cleanup_mysql_market_data(
        retention_days,
        session=session,
        progress_callback=progress_callback,
    )


def cleanup_expired_smart_money_data(retention_days, session=None, progress_callback=None):
    """Remove expired Smart Money list snapshots without touching other market data."""
    from coinx.write_backend import is_clickhouse_write

    cutoff_ms = retention_cutoff_ms(retention_days)
    if is_clickhouse_write():
        from coinx.read_backend import get_clickhouse_repository
        from coinx.write_backend import get_clickhouse_write_repository, market_write_lock

        table_name = 'market_futures_smart_money_signal'
        table_started_at = time.perf_counter()
        logger.info('')
        logger.info('[数据清理] 表开始 | %s', table_name)
        logger.info('')
        table = get_clickhouse_repository()._table(
            table_name, final=False,
        )
        with market_write_lock(table_name, 'retention_cleanup'):
            get_clickhouse_write_repository().client.execute(
                f'ALTER TABLE {table} DELETE WHERE snapshot_time < {cutoff_ms}'
            )
        logger.info('')
        logger.info(
            '[数据清理] 表完成 | %s | 删除结果=mutation_submitted | 耗时 %s',
            table_name,
            format_duration_ms((time.perf_counter() - table_started_at) * 1000),
        )
        logger.info('')
        _report_cleanup_progress(
            progress_callback,
            table_name,
            1,
            1,
            None,
            stage='mutation_submitted',
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
            progress_callback=progress_callback,
        )
        return {'market_futures_smart_money_signal': deleted}
    except Exception:
        db.rollback()
        raise
    finally:
        if own_session:
            db.close()
