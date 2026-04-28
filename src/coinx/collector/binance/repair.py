import time

from coinx.coin_manager import get_active_coins
from coinx.config import (
    BINANCE_SERIES_REPAIR_BOOTSTRAP_DAYS,
    BINANCE_SERIES_REPAIR_COVERAGE_HOURS,
    BINANCE_SERIES_REPAIR_ENABLED,
    BINANCE_SERIES_REPAIR_FUTURES_PAGE_LIMIT,
    BINANCE_SERIES_REPAIR_KLINES_PAGE_LIMIT,
    BINANCE_SERIES_REPAIR_PERIOD,
    BINANCE_SERIES_REPAIR_SLEEP_MS,
)
from coinx.database import get_session
from coinx.repositories.binance_series import (
    get_earliest_series_timestamp as get_earliest_series_timestamp_from_repo,
    get_latest_series_timestamp as get_latest_series_timestamp_from_repo,
    upsert_series_records,
    get_series_model,
)
from coinx.utils import logger
from coinx.collector.binance.series import fetch_series_payload, parse_series_payload


FIVE_MINUTES_MS = 5 * 60 * 1000
DEFAULT_REPAIR_SERIES_TYPES = [
    'klines',
    'open_interest_hist',
    'global_long_short_account_ratio',
    'top_long_short_position_ratio',
    'top_long_short_account_ratio',
    'taker_buy_sell_vol',
]


def floor_to_completed_5m(now_ms):
    return now_ms - (now_ms % FIVE_MINUTES_MS)


def latest_closed_5m_open_time(now_ms):
    """Return the latest fully closed 5m candle open time."""
    return max(0, floor_to_completed_5m(now_ms) - FIVE_MINUTES_MS)


def trim_unclosed_series_records(series_type, records, now_ms, period=BINANCE_SERIES_REPAIR_PERIOD):
    """Trim records that belong to the current in-flight 5m window."""
    if not records or period != BINANCE_SERIES_REPAIR_PERIOD:
        return records

    if series_type == 'klines':
        trimmed = []
        for record in records:
            close_time = record.get('close_time')
            if close_time is None or close_time > now_ms:
                continue
            trimmed.append(record)
        return trimmed

    cutoff_time = latest_closed_5m_open_time(now_ms)
    time_field = _get_time_field(series_type)
    trimmed = []
    for record in records:
        event_time = record.get(time_field)
        if event_time is None or event_time > cutoff_time:
            continue
        trimmed.append(record)
    return trimmed


def get_latest_series_timestamp(symbol, series_type, session=None):
    return get_latest_series_timestamp_from_repo(
        series_type=series_type,
        symbol=symbol,
        period=BINANCE_SERIES_REPAIR_PERIOD,
        session=session,
    )


def get_earliest_series_timestamp(symbol, series_type, session=None):
    return get_earliest_series_timestamp_from_repo(
        series_type=series_type,
        symbol=symbol,
        period=BINANCE_SERIES_REPAIR_PERIOD,
        session=session,
    )


def build_repair_window(symbol, series_type, now_ms, session=None):
    target_end_time = latest_closed_5m_open_time(now_ms)
    latest_local_timestamp = get_latest_series_timestamp(symbol, series_type, session=session)
    earliest_local_timestamp = get_earliest_series_timestamp(symbol, series_type, session=session)
    bootstrap_start_time = target_end_time - BINANCE_SERIES_REPAIR_BOOTSTRAP_DAYS * 24 * 60 * 60 * 1000
    coverage_start_time = target_end_time - BINANCE_SERIES_REPAIR_COVERAGE_HOURS * 60 * 60 * 1000
    desired_start_time = max(0, min(bootstrap_start_time, coverage_start_time))

    gap_start_time = _find_first_missing_series_timestamp(
        symbol=symbol,
        series_type=series_type,
        start_time=desired_start_time,
        end_time=target_end_time,
        session=session,
    )

    if gap_start_time is not None:
        start_time = gap_start_time
        has_gap = True
    elif latest_local_timestamp is None:
        start_time = desired_start_time
        has_gap = True
    else:
        start_time = latest_local_timestamp + FIVE_MINUTES_MS
        has_gap = False

    return {
        'symbol': symbol,
        'series_type': series_type,
        'start_time': start_time,
        'end_time': target_end_time,
        'has_gap': has_gap and start_time <= target_end_time,
        'earliest_local_timestamp': earliest_local_timestamp,
        'latest_local_timestamp': latest_local_timestamp,
    }


def _find_first_missing_series_timestamp(symbol, series_type, start_time, end_time, session=None):
    """Return the first missing 5m timestamp in the requested range, if any."""
    if start_time > end_time:
        return None

    model = get_series_model(series_type)
    time_field_name = _get_time_field(series_type)
    time_field = getattr(model, time_field_name)

    own_session = session is None
    db = session or get_session()

    try:
        rows = (
            db.query(time_field)
            .filter(
                model.symbol == symbol,
                model.period == BINANCE_SERIES_REPAIR_PERIOD,
                time_field >= start_time,
                time_field <= end_time,
            )
            .order_by(time_field.asc())
            .all()
        )
        timestamps = {int(row[0]) for row in rows if row[0] is not None}
        expected_time = start_time

        while expected_time <= end_time:
            if expected_time not in timestamps:
                return expected_time
            expected_time += FIVE_MINUTES_MS

        return None
    finally:
        if own_session:
            db.close()


def _trim_series_after_target_end(symbol, series_type, target_end_time, session=None):
    """Delete local records that are newer than the latest closed 5m anchor."""
    model = get_series_model(series_type)
    time_field_name = _get_time_field(series_type)
    time_field = getattr(model, time_field_name)

    own_session = session is None
    db = session or get_session()

    try:
        deleted = (
            db.query(model)
            .filter(
                model.symbol == symbol,
                model.period == BINANCE_SERIES_REPAIR_PERIOD,
                time_field > target_end_time,
            )
            .delete(synchronize_session=False)
        )
        if deleted:
            db.commit()
        return deleted
    except Exception:
        db.rollback()
        raise
    finally:
        if own_session:
            db.close()


def _get_time_field(series_type):
    return 'open_time' if series_type == 'klines' else 'event_time'


def _get_page_limit(series_type):
    if series_type == 'klines':
        return BINANCE_SERIES_REPAIR_KLINES_PAGE_LIMIT
    return BINANCE_SERIES_REPAIR_FUTURES_PAGE_LIMIT


def _get_page_end_time(cursor_time, end_time, page_limit):
    page_span_ms = max(page_limit - 1, 0) * FIVE_MINUTES_MS
    return min(end_time, cursor_time + page_span_ms)


def repair_single_series(symbol, series_type, now_ms=None, http_session=None, db_session=None):
    current_time_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    window = build_repair_window(symbol, series_type, current_time_ms, session=db_session)

    latest_local_timestamp = window.get('latest_local_timestamp')
    end_time = window.get('end_time')
    if latest_local_timestamp is not None and end_time is not None and latest_local_timestamp > end_time:
        deleted = _trim_series_after_target_end(
            symbol=symbol,
            series_type=series_type,
            target_end_time=end_time,
            session=db_session,
        )
        logger.info(
            f"修补前清理未收盘序列: 币种={symbol}, 类型={series_type}, "
            f"清理数量={deleted}, 目标结束时间={end_time}"
        )
        window = build_repair_window(symbol, series_type, current_time_ms, session=db_session)

    logger.info(
        f"开始修补历史序列: 币种={symbol}, 类型={series_type}, 周期={BINANCE_SERIES_REPAIR_PERIOD}, "
        f"开始时间={window['start_time']}, 结束时间={window['end_time']}, 是否存在缺口={window['has_gap']}, "
        f"本地最早时间={window.get('earliest_local_timestamp')}, 本地最新时间={window.get('latest_local_timestamp')}"
    )

    if not window['has_gap']:
        logger.info(
            f"跳过历史序列修补: 币种={symbol}, 类型={series_type}, "
            f"原因=无缺口, 本地最新时间={window.get('latest_local_timestamp')}"
        )
        return {
            'symbol': symbol,
            'series_type': series_type,
            'period': BINANCE_SERIES_REPAIR_PERIOD,
            'status': 'skipped',
            'start_time': window['start_time'],
            'end_time': window['end_time'],
            'affected': 0,
            'pages': 0,
        }

    if series_type == 'taker_buy_sell_vol':
        return _repair_taker_buy_sell_vol(symbol, window, http_session, db_session, current_time_ms)

    page_limit = _get_page_limit(series_type)
    time_field = _get_time_field(series_type)
    cursor_time = window['start_time']
    affected = 0
    pages = 0
    request_pages = 0
    repaired_records = 0
    last_repaired_time = None

    while cursor_time <= window['end_time']:
        request_pages += 1
        page_end_time = _get_page_end_time(cursor_time, window['end_time'], page_limit)
        logger.info(
            f"修补分页请求: 币种={symbol}, 类型={series_type}, 页码={request_pages}, "
            f"游标时间={cursor_time}, 分页结束时间={page_end_time}, 窗口结束时间={window['end_time']}, 单页上限={page_limit}"
        )
        payload = fetch_series_payload(
            series_type=series_type,
            symbol=symbol,
            period=BINANCE_SERIES_REPAIR_PERIOD,
            limit=page_limit,
            session=http_session,
            start_time=cursor_time,
            end_time=page_end_time,
        )
        records = parse_series_payload(
            series_type=series_type,
            payload=payload,
            symbol=symbol,
            period=BINANCE_SERIES_REPAIR_PERIOD,
        )
        records = trim_unclosed_series_records(
            series_type=series_type,
            records=records,
            now_ms=current_time_ms,
            period=BINANCE_SERIES_REPAIR_PERIOD,
        )
        filtered_records = [
            record
            for record in records
            if cursor_time <= record[time_field] <= page_end_time
        ]
        next_cursor_time = page_end_time + FIVE_MINUTES_MS

        if not filtered_records:
            logger.info(
                f"修补分页未返回记录: 币种={symbol}, 类型={series_type}, "
                f"页码={request_pages}, 游标时间={cursor_time}, 分页结束时间={page_end_time}"
            )
            cursor_time = next_cursor_time
            continue

        page_affected = upsert_series_records(series_type, filtered_records, session=db_session)
        affected += page_affected
        repaired_records += len(filtered_records)
        pages += 1
        last_repaired_time = max(record[time_field] for record in filtered_records)

        logger.info(
            f"修补分页完成: 币种={symbol}, 类型={series_type}, 页码={request_pages}, "
            f"记录数={len(filtered_records)}, 影响行数={page_affected}, 最后时间={last_repaired_time}, "
            f"下一游标时间={next_cursor_time}"
        )

        if next_cursor_time <= cursor_time:
            logger.warning(
                f"修补游标未推进: 币种={symbol}, 类型={series_type}, "
                f"当前游标时间={cursor_time}, 下一游标时间={next_cursor_time}"
            )
            break

        cursor_time = next_cursor_time
        if BINANCE_SERIES_REPAIR_SLEEP_MS > 0 and cursor_time <= window['end_time']:
            time.sleep(BINANCE_SERIES_REPAIR_SLEEP_MS / 1000)

    logger.info(
        f"历史序列修补完成: 币种={symbol}, 类型={series_type}, 状态=成功, "
        f"成功分页数={pages}, 记录数={repaired_records}, 影响行数={affected}, 最后修补时间={last_repaired_time}"
    )
    return {
        'symbol': symbol,
        'series_type': series_type,
        'period': BINANCE_SERIES_REPAIR_PERIOD,
        'status': 'success',
        'start_time': window['start_time'],
        'end_time': window['end_time'],
        'affected': affected,
        'records': repaired_records,
        'pages': pages,
        'last_repaired_time': last_repaired_time,
    }


def repair_latest_series_point(symbol, series_type, now_ms=None, http_session=None, db_session=None):
    """只修补最新一个已收盘 5m 点，供首页高频定时任务使用。"""
    current_time_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    target_time = latest_closed_5m_open_time(current_time_ms)
    time_field = _get_time_field(series_type)
    limit = 2 if series_type == 'taker_buy_sell_vol' else 1
    payload = fetch_series_payload(
        series_type=series_type,
        symbol=symbol,
        period=BINANCE_SERIES_REPAIR_PERIOD,
        limit=limit,
        session=http_session,
        start_time=target_time,
        end_time=target_time,
    )
    records = parse_series_payload(series_type, payload, symbol, BINANCE_SERIES_REPAIR_PERIOD)
    records = trim_unclosed_series_records(
        series_type=series_type,
        records=records,
        now_ms=current_time_ms,
        period=BINANCE_SERIES_REPAIR_PERIOD,
    )
    filtered_records = [record for record in records if record.get(time_field) == target_time]
    affected = upsert_series_records(series_type, filtered_records, session=db_session) if filtered_records else 0
    logger.info(
        f"首页最新点修补完成: 币种={symbol}, 类型={series_type}, "
        f"目标时间={target_time}, 记录数={len(filtered_records)}, 影响行数={affected}"
    )
    return {
        'symbol': symbol,
        'series_type': series_type,
        'period': BINANCE_SERIES_REPAIR_PERIOD,
        'status': 'success',
        'target_time': target_time,
        'affected': affected,
        'records': len(filtered_records),
        'pages': 1 if filtered_records else 0,
    }


def _repair_taker_buy_sell_vol(symbol, window, http_session, db_session, current_time_ms):
    from sqlalchemy import func
    from coinx.models import BinanceTakerBuySellVol

    page_limit = _get_page_limit('taker_buy_sell_vol')
    target_start = window['start_time']
    target_end = window['end_time']
    earliest_local = window.get('earliest_local_timestamp')

    coverage_start = target_end - BINANCE_SERIES_REPAIR_COVERAGE_HOURS * 60 * 60 * 1000
    needs_backfill = earliest_local is None or earliest_local > coverage_start

    backfill_start = coverage_start if needs_backfill else earliest_local
    end_time = window['end_time']
    affected = 0
    pages = 0
    request_pages = 0
    repaired_records = 0
    last_repaired_time = None

    if needs_backfill:
        while True:
            request_pages += 1
            payload = fetch_series_payload(
                series_type='taker_buy_sell_vol',
                symbol=symbol,
                period=BINANCE_SERIES_REPAIR_PERIOD,
                limit=page_limit,
                session=http_session,
                end_time=end_time,
            )
            if not payload:
                logger.info(f"taker_buy_sell_vol 修补完成: 无更多数据")
                break

            records = parse_series_payload('taker_buy_sell_vol', payload, symbol, BINANCE_SERIES_REPAIR_PERIOD)
            records = trim_unclosed_series_records(
                series_type='taker_buy_sell_vol',
                records=records,
                now_ms=current_time_ms,
                period=BINANCE_SERIES_REPAIR_PERIOD,
            )
            filtered_records = [r for r in records if r['event_time'] >= backfill_start]

            if not filtered_records:
                break

            page_affected = upsert_series_records('taker_buy_sell_vol', filtered_records, session=db_session)
            affected += page_affected
            repaired_records += len(filtered_records)
            pages += 1
            last_repaired_time = max(r['event_time'] for r in filtered_records)

            oldest_time = min(r['event_time'] for r in filtered_records)
            logger.info(
                f"taker_buy_sell_vol 修补: 页码={request_pages}, 记录数={len(filtered_records)}, "
                f"最旧时间={oldest_time}, 最新时间={last_repaired_time}"
            )

            if oldest_time <= backfill_start:
                break

            end_time = oldest_time - 1

            if BINANCE_SERIES_REPAIR_SLEEP_MS > 0:
                time.sleep(BINANCE_SERIES_REPAIR_SLEEP_MS / 1000)

    gap_affected = _repair_taker_vol_gaps(symbol, coverage_start, target_end, http_session, db_session, current_time_ms)
    affected += gap_affected

    logger.info(
        f"taker_buy_sell_vol 修补完成: 币种={symbol}, 分页数={pages}, 记录数={repaired_records}, "
        f"影响行数={affected}, 最后时间={last_repaired_time}"
    )
    return {
        'symbol': symbol,
        'series_type': 'taker_buy_sell_vol',
        'period': BINANCE_SERIES_REPAIR_PERIOD,
        'status': 'success',
        'start_time': window['start_time'],
        'end_time': window['end_time'],
        'affected': affected,
        'records': repaired_records,
        'pages': pages,
        'last_repaired_time': last_repaired_time,
    }


def _repair_taker_vol_gaps(symbol, coverage_start, coverage_end, http_session, db_session, current_time_ms):
    from sqlalchemy import func
    from coinx.models import BinanceTakerBuySellVol

    own_session = db_session is None
    session = db_session or get_session()
    gap_affected = 0

    try:
        rows = (
            session.query(BinanceTakerBuySellVol.event_time)
            .filter(
                BinanceTakerBuySellVol.symbol == symbol,
                BinanceTakerBuySellVol.event_time >= coverage_start,
                BinanceTakerBuySellVol.event_time <= coverage_end,
            )
            .order_by(BinanceTakerBuySellVol.event_time)
            .all()
        )

        if not rows:
            return 0

        existing_times = set(r[0] for r in rows)

        missing_times = []
        current = coverage_start
        while current <= coverage_end:
            if current not in existing_times:
                missing_times.append(current)
            current += FIVE_MINUTES_MS

        if not missing_times:
            logger.info(f"taker_buy_sell_vol 间隙检测: 无缺失点")
            return 0

        logger.info(f"taker_buy_sell_vol 间隙检测: 发现 {len(missing_times)} 个缺失点")

        if not missing_times:
            return 0

        earliest_gap = min(missing_times)
        latest_gap = max(missing_times)
        end_time = latest_gap - 1
        page_count = 0

        while True:
            page_count += 1
            payload = fetch_series_payload(
                series_type='taker_buy_sell_vol',
                symbol=symbol,
                period=BINANCE_SERIES_REPAIR_PERIOD,
                limit=500,
                session=http_session,
                end_time=end_time,
            )

            if not payload:
                break

            records = parse_series_payload('taker_buy_sell_vol', payload, symbol, BINANCE_SERIES_REPAIR_PERIOD)
            records = trim_unclosed_series_records(
                series_type='taker_buy_sell_vol',
                records=records,
                now_ms=current_time_ms,
                period=BINANCE_SERIES_REPAIR_PERIOD,
            )

            new_records = [r for r in records if r['event_time'] >= coverage_start and r['event_time'] <= coverage_end]

            if not new_records:
                break

            page_affected = upsert_series_records('taker_buy_sell_vol', new_records, session=session)
            gap_affected += page_affected

            oldest_time = min(r['event_time'] for r in new_records)
            logger.info(f"taker_buy_sell_vol 间隙修补: 页码={page_count}, 新增={len(new_records)}, 最旧时间={oldest_time}")

            if oldest_time <= earliest_gap:
                break

            end_time = oldest_time - 1

            if BINANCE_SERIES_REPAIR_SLEEP_MS > 0:
                time.sleep(BINANCE_SERIES_REPAIR_SLEEP_MS / 1000)

        logger.info(f"taker_buy_sell_vol 间隙修补完成: 新增 {gap_affected} 条记录")

    finally:
        if own_session:
            session.close()

    return gap_affected


def repair_tracked_symbols(symbols=None, series_types=None, now_ms=None, http_session=None, db_session=None):
    tracked_symbols = symbols if symbols else get_active_coins()
    active_series_types = series_types or DEFAULT_REPAIR_SERIES_TYPES
    results = []
    total_tasks = len(tracked_symbols) * len(active_series_types)

    logger.info(
        f"开始修补历史序列: 币种数量={len(tracked_symbols)}, "
        f"序列类型={active_series_types}, 总任务数={total_tasks}, 周期={BINANCE_SERIES_REPAIR_PERIOD}"
    )

    task_index = 0

    for symbol in tracked_symbols:
        for series_type in active_series_types:
            task_index += 1
            logger.info(
                f"修补进度: 任务={task_index}/{total_tasks}, "
                f"币种={symbol}, 类型={series_type}"
            )
            try:
                result = repair_single_series(
                    symbol=symbol,
                    series_type=series_type,
                    now_ms=now_ms,
                    http_session=http_session,
                    db_session=db_session,
                )
                results.append(result)
                logger.info(
                    f"修补结果: 任务={task_index}/{total_tasks}, 币种={symbol}, 类型={series_type}, "
                    f"状态={result.get('status')}, 影响行数={result.get('affected', 0)}, 分页数={result.get('pages', 0)}"
                )
            except Exception as exc:
                logger.error(f'Binance 历史序列修补失败: symbol={symbol}, type={series_type}, error={exc}')
                results.append(
                    {
                        'symbol': symbol,
                        'series_type': series_type,
                        'period': BINANCE_SERIES_REPAIR_PERIOD,
                        'status': 'error',
                        'error': str(exc),
                    }
                )

    success_count = sum(1 for item in results if item.get('status') == 'success')
    failure_count = sum(1 for item in results if item.get('status') == 'error')
    skipped_count = sum(1 for item in results if item.get('status') == 'skipped')

    logger.info(
        f"历史序列修补完成: 总任务数={total_tasks}, 成功={success_count}, "
        f"失败={failure_count}, 跳过={skipped_count}"
    )

    return {
        'status': 'success',
        'symbols': tracked_symbols,
        'series_types': active_series_types,
        'period': BINANCE_SERIES_REPAIR_PERIOD,
        'success_count': success_count,
        'failure_count': failure_count,
        'skipped_count': skipped_count,
        'results': results,
    }


def repair_latest_tracked_symbols(symbols=None, series_types=None, now_ms=None, http_session=None, db_session=None):
    """批量修补首页最新一个已收盘点，不做历史缺口回填。"""
    tracked_symbols = symbols if symbols else get_active_coins()
    active_series_types = series_types or DEFAULT_REPAIR_SERIES_TYPES
    results = []
    total_tasks = len(tracked_symbols) * len(active_series_types)

    logger.info(
        f"开始修补首页最新点: 币种数量={len(tracked_symbols)}, "
        f"序列类型={active_series_types}, 总任务数={total_tasks}, 周期={BINANCE_SERIES_REPAIR_PERIOD}"
    )

    for symbol in tracked_symbols:
        for series_type in active_series_types:
            try:
                result = repair_latest_series_point(
                    symbol=symbol,
                    series_type=series_type,
                    now_ms=now_ms,
                    http_session=http_session,
                    db_session=db_session,
                )
                results.append(result)
            except Exception as exc:
                logger.error(f'首页最新点修补失败: symbol={symbol}, type={series_type}, error={exc}')
                results.append(
                    {
                        'symbol': symbol,
                        'series_type': series_type,
                        'period': BINANCE_SERIES_REPAIR_PERIOD,
                        'status': 'error',
                        'error': str(exc),
                    }
                )

    success_count = sum(1 for item in results if item.get('status') == 'success')
    failure_count = sum(1 for item in results if item.get('status') == 'error')
    logger.info(
        f"首页最新点修补完成: 总任务数={total_tasks}, 成功={success_count}, 失败={failure_count}"
    )
    return {
        'status': 'success' if failure_count == 0 else 'partial_success',
        'symbols': tracked_symbols,
        'series_types': active_series_types,
        'period': BINANCE_SERIES_REPAIR_PERIOD,
        'success_count': success_count,
        'failure_count': failure_count,
        'skipped_count': 0,
        'results': results,
    }


def run_series_repair_job():
    if not BINANCE_SERIES_REPAIR_ENABLED:
        return {
            'status': 'skipped',
            'message': 'Binance 历史序列修补任务未启用',
        }

    summary = repair_tracked_symbols()
    summary.setdefault('status', 'success')
    return summary
