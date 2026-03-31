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
from coinx.repositories.binance_series import (
    get_earliest_series_timestamp as get_earliest_series_timestamp_from_repo,
    get_latest_series_timestamp as get_latest_series_timestamp_from_repo,
    upsert_series_records,
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
]


def floor_to_completed_5m(now_ms):
    return now_ms - (now_ms % FIVE_MINUTES_MS)


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
    target_end_time = floor_to_completed_5m(now_ms)
    latest_local_timestamp = get_latest_series_timestamp(symbol, series_type, session=session)
    earliest_local_timestamp = get_earliest_series_timestamp(symbol, series_type, session=session)
    bootstrap_start_time = target_end_time - BINANCE_SERIES_REPAIR_BOOTSTRAP_DAYS * 24 * 60 * 60 * 1000
    coverage_start_time = target_end_time - BINANCE_SERIES_REPAIR_COVERAGE_HOURS * 60 * 60 * 1000
    desired_start_time = min(bootstrap_start_time, coverage_start_time)

    needs_head_backfill = (
        earliest_local_timestamp is None
        or earliest_local_timestamp > coverage_start_time
    )
    needs_tail_repair = (
        latest_local_timestamp is None
        or latest_local_timestamp < target_end_time
    )

    if latest_local_timestamp is None:
        start_time = desired_start_time
    elif needs_head_backfill:
        start_time = desired_start_time
    else:
        start_time = latest_local_timestamp + FIVE_MINUTES_MS

    has_gap = needs_head_backfill or needs_tail_repair

    return {
        'symbol': symbol,
        'series_type': series_type,
        'start_time': start_time,
        'end_time': target_end_time,
        'has_gap': has_gap and start_time <= target_end_time,
        'earliest_local_timestamp': earliest_local_timestamp,
        'latest_local_timestamp': latest_local_timestamp,
    }


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


def repair_tracked_symbols(symbols=None, series_types=None, now_ms=None, http_session=None, db_session=None):
    tracked_symbols = get_active_coins(filter_symbols=symbols) if symbols else get_active_coins()
    active_series_types = series_types or DEFAULT_REPAIR_SERIES_TYPES
    results = []
    total_tasks = len(tracked_symbols) * len(active_series_types)

    logger.info(
        f"开始修补已跟踪币种历史序列: 币种数量={len(tracked_symbols)}, "
        f"序列类型={active_series_types}, 总任务数={total_tasks}, 周期={BINANCE_SERIES_REPAIR_PERIOD}"
    )

    task_index = 0

    for symbol in tracked_symbols:
        for series_type in active_series_types:
            task_index += 1
            logger.info(
                f"已跟踪币种修补进度: 任务={task_index}/{total_tasks}, "
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
                    f"已跟踪币种修补结果: 任务={task_index}/{total_tasks}, 币种={symbol}, 类型={series_type}, "
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
        f"已跟踪币种历史序列修补完成: 总任务数={total_tasks}, 成功={success_count}, "
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


def run_series_repair_job():
    if not BINANCE_SERIES_REPAIR_ENABLED:
        return {
            'status': 'skipped',
            'message': 'Binance 历史序列修补任务未启用',
        }

    summary = repair_tracked_symbols()
    summary.setdefault('status', 'success')
    return summary
