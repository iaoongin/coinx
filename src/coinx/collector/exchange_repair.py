import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from coinx.coin_manager import get_active_coins
from coinx.collector.exchange_adapters import get_exchange_adapters
from coinx.collector.binance.client import BinanceRateLimitUnavailable, is_binance_budget_unavailable
from coinx.collector.bybit.series import BybitRateLimitUnavailable, is_bybit_budget_unavailable
from coinx.collector.gate.series import GateRateLimitUnavailable, GateUnsupportedContract, is_gate_budget_unavailable
from coinx.collector.okx.series import OKXRateLimitUnavailable, is_okx_budget_unavailable
from coinx.collector.rate_limit import RateLimitUnavailable
from coinx.collector.timing import (
    add_duration_breakdown,
    attach_other_duration,
    empty_duration_breakdown,
    format_duration_breakdown,
    format_duration_ms,
    round_duration_breakdown,
    sum_duration_breakdowns,
    timed_category,
)
from coinx.config import (
    ENABLED_EXCHANGES,
    HOMEPAGE_SERIES_REPAIR_PERIOD,
    HOMEPAGE_SERIES_TYPES,
    REPAIR_HISTORY_COVERAGE_HOURS,
    REPAIR_HISTORY_MAX_WORKERS,
    REPAIR_HISTORY_SYMBOL_BATCH_SIZE,
    REPAIR_HISTORY_WRITE_BATCH_SIZE,
    REPAIR_ROLLING_MAX_WORKERS,
    REPAIR_ROLLING_POINTS,
    REPAIR_ROLLING_WRITE_BATCH_SIZE,
)
from coinx.database import get_session
from coinx.repositories.series import (
    get_existing_series_timestamps,
    upsert_series_records_in_batches,
)
from coinx.utils import logger


FIVE_MINUTES_MS = 5 * 60 * 1000
LOCAL_DAY_OFFSET_MS = 8 * 60 * 60 * 1000
_history_symbol_cursor = 0
_RATE_LIMIT_EXCEPTIONS = (
    GateRateLimitUnavailable,
    OKXRateLimitUnavailable,
    BybitRateLimitUnavailable,
    BinanceRateLimitUnavailable,
    RateLimitUnavailable,
)


def _is_exchange_budget_unavailable(exchange):
    return _exchange_budget_unavailable_seconds(exchange) > 0


def _exchange_budget_unavailable_seconds(exchange):
    if exchange == 'gate':
        from coinx.collector.gate.series import _gate_budget_unavailable_remaining_seconds
        return _gate_budget_unavailable_remaining_seconds()
    if exchange == 'okx':
        from coinx.collector.okx.series import _okx_rate_limits
        return max(
            _okx_rate_limits.unavailable_remaining_seconds('okx', 'rubik'),
            _okx_rate_limits.unavailable_remaining_seconds('okx', 'default'),
        )
    if exchange == 'bybit':
        from coinx.collector.bybit.series import _bybit_rate_limits
        return _bybit_rate_limits.unavailable_remaining_seconds('bybit', 'market')
    if exchange == 'binance':
        from coinx.collector.binance.client import _binance_rate_limits
        return _binance_rate_limits.unavailable_remaining_seconds('binance', 'default')
    return 0.0


def _budget_unavailable_reason(exchange):
    return f'{exchange}_budget_unavailable'


def _period_to_ms(period):
    if period.endswith('m'):
        return int(period[:-1]) * 60 * 1000
    if period.endswith('h'):
        return int(period[:-1]) * 60 * 60 * 1000
    if period.endswith('H'):
        return int(period[:-1]) * 60 * 60 * 1000
    if period.endswith('d'):
        return int(period[:-1]) * 24 * 60 * 60 * 1000
    if period.endswith('D'):
        return int(period[:-1]) * 24 * 60 * 60 * 1000
    raise ValueError(f'unsupported repair period: {period}')


def latest_closed_period_open_time(now_ms, period):
    period_ms = _period_to_ms(period)
    return max(0, now_ms - (now_ms % period_ms) - period_ms)


def latest_closed_5m_open_time(now_ms):
    return latest_closed_period_open_time(now_ms, '5m')


def _get_time_field(series_type):
    return 'open_time' if series_type == 'klines' else 'event_time'


def _build_rolling_target_times(now_ms, points, period='5m'):
    period_ms = _period_to_ms(period)
    latest_time = latest_closed_period_open_time(now_ms, period)
    return [
        latest_time - offset * period_ms
        for offset in range(max(1, int(points or 1)))
        if latest_time - offset * period_ms >= 0
    ]


def _build_target_times_in_range(start_time, end_time, period='5m'):
    if start_time > end_time:
        return []
    period_ms = _period_to_ms(period)
    return list(range(start_time, end_time + 1, period_ms))


def _local_day_start_time(timestamp_ms):
    day_ms = _period_to_ms('1d')
    return ((timestamp_ms + LOCAL_DAY_OFFSET_MS) // day_ms) * day_ms - LOCAL_DAY_OFFSET_MS


def _build_history_day_segments(now_ms, period, coverage_hours):
    latest_time = latest_closed_period_open_time(now_ms, period)
    local_today_start = _local_day_start_time(now_ms)
    day_ms = _period_to_ms('1d')
    period_ms = _period_to_ms(period)
    days_back = max(0, (int(coverage_hours or 0) + 23) // 24)

    segments = []
    for offset in range(days_back, 0, -1):
        day_start = local_today_start - offset * day_ms
        if day_start < 0:
            day_start = 0
        day_end = local_today_start - (offset - 1) * day_ms - period_ms
        if day_start <= day_end:
            segments.append((day_start, day_end))

    if local_today_start <= latest_time:
        segments.append((max(0, local_today_start), latest_time))

    return segments


def _build_history_window_bounds(now_ms, period, coverage_hours):
    segments = _build_history_day_segments(now_ms, period, coverage_hours)
    if not segments:
        return []
    return [segments[0][0], segments[-1][1]]


def _format_history_missing_day_stats(stats_by_exchange):
    if not stats_by_exchange:
        return '无'
    parts = []
    for exchange in sorted(stats_by_exchange):
        stats = stats_by_exchange[exchange]
        by_type_parts = []
        for series_type in sorted(stats.get('by_type') or {}):
            type_stats = stats['by_type'][series_type]
            by_type_parts.append(
                f"{series_type}(币种={len(type_stats.get('symbols') or set())},缺天={type_stats.get('days', 0)})"
            )
        parts.append(
            f"{exchange}(币种={len(stats.get('symbols') or set())},"
            f"类型={len(stats.get('by_type') or {})},"
            f"缺天={stats.get('days', 0)},"
            f"按类型={ '|'.join(by_type_parts) if by_type_parts else '无' })"
        )
    return '; '.join(parts)


def _record_history_missing_day_stats(stats_by_exchange, exchange, symbol, series_type, day_count):
    if day_count <= 0:
        return
    exchange_stats = stats_by_exchange.setdefault(
        exchange,
        {'symbols': set(), 'days': 0, 'by_type': {}},
    )
    exchange_stats['symbols'].add(symbol)
    exchange_stats['days'] += day_count
    type_stats = exchange_stats['by_type'].setdefault(series_type, {'symbols': set(), 'days': 0})
    type_stats['symbols'].add(symbol)
    type_stats['days'] += day_count


def _result_with_breakdown(result, breakdown):
    result['duration_breakdown_ms'] = round_duration_breakdown(breakdown)
    return result


def _build_grouped_duration_breakdowns(results, field):
    grouped = {}
    for item in results or []:
        group_key = item.get(field)
        if not group_key:
            continue
        grouped.setdefault(group_key, empty_duration_breakdown())
        add_duration_breakdown(grouped[group_key], item.get('duration_breakdown_ms'))
    return {key: round_duration_breakdown(value) for key, value in grouped.items()}


def _chunks(items, size):
    size = max(1, int(size or 1))
    for index in range(0, len(items), size):
        yield items[index:index + size]


def _group_contiguous_times(times, period='5m'):
    period_ms = _period_to_ms(period)
    groups = []
    for timestamp in sorted(set(times)):
        if not groups or timestamp != groups[-1][-1] + period_ms:
            groups.append([timestamp])
        else:
            groups[-1].append(timestamp)
    return groups


def _trim_unclosed_records(series_type, records, now_ms, period=HOMEPAGE_SERIES_REPAIR_PERIOD):
    if not records:
        return records
    if series_type == 'klines':
        return [record for record in records if record.get('close_time') is not None and record.get('close_time') <= now_ms]
    cutoff_time = latest_closed_period_open_time(now_ms, period)
    time_field = _get_time_field(series_type)
    return [record for record in records if record.get(time_field) is not None and record.get(time_field) <= cutoff_time]


def _page_limit(series_type, adapter=None):
    if adapter is not None and hasattr(adapter, 'page_limit'):
        adapter_limit = adapter.page_limit(series_type)
        if adapter_limit:
            return adapter_limit
    return 1000 if series_type == 'klines' else 500


def _page_end_time(start_time, end_time, page_limit, period='5m'):
    return min(end_time, start_time + max(page_limit - 1, 0) * _period_to_ms(period))


def _uses_open_ended_history_paging(adapter, series_type):
    return (
        getattr(adapter, 'exchange_id', None) == 'gate'
        and series_type == 'open_interest_hist'
    )


def _uses_backward_window_history_paging(adapter, series_type):
    return (
        getattr(adapter, 'exchange_id', None) == 'okx'
        and series_type == 'open_interest_hist'
    )


def resolve_repair_worker_count(exchanges=None, max_workers=None):
    resolved_exchanges = list(exchanges or ENABLED_EXCHANGES)
    if max_workers is not None:
        return max(1, int(max_workers))
    return max(1, len(resolved_exchanges))


def _active_series_types(adapter, series_types):
    requested_types = tuple(series_types or HOMEPAGE_SERIES_TYPES)
    return [series_type for series_type in requested_types if series_type in adapter.supported_series_types]


def _build_history_gap_tasks(exchange, symbol, series_type, period, day_segments, session=None):
    target_times = []
    for segment_start, segment_end in day_segments:
        target_times.extend(_build_target_times_in_range(segment_start, segment_end, period=period))
    if not target_times:
        return []

    existing_by_symbol = get_existing_series_timestamps(
        exchange,
        series_type,
        [symbol],
        target_times,
        period=period,
        session=session,
    )
    existing_times = existing_by_symbol.get(symbol, set())
    gap_tasks = []
    for segment_start, segment_end in day_segments:
        segment_target_times = _build_target_times_in_range(segment_start, segment_end, period=period)
        if not segment_target_times:
            continue
        if all(timestamp in existing_times for timestamp in segment_target_times):
            continue
        gap_tasks.append(
            {
                'exchange': exchange,
                'symbol': symbol,
                'series_type': series_type,
                'period': period,
                'start_time': segment_start,
                'end_time': segment_target_times[-1],
            }
        )
    return gap_tasks


def _unsupported_symbol_result(adapter, symbol, series_type, mode, window_precise, extra=None):
    result = {
        'exchange': adapter.exchange_id,
        'symbol': symbol,
        'series_type': series_type,
        'period': HOMEPAGE_SERIES_REPAIR_PERIOD,
        'status': 'skipped',
        'mode': mode,
        'reason': 'unsupported_symbol',
        'window_precise': window_precise,
        'affected': 0,
        'records': 0,
        'expected_records': 0,
        'api_records': 0,
        'written_records': 0,
        'pages': 0,
    }
    if extra:
        result.update(extra)
    return _result_with_breakdown(result, result.get('duration_breakdown_ms') or {})


def _supported_symbol_lookup_failed_result(adapter, symbol, series_type, mode, window_precise, error, extra=None):
    result = {
        'exchange': adapter.exchange_id,
        'symbol': symbol,
        'series_type': series_type,
        'period': HOMEPAGE_SERIES_REPAIR_PERIOD,
        'status': 'skipped',
        'mode': mode,
        'reason': 'supported_symbol_lookup_failed',
        'window_precise': window_precise,
        'affected': 0,
        'records': 0,
        'expected_records': 0,
        'api_records': 0,
        'written_records': 0,
        'pages': 0,
        'error': str(error),
    }
    if extra:
        result.update(extra)
    return _result_with_breakdown(result, result.get('duration_breakdown_ms') or {})


def _sample_values(values, limit=8):
    values = sorted(set(values))
    if len(values) <= limit:
        return ','.join(values)
    return f"{','.join(values[:limit])}...(+{len(values) - limit})"


def _summarize_results(results):
    return {
        'success_count': sum(1 for item in results if item.get('status') == 'success'),
        'failure_count': sum(1 for item in results if item.get('status') == 'error'),
        'skipped_count': sum(1 for item in results if item.get('status') == 'skipped'),
        'expected_records': sum(item.get('expected_records') or 0 for item in results),
        'api_records': sum(item.get('api_records') or 0 for item in results),
        'records': sum(item.get('records') or 0 for item in results),
        'missing_records': sum(
            max((item.get('expected_records') or 0) - (item.get('records') or 0), 0)
            for item in results
        ),
        'no_data_records': sum(item.get('no_data_records') or 0 for item in results),
        'written_records': sum(item.get('written_records') or 0 for item in results),
        'affected': sum(item.get('affected') or 0 for item in results),
    }


def _reason_label(reason):
    mapping = {
        'unsupported_symbol': '不支持币种',
        'supported_symbol_lookup_failed': '币种支持检查失败',
        'no_data': '无可用数据',
    }
    if reason and str(reason).endswith('_budget_unavailable'):
        return '限流冷却中'
    return mapping.get(reason, reason or '未知')


def _format_series_counts(series_counts):
    if not series_counts:
        return '无'
    return ','.join(f'{series_type}:{count}' for series_type, count in sorted(series_counts.items()))


def _format_reason_counts(results):
    counts = {}
    for item in results:
        if item.get('status') != 'skipped':
            continue
        label = _reason_label(item.get('reason'))
        counts[label] = counts.get(label, 0) + 1
    if not counts:
        return '无'
    return ','.join(f'{label}={count}' for label, count in sorted(counts.items()))


def _format_exchange_result_summary(results, exchanges):
    parts = []
    for exchange in exchanges:
        exchange_results = [item for item in results if item.get('exchange') == exchange]
        parts.append(
            f"{exchange}(成功={sum(1 for item in exchange_results if item.get('status') == 'success')},"
            f"失败={sum(1 for item in exchange_results if item.get('status') == 'error')},"
            f"跳过={sum(1 for item in exchange_results if item.get('status') == 'skipped')})"
        )
    return '; '.join(parts) if parts else '无'


def _log_result_volume_stats(mode, exchange, stats):
    logger.info(
        '数据量统计: 模式=%s 交易所=%s 缺口记录=%d 目标命中=%d 未命中缺口=%d 无数据缺口=%d 写库记录=%d 影响行=%d API返回=%d',
        mode,
        exchange or 'all',
        stats.get('expected_records', 0),
        stats.get('records', 0),
        stats.get('missing_records', 0),
        stats.get('no_data_records', 0),
        stats.get('written_records', 0),
        stats.get('affected', 0),
        stats.get('api_records', 0),
    )


def _log_result_volume_stats_by_series(mode, results):
    grouped = {}
    for item in results or []:
        key = (item.get('exchange') or 'unknown', item.get('series_type') or 'unknown')
        stats = grouped.setdefault(
            key,
            {
                'expected_records': 0,
                'records': 0,
                'missing_records': 0,
                'no_data_records': 0,
                'written_records': 0,
                'affected': 0,
                'api_records': 0,
            },
        )
        for field in stats:
            if field == 'missing_records':
                stats[field] += max((item.get('expected_records') or 0) - (item.get('records') or 0), 0)
            else:
                stats[field] += item.get(field) or 0

    for (exchange, series_type), stats in sorted(grouped.items()):
        logger.info(
            '数据量统计明细: 模式=%s 交易所=%s 序列类型=%s 缺口记录=%d 目标命中=%d 未命中缺口=%d 无数据缺口=%d 写库记录=%d 影响行=%d API返回=%d',
            mode,
            exchange,
            series_type,
            stats['expected_records'],
            stats['records'],
            stats['missing_records'],
            stats['no_data_records'],
            stats['written_records'],
            stats['affected'],
            stats['api_records'],
        )


def _format_exchange_progress(stats_by_exchange):
    parts = []
    for exchange in sorted(stats_by_exchange):
        stats = stats_by_exchange.get(exchange) or {}
        parts.append(
            f"{exchange}(支持={stats.get('supported_symbols', 0)},"
            f"已完整={stats.get('complete', 0)},"
            f"待修补={stats.get('pending', 0)},"
            f"不支持={stats.get('unsupported', 0)})"
        )
    return '; '.join(parts)


def _log_repair_summary(summary):
    extra_parts = []
    if summary.get('mode') == 'rolling':
        extra_parts.extend(
            [
                f"预检已完整={summary.get('precheck_skipped_count', 0)}",
                f"待修补任务={summary.get('pending_task_count', 0)}",
                f"不支持数量={summary.get('unsupported_count', 0)}",
            ]
        )
    if summary.get('mode') == 'history':
        extra_parts.extend(
            [
                f"预检已完整={summary.get('precheck_skipped_count', 0)}",
                f"待修补任务={summary.get('pending_task_count', 0)}",
                f"当天裁剪截止={summary.get('current_day_trimmed_end_time')}",
                f"缺口分布={summary.get('history_missing_day_stats')}",
                f"覆盖时长={summary.get('coverage_hours')}",
                f"是否全量扫描={'是' if summary.get('full_scan') else '否'}",
                f"修补窗口={summary.get('start_time')}~{summary.get('end_time')}",
            ]
        )

    message = (
        f"修补完成: 模式={summary['mode']} "
        f"交易所={','.join(summary['exchanges'])} "
        f"币种数={len(summary['symbols'])} "
        f"序列类型={','.join(summary['series_types'])} "
        f"成功={summary['success_count']} "
        f"失败={summary['failure_count']} "
        f"跳过={summary['skipped_count']} "
        f"缺口记录={summary.get('expected_records', 0)} "
        f"目标命中={summary.get('records', 0)} "
        f"未命中缺口={summary.get('missing_records', 0)} "
        f"无数据缺口={summary.get('no_data_records', 0)} "
        f"写库记录={summary.get('written_records', 0)} "
        f"影响行={summary.get('affected', 0)} "
        f"API返回={summary.get('api_records', 0)} "
        f"跳过原因={_format_reason_counts(summary.get('results') or [])} "
        f"各交易所={_format_exchange_result_summary(summary.get('results') or [], summary.get('exchanges') or [])} "
        f"耗时={format_duration_ms(summary.get('duration_ms'))} "
        f"累计耗时分类={format_duration_breakdown(summary.get('duration_breakdown_ms'))}"
    )
    if extra_parts:
        message = f"{message} {' '.join(extra_parts)}"
    logger.info(message)
    _log_result_volume_stats(summary.get('mode'), 'all', _summarize_results(summary.get('results') or []))
    _log_result_volume_stats_by_series(summary.get('mode'), summary.get('results') or [])

    unsupported_groups = {}
    supported_lookup_failed_groups = {}
    for item in summary.get('results', []):
        if item.get('status') != 'skipped':
            continue
        key = (item.get('exchange'), item.get('series_type'))
        if item.get('reason') == 'unsupported_symbol':
            unsupported_groups.setdefault(key, []).append(item.get('symbol'))
        if item.get('reason') == 'supported_symbol_lookup_failed':
            supported_lookup_failed_groups.setdefault(key, []).append(item.get('symbol'))

    for (exchange, series_type), symbols in sorted(unsupported_groups.items()):
        logger.info(
            '修补跳过汇总: 原因=不支持币种 交易所=%s 序列类型=%s 数量=%d 币种摘要=%s',
            exchange,
            series_type,
            len(symbols),
            _sample_values(symbols),
        )
    for (exchange, series_type), symbols in sorted(supported_lookup_failed_groups.items()):
        logger.warning(
            '修补跳过汇总: 原因=币种支持检查失败 交易所=%s 序列类型=%s 数量=%d 币种摘要=%s',
            exchange,
            series_type,
            len(symbols),
            _sample_values(symbols),
        )


def _task_progress_label(task):
    return f"{task.get('symbol')}/{task.get('series_type')}/{task.get('period')}"


def _should_log_task_progress(completed, total):
    return completed <= 3 or completed == total or completed % 10 == 0


def _log_task_progress(mode, exchange, completed, total, results, task, started_at):
    if not _should_log_task_progress(completed, total):
        return
    stats = _summarize_results(results)
    logger.info(
        '任务进度: 模式=%s 交易所=%s 已完成=%d/%d 成功=%d 失败=%d 跳过=%d 当前=%s 耗时=%s',
        mode,
        exchange or task.get('exchange'),
        completed,
        total,
        stats['success_count'],
        stats['failure_count'],
        stats['skipped_count'],
        _task_progress_label(task),
        format_duration_ms((time.perf_counter() - started_at) * 1000),
    )


def _run_tasks(tasks, worker_func, max_workers, db_session=None, mode=None, exchange=None):
    if not tasks:
        return []
    worker_count = max(1, int(max_workers or 1))
    started_at = time.perf_counter()
    total = len(tasks)
    if db_session is not None or worker_count <= 1:
        results = []
        for task in tasks:
            results.append(worker_func(task, db_session=db_session))
            if mode:
                _log_task_progress(mode, exchange, len(results), total, results, task, started_at)
        return results

    results = []
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_task = {executor.submit(worker_func, task, db_session=None): task for task in tasks}
        for future in as_completed(future_to_task):
            results.append(future.result())
            if mode:
                _log_task_progress(mode, exchange, len(results), total, results, future_to_task[future], started_at)
    return results


def _filter_budget_unavailable_rolling_tasks(tasks):
    if not tasks:
        return tasks, []

    runnable = []
    skipped = []
    for task in tasks:
        exchange = task.get('exchange')
        cooldown_seconds = _exchange_budget_unavailable_seconds(exchange)
        if cooldown_seconds <= 0:
            runnable.append(task)
            continue
        skipped.append(
            _result_with_breakdown(
                {
                    'exchange': exchange,
                    'symbol': task['symbol'],
                    'series_type': task['series_type'],
                    'period': task['period'],
                    'status': 'skipped',
                    'mode': 'rolling',
                    'reason': _budget_unavailable_reason(exchange),
                    'window_precise': task['adapter'].supports_time_window(task['series_type']),
                    'affected': 0,
                    'records': 0,
                    'expected_records': len(task.get('target_times') or []),
                    'api_records': 0,
                    'written_records': 0,
                    'pages': 0,
                    'target_times': sorted(task.get('target_times') or []),
                    'cooldown_skip_ms': cooldown_seconds * 1000,
                },
                {'cooldown_skip_ms': cooldown_seconds * 1000},
            )
        )

    if skipped:
        grouped = {}
        for item in skipped:
            grouped.setdefault(item['exchange'], []).append(item['symbol'])
        for exchange, symbols in sorted(grouped.items()):
            logger.warning(
                '交易所限流冷却中，跳过剩余任务: 模式=rolling 交易所=%s 原因=限流冷却中 跳过数量=%d 币种摘要=%s',
                exchange,
                len(symbols),
                _sample_values(symbols),
            )
    return runnable, skipped


def _filter_budget_unavailable_tasks(tasks, mode):
    if mode == 'rolling':
        return _filter_budget_unavailable_rolling_tasks(tasks)

    if not tasks:
        return tasks, []

    runnable = []
    skipped = []
    for task in tasks:
        exchange = task.get('exchange')
        cooldown_seconds = _exchange_budget_unavailable_seconds(exchange)
        if cooldown_seconds <= 0:
            runnable.append(task)
            continue
        skipped.append(
            _result_with_breakdown(
                {
                    'exchange': exchange,
                    'symbol': task['symbol'],
                    'series_type': task['series_type'],
                    'period': task['period'],
                    'status': 'skipped',
                    'mode': mode,
                    'reason': _budget_unavailable_reason(exchange),
                    'window_precise': task['adapter'].supports_time_window(task['series_type']),
                    'affected': 0,
                    'records': 0,
                    'expected_records': len(_build_target_times_in_range(task.get('start_time'), task.get('end_time'), period=task.get('period'))),
                    'api_records': 0,
                    'written_records': 0,
                    'pages': 0,
                    'start_time': task.get('start_time'),
                    'end_time': task.get('end_time'),
                    'cooldown_skip_ms': cooldown_seconds * 1000,
                },
                {'cooldown_skip_ms': cooldown_seconds * 1000},
            )
        )

    if skipped:
        grouped = {}
        for item in skipped:
            grouped.setdefault(item['exchange'], []).append(item['symbol'])
        for exchange, symbols in sorted(grouped.items()):
            logger.warning(
                '交易所限流冷却中，跳过剩余任务: 模式=%s 交易所=%s 原因=限流冷却中 跳过数量=%d 币种摘要=%s',
                mode,
                exchange,
                len(symbols),
                _sample_values(symbols),
            )
    return runnable, skipped


def _flush_group_records(exchange, group_results, db_session=None, mode='rolling'):
    pending_by_series = {}
    result_refs_by_series = {}
    for result in group_results:
        pending_records = result.pop('pending_records', None) or []
        if not pending_records:
            continue
        series_type = result.get('series_type')
        pending_by_series.setdefault(series_type, []).extend(pending_records)
        result_refs_by_series.setdefault(series_type, []).append((result, len(pending_records)))

    for series_type, records in pending_by_series.items():
        write_breakdown = empty_duration_breakdown()
        affected = 0
        batch_size = REPAIR_HISTORY_WRITE_BATCH_SIZE if mode == 'history' else REPAIR_ROLLING_WRITE_BATCH_SIZE
        logger.info(
            '批量写入开始: 模式=%s 交易所=%s 序列类型=%s 记录数=%d batch_size=%d',
            mode,
            exchange,
            series_type,
            len(records),
            batch_size,
        )
        with timed_category(write_breakdown, 'db_write_ms'):
            affected += upsert_series_records_in_batches(
                exchange,
                series_type,
                records,
                batch_size=batch_size,
                session=db_session,
            )
        logger.info(
            '批量写入完成: 模式=%s 交易所=%s 序列类型=%s 记录数=%d 影响行=%d 耗时=%s',
            mode,
            exchange,
            series_type,
            len(records),
            affected,
            format_duration_ms(write_breakdown.get('db_write_ms', 0.0)),
        )

        refs = result_refs_by_series.get(series_type) or []
        total_records = sum(record_count for _, record_count in refs)
        allocated_affected = 0
        allocated_write_ms = 0.0
        db_write_ms = write_breakdown.get('db_write_ms', 0.0)
        for index, (result, record_count) in enumerate(refs):
            if index == len(refs) - 1:
                result_affected = max(0, affected - allocated_affected)
                result_write_ms = max(0.0, db_write_ms - allocated_write_ms)
            else:
                ratio = record_count / total_records if total_records else 0
                result_affected = int(round(affected * ratio))
                result_write_ms = db_write_ms * ratio
                allocated_affected += result_affected
                allocated_write_ms += result_write_ms
            result['affected'] = result_affected
            result['written_records'] = record_count
            breakdown = result.get('duration_breakdown_ms') or empty_duration_breakdown()
            add_duration_breakdown(breakdown, {'db_write_ms': result_write_ms})
            result['duration_breakdown_ms'] = round_duration_breakdown(breakdown)

    return group_results


def _run_grouped_tasks(tasks, worker_func, max_workers, group_key_func, db_session=None, group_runner=None):
    grouped_tasks = {}
    for task in tasks:
        grouped_tasks.setdefault(group_key_func(task), []).append(task)

    if not grouped_tasks:
        return []

    def run_group(group_key, group_tasks):
        if group_runner is not None:
            return group_runner(group_key, group_tasks, worker_func, db_session)
        return _run_tasks(group_tasks, worker_func, 1, db_session=db_session)

    if len(grouped_tasks) <= 1:
        group_key, group_tasks = next(iter(grouped_tasks.items()))
        return run_group(group_key, group_tasks)

    worker_count = max(1, min(int(max_workers or 1), len(grouped_tasks)))
    if db_session is not None or worker_count <= 1:
        results = []
        for group_key, group_tasks in grouped_tasks.items():
            results.extend(run_group(group_key, group_tasks))
        return results

    results = []
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_group = {
            executor.submit(run_group, group_key, group_tasks): group_key
            for group_key, group_tasks in grouped_tasks.items()
        }
        for future in as_completed(future_to_group):
            results.extend(future.result())
    return results


def _repair_rolling_series(adapter, symbol, series_type, period, target_times, now_ms, http_session=None, db_session=None):
    breakdown = empty_duration_breakdown()
    time_field = _get_time_field(series_type)
    expected_records = len(set(target_times or []))
    api_records = 0
    repaired_records = 0
    pages = 0
    window_precise = adapter.supports_time_window(series_type)
    latest_event_time = None
    pending_records = []

    groups = _group_contiguous_times(target_times, period=period) if window_precise else [sorted(set(target_times))]
    for group in groups:
        start_time = group[0] if window_precise else None
        end_time = group[-1] if window_precise else None
        limit = max(len(group), 2)
        expected_times = set(group)
        fetch_attempts = [{'start_time': start_time, 'end_time': end_time}]
        if window_precise:
            fetch_attempts.append({'start_time': None, 'end_time': None})

        filtered_records = []
        for attempt in fetch_attempts:
            with timed_category(breakdown, 'api_ms'):
                payload = adapter.fetch_series_payload(
                    series_type=series_type,
                    symbol=symbol,
                    period=period,
                    limit=limit,
                    session=http_session,
                    start_time=attempt['start_time'],
                    end_time=attempt['end_time'],
                )
            with timed_category(breakdown, 'parse_ms'):
                records = adapter.parse_series_payload(series_type, payload, symbol, period)
                api_records += len(records)
                records = _trim_unclosed_records(series_type, records, now_ms, period=period)
                for record in records:
                    record_time = record.get(time_field)
                    if record_time is not None:
                        latest_event_time = max(latest_event_time or record_time, record_time)
                filtered_records = [record for record in records if record.get(time_field) in expected_times]
            if filtered_records:
                break

        if not filtered_records:
            continue

        pending_records.extend(filtered_records)
        repaired_records += len(filtered_records)
        pages += 1

    if repaired_records == 0:
        return _result_with_breakdown(
            {
                'exchange': adapter.exchange_id,
                'symbol': symbol,
                'series_type': series_type,
                'period': period,
                'status': 'skipped',
                'mode': 'rolling',
                'reason': 'no_data',
                'window_precise': window_precise,
                'target_times': sorted(target_times),
                'affected': 0,
                'records': 0,
                'expected_records': expected_records,
                'api_records': api_records,
                'written_records': 0,
                'pages': 0,
                'latest_event_time': latest_event_time,
            },
            breakdown,
        )

    return _result_with_breakdown(
        {
            'exchange': adapter.exchange_id,
            'symbol': symbol,
            'series_type': series_type,
            'period': period,
            'status': 'success',
            'mode': 'rolling',
            'window_precise': window_precise,
            'target_times': sorted(target_times),
            'affected': 0,
            'records': repaired_records,
            'expected_records': expected_records,
            'api_records': api_records,
            'written_records': 0,
            'pages': pages,
            'latest_event_time': latest_event_time,
            'pending_records': pending_records,
        },
        breakdown,
    )


def repair_rolling_symbols(symbols=None, series_types=None, exchanges=None, now_ms=None, points=None, max_workers=None, http_session=None, db_session=None):
    target_symbols = symbols if symbols is not None else get_active_coins()
    target_exchanges = exchanges or ENABLED_EXCHANGES
    adapters = get_exchange_adapters(target_exchanges)
    current_time_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    worker_count = max_workers if max_workers is not None else REPAIR_ROLLING_MAX_WORKERS
    started_at = time.perf_counter()
    precheck_started_at = time.perf_counter()
    precheck_breakdown = empty_duration_breakdown()
    results = []
    tasks = []
    precheck_skipped_count = 0
    all_target_times = set()
    exchange_progress = {}

    logger.info(
        '开始修补: 模式=rolling 交易所=%s 币种数=%d 序列类型=%s 点数=%s 并发=%s',
        ','.join(target_exchanges),
        len(target_symbols),
        ','.join(series_types or HOMEPAGE_SERIES_TYPES),
        points or REPAIR_ROLLING_POINTS,
        worker_count,
    )

    for adapter in adapters:
        exchange_stats = exchange_progress.setdefault(
            adapter.exchange_id,
            {'supported_symbols': 0, 'unsupported': 0, 'complete': 0, 'pending': 0},
        )
        for series_type in _active_series_types(adapter, series_types):
            periods = adapter.periods_for_series(series_type) if hasattr(adapter, 'periods_for_series') else (HOMEPAGE_SERIES_REPAIR_PERIOD,)
            supported_symbols = []
            for symbol in target_symbols:
                try:
                    with timed_category(precheck_breakdown, 'api_ms'):
                        is_supported = adapter.supports_symbol(symbol, series_type=series_type, session=http_session)
                except Exception as exc:
                    exchange_stats['unsupported'] += 1
                    results.append(
                        _supported_symbol_lookup_failed_result(
                            adapter,
                            symbol,
                            series_type,
                            mode='rolling',
                            window_precise=adapter.supports_time_window(series_type),
                            error=exc,
                            extra={'target_times': []},
                        )
                    )
                    continue
                if is_supported:
                    supported_symbols.append(symbol)
                else:
                    exchange_stats['unsupported'] += 1
                    results.append(
                        _unsupported_symbol_result(
                            adapter,
                            symbol,
                            series_type,
                            mode='rolling',
                            window_precise=adapter.supports_time_window(series_type),
                            extra={'target_times': []},
                        )
                    )
            if not supported_symbols:
                continue
            exchange_stats['supported_symbols'] += len(supported_symbols)
            for period in periods:
                target_times = _build_rolling_target_times(current_time_ms, points or REPAIR_ROLLING_POINTS, period=period)
                all_target_times.update(target_times)
                with timed_category(precheck_breakdown, 'db_read_ms'):
                    existing_by_symbol = get_existing_series_timestamps(
                        exchange=adapter.exchange_id,
                        series_type=series_type,
                        symbols=supported_symbols,
                        timestamps=target_times,
                        period=period,
                        session=db_session,
                    )
                for symbol in supported_symbols:
                    missing_times = [timestamp for timestamp in target_times if timestamp not in existing_by_symbol.get(symbol, set())]
                    if not missing_times:
                        precheck_skipped_count += 1
                        exchange_stats['complete'] += 1
                        results.append(
                            {
                                'exchange': adapter.exchange_id,
                                'symbol': symbol,
                                'series_type': series_type,
                                'period': period,
                                'status': 'skipped',
                                'mode': 'rolling',
                                'reason': 'rolling window already complete',
                                'window_precise': adapter.supports_time_window(series_type),
                                'affected': 0,
                                'records': 0,
                                'pages': 0,
                                'target_times': sorted(target_times),
                            }
                        )
                    else:
                        exchange_stats['pending'] += 1
                        tasks.append(
                            {
                                'adapter': adapter,
                                'exchange': adapter.exchange_id,
                                'symbol': symbol,
                                'series_type': series_type,
                                'period': period,
                                'target_times': missing_times,
                            }
                        )

    precheck_duration_ms = (time.perf_counter() - precheck_started_at) * 1000
    precheck_breakdown['precheck_ms'] += precheck_duration_ms
    unsupported_count = sum(stats['unsupported'] for stats in exchange_progress.values())
    logger.info(
        '预检完成: 模式=rolling 支持进度=%s 已完整=%s 待修补=%s 不支持=%s 耗时=%s',
        _format_exchange_progress(exchange_progress) if exchange_progress else '无',
        precheck_skipped_count,
        len(tasks),
        unsupported_count,
        format_duration_ms(precheck_duration_ms),
    )

    def worker(task, db_session=None):
        own_session = db_session is None
        session = db_session or get_session()
        try:
            return _repair_rolling_series(
                adapter=task['adapter'],
                symbol=task['symbol'],
                series_type=task['series_type'],
                period=task['period'],
                target_times=task['target_times'],
                now_ms=current_time_ms,
                http_session=http_session if db_session is not None else None,
                db_session=session,
            )
        except _RATE_LIMIT_EXCEPTIONS as exc:
            cooldown_skip_ms = max(0.0, float(getattr(exc, 'wait_seconds', 0.0) or 0.0) * 1000)
            logger.warning(
                '修补跳过: 模式=rolling 交易所=%s 币种=%s 序列类型=%s 原因=%s',
                task['exchange'],
                task['symbol'],
                task['series_type'],
                _reason_label(_budget_unavailable_reason(task['exchange'])),
            )
            return _result_with_breakdown(
                {
                    'exchange': task['exchange'],
                    'symbol': task['symbol'],
                    'series_type': task['series_type'],
                    'period': task['period'],
                    'status': 'skipped',
                    'mode': 'rolling',
                    'reason': _budget_unavailable_reason(task['exchange']),
                    'affected': 0,
                    'records': 0,
                    'expected_records': len(task.get('target_times') or []),
                    'api_records': 0,
                    'written_records': 0,
                    'pages': 0,
                    'cooldown_skip_ms': cooldown_skip_ms,
                    'error': str(exc),
                },
                {'cooldown_skip_ms': cooldown_skip_ms},
            )
        except GateUnsupportedContract as exc:
            logger.warning(
                '修补跳过: 模式=rolling 交易所=%s 币种=%s 序列类型=%s 原因=Gate 合约不存在，按不支持币种跳过',
                task['exchange'],
                task['symbol'],
                task['series_type'],
            )
            return _unsupported_symbol_result(
                task['adapter'],
                task['symbol'],
                task['series_type'],
                mode='rolling',
                window_precise=task['adapter'].supports_time_window(task['series_type']),
                extra={
                    'period': task['period'],
                    'target_times': sorted(task.get('target_times') or []),
                    'error': str(exc),
                },
            )
        except Exception as exc:
            logger.error(
                '修补失败: 模式=rolling 交易所=%s 币种=%s 序列类型=%s 周期=%s 错误=%s',
                task['exchange'],
                task['symbol'],
                task['series_type'],
                task['period'],
                exc,
            )
            return _result_with_breakdown(
                {
                    'exchange': task['exchange'],
                    'symbol': task['symbol'],
                    'series_type': task['series_type'],
                    'period': task['period'],
                    'status': 'error',
                    'mode': 'rolling',
                    'error': str(exc),
                },
                {},
            )
        finally:
            if own_session:
                session.close()

    def run_exchange_group(exchange, group_tasks, group_worker, db_session):
        started = time.perf_counter()
        series_counts = {}
        symbol_counts = set()
        for task in group_tasks:
            series_counts[task['series_type']] = series_counts.get(task['series_type'], 0) + 1
            symbol_counts.add(task['symbol'])
        logger.info(
            '交易所执行开始: 模式=rolling 交易所=%s 币种数=%d 任务数=%d 序列摘要=%s',
            exchange,
            len(symbol_counts),
            len(group_tasks),
            _format_series_counts(series_counts),
        )
        runnable_tasks, skipped_results = _filter_budget_unavailable_tasks(group_tasks, mode='rolling')
        group_results = skipped_results + _run_tasks(
            runnable_tasks,
            group_worker,
            1,
            db_session=db_session,
            mode='rolling',
            exchange=exchange,
        )
        group_results = _flush_group_records(exchange, group_results, db_session=db_session, mode='rolling')
        result_stats = _summarize_results(group_results)
        logger.info(
            '交易所执行完成: 模式=rolling 交易所=%s 成功=%s 失败=%s 跳过=%s 跳过原因=%s 耗时=%s 累计耗时分类=%s',
            exchange,
            result_stats['success_count'],
            result_stats['failure_count'],
            result_stats['skipped_count'],
            _format_reason_counts(group_results),
            format_duration_ms((time.perf_counter() - started) * 1000),
            format_duration_breakdown(
                sum_duration_breakdowns(item.get('duration_breakdown_ms') for item in group_results)
            ),
        )
        return group_results

    results.extend(
        _run_grouped_tasks(
            tasks,
            worker,
            worker_count,
            group_key_func=lambda task: task['exchange'],
            db_session=db_session,
            group_runner=run_exchange_group,
        )
    )
    results_breakdown = sum_duration_breakdowns(item.get('duration_breakdown_ms') for item in results)
    total_breakdown = empty_duration_breakdown()
    add_duration_breakdown(total_breakdown, precheck_breakdown)
    add_duration_breakdown(total_breakdown, results_breakdown)
    summary = _build_summary(
        mode='rolling',
        symbols=target_symbols,
        series_types=series_types or HOMEPAGE_SERIES_TYPES,
        exchanges=[adapter.exchange_id for adapter in adapters],
        results=results,
        started_at=started_at,
        extra={
            'target_times': sorted(all_target_times),
            'precheck_skipped_count': precheck_skipped_count,
            'precheck_duration_ms': precheck_duration_ms,
            'pending_task_count': len(tasks),
            'unsupported_count': unsupported_count,
            'exchange_progress': exchange_progress,
            'duration_breakdown_ms': total_breakdown,
            'duration_breakdown_by_exchange': _build_grouped_duration_breakdowns(results, 'exchange'),
            'duration_breakdown_by_series_type': _build_grouped_duration_breakdowns(results, 'series_type'),
        },
    )
    _log_repair_summary(summary)
    return summary


def _history_target_symbols(symbols, full_scan):
    global _history_symbol_cursor
    target_symbols = symbols if symbols is not None else get_active_coins()
    if full_scan or not target_symbols or REPAIR_HISTORY_SYMBOL_BATCH_SIZE <= 0:
        return target_symbols

    if _history_symbol_cursor >= len(target_symbols):
        _history_symbol_cursor = 0
    start_index = _history_symbol_cursor
    end_index = start_index + REPAIR_HISTORY_SYMBOL_BATCH_SIZE
    if end_index <= len(target_symbols):
        batch = target_symbols[start_index:end_index]
    else:
        batch = target_symbols[start_index:] + target_symbols[:end_index % len(target_symbols)]
    _history_symbol_cursor = end_index % len(target_symbols)
    return batch


def repair_history_symbols(symbols=None, series_types=None, exchanges=None, now_ms=None, full_scan=False, max_workers=None, coverage_hours=None, http_session=None, db_session=None):
    target_symbols = _history_target_symbols(symbols, full_scan)
    target_exchanges = exchanges or ENABLED_EXCHANGES
    adapters = get_exchange_adapters(target_exchanges)
    current_time_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    effective_coverage_hours = coverage_hours or REPAIR_HISTORY_COVERAGE_HOURS
    worker_count = max_workers if max_workers is not None else REPAIR_HISTORY_MAX_WORKERS
    started_at = time.perf_counter()
    precheck_started_at = time.perf_counter()
    precheck_breakdown = empty_duration_breakdown()
    tasks = []
    skipped_results = []
    exchange_task_counts = {}
    precheck_skipped_count = 0
    current_day_trimmed_end_time = None
    exchange_progress = {}
    summary_bounds = _build_history_window_bounds(current_time_ms, HOMEPAGE_SERIES_REPAIR_PERIOD, effective_coverage_hours)
    summary_start_time = summary_bounds[0] if summary_bounds else 0
    summary_end_time = summary_bounds[1] if summary_bounds else latest_closed_5m_open_time(current_time_ms)
    history_missing_day_stats = {}

    for adapter in adapters:
        exchange_stats = exchange_progress.setdefault(
            adapter.exchange_id,
            {'supported_symbols': 0, 'unsupported': 0, 'complete': 0, 'pending': 0},
        )
        for series_type in _active_series_types(adapter, series_types):
            periods = adapter.periods_for_series(series_type) if hasattr(adapter, 'periods_for_series') else (HOMEPAGE_SERIES_REPAIR_PERIOD,)
            for period in periods:
                day_segments = _build_history_day_segments(current_time_ms, period, effective_coverage_hours)
                if not day_segments:
                    continue
                target_start_time = day_segments[0][0]
                target_end_time = day_segments[-1][1]
                current_day_trimmed_end_time = (
                    target_end_time
                    if current_day_trimmed_end_time is None
                    else max(current_day_trimmed_end_time, target_end_time)
                )
                for symbol in target_symbols:
                    try:
                        with timed_category(precheck_breakdown, 'api_ms'):
                            is_supported = adapter.supports_symbol(symbol, series_type=series_type, session=http_session)
                    except Exception as exc:
                        exchange_stats['unsupported'] += 1
                        skipped_results.append(
                            _supported_symbol_lookup_failed_result(
                                adapter,
                                symbol,
                                series_type,
                                mode='history',
                                window_precise=adapter.supports_time_window(series_type),
                                error=exc,
                                extra={
                                    'period': period,
                                    'start_time': target_start_time,
                                    'end_time': target_end_time,
                                },
                            )
                        )
                        continue
                    if not is_supported:
                        exchange_stats['unsupported'] += 1
                        skipped_results.append(
                            _unsupported_symbol_result(
                                adapter,
                                symbol,
                                series_type,
                                mode='history',
                                window_precise=adapter.supports_time_window(series_type),
                                extra={
                                    'period': period,
                                    'start_time': target_start_time,
                                    'end_time': target_end_time,
                                },
                            )
                        )
                        continue
                    exchange_stats['supported_symbols'] += 1
                    window_precise = adapter.supports_time_window(series_type)
                    if not window_precise:
                        exchange_stats['pending'] += 1
                        tasks.append(
                            {
                                'adapter': adapter,
                                'exchange': adapter.exchange_id,
                                'symbol': symbol,
                                'series_type': series_type,
                                'period': period,
                                'start_time': target_start_time,
                                'end_time': target_end_time,
                            }
                        )
                        exchange_task_counts[adapter.exchange_id] = exchange_task_counts.get(adapter.exchange_id, 0) + 1
                        continue

                    with timed_category(precheck_breakdown, 'db_read_ms'):
                        gap_tasks = _build_history_gap_tasks(
                            adapter.exchange_id,
                            symbol,
                            series_type,
                            period,
                            day_segments,
                            session=db_session,
                        )
                    if not gap_tasks:
                        precheck_skipped_count += 1
                        exchange_stats['complete'] += 1
                    for gap_task in gap_tasks:
                        tasks.append(
                            {
                                'adapter': adapter,
                                **gap_task,
                            }
                        )
                    _record_history_missing_day_stats(
                        history_missing_day_stats,
                        adapter.exchange_id,
                        symbol,
                        series_type,
                        len(gap_tasks),
                    )
                    exchange_stats['pending'] += len(gap_tasks)
                    exchange_task_counts[adapter.exchange_id] = exchange_task_counts.get(adapter.exchange_id, 0) + len(gap_tasks)

    precheck_duration_ms = (time.perf_counter() - precheck_started_at) * 1000
    precheck_breakdown['precheck_ms'] += precheck_duration_ms
    unsupported_count = sum(stats['unsupported'] for stats in exchange_progress.values())
    logger.info(
        '预检完成: 模式=history 支持进度=%s 已完整=%s 待修补=%s 不支持=%s 当天裁剪截止=%s 缺口分布=%s 耗时=%s',
        _format_exchange_progress(exchange_progress) if exchange_progress else '无',
        precheck_skipped_count,
        len(tasks),
        unsupported_count,
        current_day_trimmed_end_time,
        _format_history_missing_day_stats(history_missing_day_stats),
        format_duration_ms(precheck_duration_ms),
    )

    def worker(task, db_session=None):
        breakdown = empty_duration_breakdown()
        own_session = db_session is None
        session = db_session or get_session()
        window_precise = task['adapter'].supports_time_window(task['series_type'])
        try:
            time_field = _get_time_field(task['series_type'])
            limit = _page_limit(task['series_type'], adapter=task['adapter'])
            period = task['period']
            target_start_time = task['start_time']
            target_end_time = task['end_time']
            expected_records = len(_build_target_times_in_range(target_start_time, target_end_time, period=period))
            cursor_time = target_start_time
            backward_cursor_end_time = target_end_time
            pending_records = []
            affected = 0
            api_records = 0
            record_count = 0
            pages = 0

            while cursor_time <= target_end_time:
                use_open_ended_history_paging = _uses_open_ended_history_paging(task['adapter'], task['series_type'])
                use_backward_window_history_paging = (
                    window_precise and _uses_backward_window_history_paging(task['adapter'], task['series_type'])
                )
                page_start_time = (
                    max(target_start_time, backward_cursor_end_time - max(limit - 1, 0) * _period_to_ms(period))
                    if use_backward_window_history_paging
                    else (cursor_time if window_precise else None)
                )
                page_end_time = (
                    None
                    if use_open_ended_history_paging
                    else (
                        backward_cursor_end_time
                        if use_backward_window_history_paging
                        else (_page_end_time(cursor_time, target_end_time, limit, period=period) if window_precise else None)
                    )
                )
                with timed_category(breakdown, 'api_ms'):
                    payload = task['adapter'].fetch_series_payload(
                        series_type=task['series_type'],
                        symbol=task['symbol'],
                        period=period,
                        limit=limit,
                        session=http_session if db_session is not None else None,
                        start_time=page_start_time,
                        end_time=page_end_time if window_precise else None,
                    )
                with timed_category(breakdown, 'parse_ms'):
                    records = task['adapter'].parse_series_payload(task['series_type'], payload, task['symbol'], period)
                    api_records += len(records)
                    records = _trim_unclosed_records(task['series_type'], records, current_time_ms, period=period)
                    current_end_time = (
                        target_end_time
                        if use_open_ended_history_paging
                        else (page_end_time if window_precise else target_end_time)
                    )
                    filtered_records = [
                        record
                        for record in records
                        if target_start_time <= record.get(time_field, -1) <= current_end_time
                    ]
                pending_records.extend(filtered_records)
                record_count += len(filtered_records)
                if filtered_records:
                    pages += 1

                if not window_precise:
                    break
                if use_open_ended_history_paging:
                    if not filtered_records:
                        break
                    last_time = filtered_records[-1].get(time_field)
                    if last_time is None or last_time < cursor_time:
                        break
                    cursor_time = last_time + _period_to_ms(period)
                    if len(filtered_records) < limit:
                        break
                    continue
                if use_backward_window_history_paging:
                    if not filtered_records:
                        break
                    earliest_time = filtered_records[-1].get(time_field)
                    if earliest_time is None or earliest_time <= target_start_time:
                        break
                    backward_cursor_end_time = earliest_time - _period_to_ms(period)
                    cursor_time = max(target_start_time, backward_cursor_end_time - max(limit - 1, 0) * _period_to_ms(period))
                    if backward_cursor_end_time < target_start_time:
                        break
                    continue
                cursor_time = page_end_time + _period_to_ms(period)

            if record_count == 0:
                return _result_with_breakdown(
                    {
                        'exchange': task['exchange'],
                        'symbol': task['symbol'],
                        'series_type': task['series_type'],
                        'period': period,
                        'status': 'skipped',
                        'mode': 'history',
                        'reason': 'no_data',
                        'window_precise': window_precise,
                        'start_time': target_start_time,
                        'end_time': target_end_time,
                        'affected': 0,
                        'records': 0,
                        'expected_records': expected_records,
                        'no_data_records': expected_records,
                        'api_records': api_records,
                        'written_records': 0,
                        'pages': pages,
                    },
                    breakdown,
                )

            return _result_with_breakdown(
                {
                    'exchange': task['exchange'],
                    'symbol': task['symbol'],
                    'series_type': task['series_type'],
                    'period': period,
                    'status': 'success',
                    'mode': 'history',
                    'window_precise': window_precise,
                    'start_time': target_start_time,
                    'end_time': target_end_time,
                    'affected': affected,
                    'records': record_count,
                    'expected_records': expected_records,
                    'api_records': api_records,
                    'written_records': 0,
                    'pages': pages,
                    'pending_records': pending_records,
                },
                breakdown,
            )
        except GateUnsupportedContract as exc:
            logger.warning(
                '修补跳过: 模式=history 交易所=%s 币种=%s 序列类型=%s 原因=Gate 合约不存在，按不支持币种跳过',
                task['exchange'],
                task['symbol'],
                task['series_type'],
            )
            return _unsupported_symbol_result(
                task['adapter'],
                task['symbol'],
                task['series_type'],
                mode='history',
                window_precise=window_precise,
                extra={
                    'period': task['period'],
                    'start_time': task['start_time'],
                    'end_time': task['end_time'],
                    'error': str(exc),
                },
            )
        except _RATE_LIMIT_EXCEPTIONS as exc:
            cooldown_skip_ms = max(0.0, float(getattr(exc, 'wait_seconds', 0.0) or 0.0) * 1000)
            logger.warning(
                '修补跳过: 模式=history 交易所=%s 币种=%s 序列类型=%s 原因=%s',
                task['exchange'],
                task['symbol'],
                task['series_type'],
                _reason_label(_budget_unavailable_reason(task['exchange'])),
            )
            return _result_with_breakdown(
                {
                    'exchange': task['exchange'],
                    'symbol': task['symbol'],
                    'series_type': task['series_type'],
                    'period': task['period'],
                    'status': 'skipped',
                    'mode': 'history',
                    'window_precise': window_precise,
                    'reason': _budget_unavailable_reason(task['exchange']),
                    'affected': 0,
                    'records': 0,
                    'expected_records': len(_build_target_times_in_range(task.get('start_time'), task.get('end_time'), period=task.get('period'))),
                    'api_records': 0,
                    'written_records': 0,
                    'pages': 0,
                    'cooldown_skip_ms': cooldown_skip_ms,
                    'error': str(exc),
                },
                {'cooldown_skip_ms': cooldown_skip_ms},
            )
        except Exception as exc:
            logger.error(
                '修补失败: 模式=history 交易所=%s 币种=%s 序列类型=%s 周期=%s 错误=%s',
                task['exchange'],
                task['symbol'],
                task['series_type'],
                task['period'],
                exc,
            )
            return _result_with_breakdown(
                {
                    'exchange': task['exchange'],
                    'symbol': task['symbol'],
                    'series_type': task['series_type'],
                    'period': task['period'],
                    'status': 'error',
                    'mode': 'history',
                    'window_precise': window_precise,
                    'error': str(exc),
                },
                breakdown,
            )
        finally:
            if own_session:
                session.close()

    def run_exchange_group(exchange, group_tasks, group_worker, db_session):
        started = time.perf_counter()
        logger.info(
            '交易所执行开始: 模式=history 交易所=%s 币种数=%d 任务数=%d',
            exchange,
            len({task['symbol'] for task in group_tasks}),
            len(group_tasks),
        )
        runnable_tasks, skipped_results = _filter_budget_unavailable_tasks(group_tasks, mode='history')
        group_results = skipped_results + _run_tasks(
            runnable_tasks,
            group_worker,
            1,
            db_session=db_session,
            mode='history',
            exchange=exchange,
        )
        group_results = _flush_group_records(exchange, group_results, db_session=db_session, mode='history')
        result_stats = _summarize_results(group_results)
        logger.info(
            '交易所执行完成: 模式=history 交易所=%s 成功=%s 失败=%s 跳过=%s 跳过原因=%s 耗时=%s 累计耗时分类=%s',
            exchange,
            result_stats['success_count'],
            result_stats['failure_count'],
            result_stats['skipped_count'],
            _format_reason_counts(group_results),
            format_duration_ms((time.perf_counter() - started) * 1000),
            format_duration_breakdown(
                sum_duration_breakdowns(item.get('duration_breakdown_ms') for item in group_results)
            ),
        )
        return group_results

    if tasks:
        logger.info(
            '开始修补: 模式=history 交易所=%s 待修补任务=%d 覆盖时长=%s 并发=%s 全量扫描=%s 任务摘要=%s',
            ','.join(sorted(exchange_task_counts)),
            len(tasks),
            effective_coverage_hours,
            worker_count,
            '是' if full_scan else '否',
            '; '.join(f'{exchange}(待补={count})' for exchange, count in sorted(exchange_task_counts.items())),
        )

    results = skipped_results + _run_grouped_tasks(
        tasks,
        worker,
        worker_count,
        group_key_func=lambda task: task['exchange'],
        db_session=db_session,
        group_runner=run_exchange_group,
    )
    results_breakdown = sum_duration_breakdowns(item.get('duration_breakdown_ms') for item in results)
    total_breakdown = empty_duration_breakdown()
    add_duration_breakdown(total_breakdown, precheck_breakdown)
    add_duration_breakdown(total_breakdown, results_breakdown)
    summary = _build_summary(
        mode='history',
        symbols=target_symbols,
        series_types=series_types or HOMEPAGE_SERIES_TYPES,
        exchanges=[adapter.exchange_id for adapter in adapters],
        results=results,
        started_at=started_at,
        extra={
            'precheck_skipped_count': precheck_skipped_count,
            'precheck_duration_ms': precheck_duration_ms,
            'pending_task_count': len(tasks),
            'unsupported_count': unsupported_count,
            'history_missing_day_stats': _format_history_missing_day_stats(history_missing_day_stats),
            'coverage_hours': effective_coverage_hours,
            'start_time': summary_start_time,
            'end_time': summary_end_time,
            'full_scan': full_scan,
            'current_day_trimmed_end_time': current_day_trimmed_end_time,
            'duration_breakdown_ms': total_breakdown,
            'duration_breakdown_by_exchange': _build_grouped_duration_breakdowns(results, 'exchange'),
            'duration_breakdown_by_series_type': _build_grouped_duration_breakdowns(results, 'series_type'),
        },
    )
    _log_repair_summary(summary)
    return summary


def _build_summary(mode, symbols, series_types, exchanges, results, started_at, extra=None):
    success_count = sum(1 for item in results if item.get('status') == 'success')
    failure_count = sum(1 for item in results if item.get('status') == 'error')
    skipped_count = sum(1 for item in results if item.get('status') == 'skipped')
    exchange_summaries = {}
    for exchange in exchanges:
        exchange_results = [item for item in results if item.get('exchange') == exchange]
        exchange_summaries[exchange] = {
            'exchange': exchange,
            'success_count': sum(1 for item in exchange_results if item.get('status') == 'success'),
            'failure_count': sum(1 for item in exchange_results if item.get('status') == 'error'),
            'skipped_count': sum(1 for item in exchange_results if item.get('status') == 'skipped'),
            'results': exchange_results,
        }

    duration_ms = (time.perf_counter() - started_at) * 1000
    summary = {
        'status': 'success' if failure_count == 0 else 'partial_success',
        'mode': mode,
        'symbols': symbols,
        'series_types': list(series_types),
        'exchanges': exchanges,
        'period': HOMEPAGE_SERIES_REPAIR_PERIOD,
        'success_count': success_count,
        'failure_count': failure_count,
        'skipped_count': skipped_count,
        'duration_ms': duration_ms,
        'exchange_summaries': exchange_summaries,
        'results': results,
        'expected_records': sum(item.get('expected_records') or 0 for item in results),
        'api_records': sum(item.get('api_records') or 0 for item in results),
        'records': sum(item.get('records') or 0 for item in results),
        'missing_records': sum(
            max((item.get('expected_records') or 0) - (item.get('records') or 0), 0)
            for item in results
        ),
        'written_records': sum(item.get('written_records') or 0 for item in results),
        'no_data_records': sum(item.get('no_data_records') or 0 for item in results),
        'affected': sum(item.get('affected') or 0 for item in results),
    }
    if extra:
        summary.update(extra)
    summary['duration_breakdown_ms'] = attach_other_duration(
        summary.get('duration_breakdown_ms') or sum_duration_breakdowns(
            item.get('duration_breakdown_ms') for item in results
        ),
        duration_ms,
    )
    summary.setdefault('duration_breakdown_by_exchange', _build_grouped_duration_breakdowns(results, 'exchange'))
    summary.setdefault('duration_breakdown_by_series_type', _build_grouped_duration_breakdowns(results, 'series_type'))
    return summary
