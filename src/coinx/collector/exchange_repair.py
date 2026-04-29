import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from coinx.coin_manager import get_active_coins
from coinx.collector.exchange_adapters import get_exchange_adapters
from coinx.config import (
    ENABLED_EXCHANGES,
    HOMEPAGE_SERIES_REPAIR_PERIOD,
    HOMEPAGE_SERIES_TYPES,
    REPAIR_HISTORY_COVERAGE_HOURS,
    REPAIR_HISTORY_MAX_WORKERS,
    REPAIR_HISTORY_SYMBOL_BATCH_SIZE,
    REPAIR_ROLLING_MAX_WORKERS,
    REPAIR_ROLLING_POINTS,
)
from coinx.database import get_session
from coinx.repositories.series import get_existing_series_timestamps, upsert_series_records
from coinx.utils import logger


FIVE_MINUTES_MS = 5 * 60 * 1000
_history_symbol_cursor = 0


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


def _active_series_types(adapter, series_types):
    requested_types = tuple(series_types or HOMEPAGE_SERIES_TYPES)
    return [series_type for series_type in requested_types if series_type in adapter.supported_series_types]


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
        'pages': 0,
    }
    if extra:
        result.update(extra)
    return result


def _sample_values(values, limit=8):
    values = sorted(set(values))
    if len(values) <= limit:
        return ','.join(values)
    return f"{','.join(values[:limit])}...(+{len(values) - limit})"


def _log_repair_summary(summary):
    logger.info(
        f"交易所{summary['mode']}补齐完成: exchanges={','.join(summary['exchanges'])}, "
        f"symbols={len(summary['symbols'])}, series_types={','.join(summary['series_types'])}, "
        f"成功={summary['success_count']}, 失败={summary['failure_count']}, "
        f"跳过={summary['skipped_count']}, 耗时={summary['duration_ms']:.2f}ms"
    )

    precheck_skipped_count = summary.get('precheck_skipped_count')
    if precheck_skipped_count:
        logger.info(f"交易所滚动补齐预检查跳过: 已完整={precheck_skipped_count}")

    unsupported_groups = {}
    for item in summary.get('results', []):
        if item.get('status') == 'skipped' and item.get('reason') == 'unsupported_symbol':
            key = (item.get('exchange'), item.get('series_type'))
            unsupported_groups.setdefault(key, []).append(item.get('symbol'))

    for (exchange, series_type), symbols in sorted(unsupported_groups.items()):
        logger.info(
            f"交易所补齐跳过不支持币种: exchange={exchange}, type={series_type}, "
            f"count={len(symbols)}, symbols={_sample_values(symbols)}"
        )


def _run_tasks(tasks, worker_func, max_workers, db_session=None):
    if not tasks:
        return []
    worker_count = max(1, int(max_workers or 1))
    if db_session is not None or worker_count <= 1:
        return [worker_func(task, db_session=db_session) for task in tasks]

    results = []
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_task = {executor.submit(worker_func, task, db_session=None): task for task in tasks}
        for future in as_completed(future_to_task):
            results.append(future.result())
    return results


def _repair_rolling_series(adapter, symbol, series_type, period, target_times, now_ms, http_session=None, db_session=None):
    time_field = _get_time_field(series_type)
    affected = 0
    repaired_records = 0
    pages = 0
    window_precise = adapter.supports_time_window(series_type)

    groups = _group_contiguous_times(target_times, period=period) if window_precise else [sorted(set(target_times))]
    for group in groups:
        start_time = group[0] if window_precise else None
        end_time = group[-1] if window_precise else None
        limit = max(len(group), 2)
        payload = adapter.fetch_series_payload(
            series_type=series_type,
            symbol=symbol,
            period=period,
            limit=limit,
            session=http_session,
            start_time=start_time,
            end_time=end_time,
        )
        records = adapter.parse_series_payload(series_type, payload, symbol, period)
        records = _trim_unclosed_records(series_type, records, now_ms, period=period)
        expected_times = set(group)
        filtered_records = [record for record in records if record.get(time_field) in expected_times]
        if not filtered_records:
            continue

        affected += upsert_series_records(adapter.exchange_id, series_type, filtered_records, session=db_session)
        repaired_records += len(filtered_records)
        pages += 1

    return {
        'exchange': adapter.exchange_id,
        'symbol': symbol,
        'series_type': series_type,
        'period': period,
        'status': 'success',
        'mode': 'rolling',
        'window_precise': window_precise,
        'target_times': sorted(target_times),
        'affected': affected,
        'records': repaired_records,
        'pages': pages,
    }


def repair_rolling_symbols(
    symbols=None,
    series_types=None,
    exchanges=None,
    now_ms=None,
    points=None,
    max_workers=None,
    http_session=None,
    db_session=None,
):
    target_symbols = symbols if symbols is not None else get_active_coins()
    target_exchanges = exchanges or ENABLED_EXCHANGES
    adapters = get_exchange_adapters(target_exchanges)
    current_time_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    worker_count = max_workers if max_workers is not None else REPAIR_ROLLING_MAX_WORKERS
    started_at = time.perf_counter()
    results = []
    tasks = []
    precheck_skipped_count = 0
    all_target_times = set()

    for adapter in adapters:
        for series_type in _active_series_types(adapter, series_types):
            periods = adapter.periods_for_series(series_type) if hasattr(adapter, 'periods_for_series') else (HOMEPAGE_SERIES_REPAIR_PERIOD,)
            supported_symbols = []
            for symbol in target_symbols:
                if adapter.supports_symbol(symbol, series_type=series_type, session=http_session):
                    supported_symbols.append(symbol)
                else:
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
            for period in periods:
                target_times = _build_rolling_target_times(current_time_ms, points or REPAIR_ROLLING_POINTS, period=period)
                all_target_times.update(target_times)
                existing_by_symbol = get_existing_series_timestamps(
                    exchange=adapter.exchange_id,
                    series_type=series_type,
                    symbols=supported_symbols,
                    timestamps=target_times,
                    period=period,
                    session=db_session,
                )
                for symbol in supported_symbols:
                    missing_times = [
                        timestamp
                        for timestamp in target_times
                        if timestamp not in existing_by_symbol.get(symbol, set())
                    ]
                    if not missing_times:
                        precheck_skipped_count += 1
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
        except Exception as exc:
            logger.error(
                f"交易所滚动补齐失败: exchange={task['exchange']}, "
                f"symbol={task['symbol']}, type={task['series_type']}, error={exc}"
            )
            return {
                'exchange': task['exchange'],
                'symbol': task['symbol'],
                'series_type': task['series_type'],
                'period': task['period'],
                'status': 'error',
                'mode': 'rolling',
                'error': str(exc),
            }
        finally:
            if own_session:
                session.close()

    results.extend(_run_tasks(tasks, worker, worker_count, db_session=db_session))
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


def repair_history_symbols(
    symbols=None,
    series_types=None,
    exchanges=None,
    now_ms=None,
    full_scan=False,
    max_workers=None,
    coverage_hours=None,
    http_session=None,
    db_session=None,
):
    target_symbols = _history_target_symbols(symbols, full_scan)
    target_exchanges = exchanges or ENABLED_EXCHANGES
    adapters = get_exchange_adapters(target_exchanges)
    current_time_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    effective_coverage_hours = coverage_hours or REPAIR_HISTORY_COVERAGE_HOURS
    worker_count = max_workers if max_workers is not None else REPAIR_HISTORY_MAX_WORKERS
    started_at = time.perf_counter()
    tasks = []
    skipped_results = []
    summary_end_time = latest_closed_5m_open_time(current_time_ms)
    summary_start_time = max(0, summary_end_time - effective_coverage_hours * 60 * 60 * 1000)

    for adapter in adapters:
        for series_type in _active_series_types(adapter, series_types):
            periods = adapter.periods_for_series(series_type) if hasattr(adapter, 'periods_for_series') else (HOMEPAGE_SERIES_REPAIR_PERIOD,)
            for period in periods:
                target_end_time = latest_closed_period_open_time(current_time_ms, period)
                target_start_time = max(0, target_end_time - effective_coverage_hours * 60 * 60 * 1000)
                for symbol in target_symbols:
                    if not adapter.supports_symbol(symbol, series_type=series_type, session=http_session):
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

    def worker(task, db_session=None):
        own_session = db_session is None
        session = db_session or get_session()
        window_precise = task['adapter'].supports_time_window(task['series_type'])
        try:
            time_field = _get_time_field(task['series_type'])
            limit = _page_limit(task['series_type'], adapter=task['adapter'])
            period = task['period']
            target_start_time = task['start_time']
            target_end_time = task['end_time']
            cursor_time = target_start_time
            affected = 0
            record_count = 0
            pages = 0

            while cursor_time <= target_end_time:
                page_end_time = _page_end_time(cursor_time, target_end_time, limit, period=period) if window_precise else None
                payload = task['adapter'].fetch_series_payload(
                    series_type=task['series_type'],
                    symbol=task['symbol'],
                    period=period,
                    limit=limit,
                    session=http_session if db_session is not None else None,
                    start_time=cursor_time if window_precise else None,
                    end_time=page_end_time if window_precise else None,
                )
                records = task['adapter'].parse_series_payload(
                    task['series_type'],
                    payload,
                    task['symbol'],
                    period,
                )
                records = _trim_unclosed_records(task['series_type'], records, current_time_ms, period=period)
                current_end_time = page_end_time if window_precise else target_end_time
                filtered_records = [
                    record
                    for record in records
                    if target_start_time <= record.get(time_field, -1) <= current_end_time
                ]
                affected += upsert_series_records(
                    task['exchange'],
                    task['series_type'],
                    filtered_records,
                    session=session,
                )
                record_count += len(filtered_records)
                if filtered_records:
                    pages += 1

                if not window_precise:
                    break
                cursor_time = page_end_time + _period_to_ms(period)

            return {
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
                'pages': pages,
            }
        except Exception as exc:
            logger.error(
                f"交易所历史补齐失败: exchange={task['exchange']}, "
                f"symbol={task['symbol']}, type={task['series_type']}, error={exc}"
            )
            return {
                'exchange': task['exchange'],
                'symbol': task['symbol'],
                'series_type': task['series_type'],
                'period': task['period'],
                'status': 'error',
                'mode': 'history',
                'window_precise': window_precise,
                'error': str(exc),
            }
        finally:
            if own_session:
                session.close()

    results = skipped_results + _run_tasks(tasks, worker, worker_count, db_session=db_session)
    summary = _build_summary(
        mode='history',
        symbols=target_symbols,
        series_types=series_types or HOMEPAGE_SERIES_TYPES,
        exchanges=[adapter.exchange_id for adapter in adapters],
        results=results,
        started_at=started_at,
        extra={
            'coverage_hours': effective_coverage_hours,
            'start_time': summary_start_time,
            'end_time': summary_end_time,
            'full_scan': full_scan,
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
        'duration_ms': (time.perf_counter() - started_at) * 1000,
        'exchange_summaries': exchange_summaries,
        'results': results,
    }
    if extra:
        summary.update(extra)
    return summary
