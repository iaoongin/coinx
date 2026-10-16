import threading
import time
from collections import Counter
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from sqlalchemy import select

from .collector import (
    get_all_24hr_tickers,
    refresh_market_tickers,
    repair_rolling_tracked_symbols,
    run_history_repair_job,
)
from .collector.exchange_repair import resolve_repair_worker_count
from .coin_manager import get_active_coins, update_coins_config
from .config import (
    FETCH_COINS_ENABLED,
    FETCH_COINS_INTERVAL,
    FETCH_COINS_TOP_GAINERS_COUNT,
    FETCH_COINS_TOP_LOSERS_COUNT,
    FETCH_COINS_TOP_VOLUME_COUNT,
    FUNDING_RATE_COLLECT_ENABLED,
    ENABLED_EXCHANGES,
    HOMEPAGE_SERIES_REPAIR_ENABLED,
    REPAIR_HISTORY_COVERAGE_HOURS,
    SCHEDULER_ENABLED,
    UPDATE_INTERVAL,
    REPAIR_HISTORY_ENABLED,
    REPAIR_HISTORY_INTERVAL,
    REPAIR_HISTORY_SYMBOL_BATCH_SIZE,
    REPAIR_ROLLING_POINTS,
    REPAIR_TRACKED_INTERVAL,
    RSS_ENABLED,
    RSS_POLL_INTERVAL,
    MARKET_DATA_RETENTION_DAYS,
    SMART_MONEY_ENABLED,
    SMART_MONEY_INTERVAL,
    SMART_MONEY_RETENTION_DAYS,
    SMART_MONEY_TIME_RANGES,
    TASK_RUN_HISTORY_RETENTION_DAYS,
    DATABASE_URI,
)
from .repositories.funding_rate import collect_funding_rates
from .repositories.smart_money import (
    align_snapshot_time,
    collect_and_save_smart_money_signals,
    get_smart_money_scope_symbols,
)
from .repositories.homepage_series import HOMEPAGE_REQUIRED_SERIES_TYPES
from .repositories.job_runs import (
    create_job_run,
    delete_expired_job_runs,
    ensure_job_run_schema,
    finish_job_run,
    recover_interrupted_job_runs,
    update_job_run,
)
from .repositories.retention import (
    cleanup_expired_market_data,
    cleanup_expired_smart_money_data,
)
from .repositories.market_tickers import get_market_scope_symbols_from_tickers
from .repositories.market_structure_score import get_market_structure_score_symbols
from .collector.timing import format_duration_ms
from .utils import logger


scheduler_jobstore = SQLAlchemyJobStore(url=DATABASE_URI)
scheduler = BackgroundScheduler(jobstores={'default': scheduler_jobstore})
JOB_METADATA_LOCK = threading.Lock()
JOB_METADATA = {}
JOB_ERROR_MAX_LENGTH = 500
JOB_ERROR_DETAIL_MAX_LENGTH = 20000
JOB_PROGRESS_EVENT_LIMIT = 40


def scheduled_job(*args, **kwargs):
    """Register task definitions even when scheduling is disabled."""
    return scheduler.scheduled_job(*args, **kwargs)


def initialize_job_run_history():
    """Ensure task history is available and recover runs interrupted by a restart."""
    try:
        ensure_job_run_schema()
        interrupted_count = recover_interrupted_job_runs()
        if interrupted_count:
            logger.warning('已标记中断的任务运行记录: count=%s', interrupted_count)
    except Exception:
        logger.exception('任务运行记录初始化失败，将继续启动服务')


def _get_persisted_paused_job_ids():
    """Read paused jobs before decorator registration can replace their state."""
    scheduler_jobstore.jobs_t.create(bind=scheduler_jobstore.engine, checkfirst=True)
    statement = select(scheduler_jobstore.jobs_t.c.id).where(
        scheduler_jobstore.jobs_t.c.next_run_time.is_(None)
    )
    with scheduler_jobstore.engine.begin() as connection:
        return {row[0] for row in connection.execute(statement)}


def _restore_persisted_paused_jobs(paused_job_ids):
    """Restore paused jobs after pending definitions have been loaded."""
    for job_id in paused_job_ids:
        try:
            job = scheduler.get_job(job_id)
            if job is None or job.next_run_time is None:
                continue
            job.pause()
            logger.info('已恢复任务暂停状态: job_id=%s', job_id)
        except Exception:
            logger.exception('恢复任务暂停状态失败: job_id=%s', job_id)


def _update_job_metadata(job_id, **fields):
    with JOB_METADATA_LOCK:
        metadata = JOB_METADATA.setdefault(job_id, {})
        metadata.update(fields)
        metadata['job_id'] = job_id
        return dict(metadata)


def format_job_error(error, max_length=JOB_ERROR_MAX_LENGTH):
    """Return a bounded, useful error message without embedding SQL payloads."""
    if not error:
        return None
    original = getattr(error, 'orig', None)
    message = f'{type(error).__name__}: {original}' if original else str(error)
    if len(message) <= max_length:
        return message
    return f'{message[:max_length - 3]}...'


def format_job_error_detail(error, max_length=JOB_ERROR_DETAIL_MAX_LENGTH):
    """Keep an on-demand diagnostic detail without letting it grow unbounded."""
    if not error:
        return None
    message = str(error)
    if len(message) <= max_length:
        return message
    return f'{message[:max_length - 3]}...'


def _mark_job_started(job_id):
    started_at_ms = int(time.time() * 1000)
    metadata = _update_job_metadata(
        job_id,
        running=True,
        last_started_at_ms=started_at_ms,
        last_finished_at_ms=None,
        last_duration_ms=None,
        last_status='running',
        last_summary=None,
        last_error=None,
        run_id=None,
    )
    try:
        run_id = create_job_run(job_id, started_at=started_at_ms)
        metadata = _update_job_metadata(job_id, run_id=run_id)
    except Exception:
        logger.exception('任务运行记录创建失败: job_id=%s', job_id)
    return metadata


def _append_job_progress_event(summary, event):
    progress_summary = dict(summary or {})
    events = list(progress_summary.get('progress_events') or [])
    events.append(event)
    progress_summary['progress_events'] = events[-JOB_PROGRESS_EVENT_LIMIT:]
    progress_summary['stage'] = event.get('stage')
    progress_summary['message'] = event.get('message')
    progress_summary['status'] = 'running'
    return progress_summary


def _mark_job_progress(job_id, stage, message, **details):
    """Persist a bounded in-flight snapshot so long jobs remain diagnosable."""
    event = {
        'timestamp_ms': int(time.time() * 1000),
        'stage': stage,
        'message': message,
    }
    event.update({key: value for key, value in details.items() if value is not None})
    with JOB_METADATA_LOCK:
        metadata = JOB_METADATA.setdefault(job_id, {})
        summary = _append_job_progress_event(metadata.get('last_summary'), event)
        run_id = metadata.get('run_id')
        metadata.update(
            {
                'running': True,
                'last_status': 'running',
                'last_summary': summary,
                'last_updated_at_ms': event['timestamp_ms'],
            }
        )
        metadata['job_id'] = job_id
        snapshot = dict(metadata)
    if run_id is not None:
        try:
            update_job_run(run_id, status='running', summary=summary)
        except Exception:
            logger.exception('任务运行进度记录更新失败: job_id=%s run_id=%s stage=%s', job_id, run_id, stage)
    return snapshot


def _mark_job_finished(job_id, status='success', summary=None, error=None, started_at=None):
    finished_at_ms = int(time.time() * 1000)
    duration_ms = None
    if started_at is not None:
        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
    error_message = format_job_error(error)
    error_detail = format_job_error_detail(error)
    runtime_summary = dict(summary) if isinstance(summary, dict) else summary
    with JOB_METADATA_LOCK:
        previous_summary = (JOB_METADATA.get(job_id) or {}).get('last_summary')
    progress_events = previous_summary.get('progress_events') if isinstance(previous_summary, dict) else None
    if progress_events:
        runtime_summary = dict(runtime_summary or {})
        terminal_messages = {
            'success': '任务完成',
            'partial': '任务未全部完成',
            'partial_success': '任务未全部完成',
            'error': '任务失败',
        }
        runtime_summary['progress_events'] = (
            list(progress_events)
            + [{
                'timestamp_ms': finished_at_ms,
                'stage': 'finished',
                'message': terminal_messages.get(status, f'任务结束：{status}'),
            }]
        )[-JOB_PROGRESS_EVENT_LIMIT:]
    if error_detail:
        persisted_summary = dict(runtime_summary or {})
        persisted_summary['_error_detail'] = error_detail
    else:
        persisted_summary = runtime_summary
    metadata = _update_job_metadata(
        job_id,
        running=False,
        last_finished_at_ms=finished_at_ms,
        last_duration_ms=duration_ms,
        last_status=status,
        last_summary=runtime_summary,
        last_error=error_message,
        last_updated_at_ms=finished_at_ms,
    )
    run_id = metadata.get('run_id')
    if run_id is not None:
        try:
            finish_job_run(
                run_id,
                status=status,
                summary=persisted_summary,
                error=error_message,
                completed_at=finished_at_ms,
                duration_ms=duration_ms,
            )
        except Exception:
            logger.exception('任务运行记录更新失败: job_id=%s run_id=%s', job_id, run_id)
    try:
        from .notifications import EVENT_JOB_FAILURE, evaluate_scheduled_rules
        evaluate_scheduled_rules(EVENT_JOB_FAILURE, metadata=get_all_job_runtime_metadata())
    except Exception:
        logger.exception('任务失败通知评估异常: job_id=%s', job_id)
    return metadata


def _evaluate_market_notifications(event_type):
    try:
        from .notifications import (
            EVENT_FUNDING_RATE,
            EVENT_PRICE_VOLUME,
            EVENT_TRADE_OPPORTUNITY,
            EVENT_PRICE_OI_OUTFLOW_RETEST,
            evaluate_scheduled_rules,
        )
        event_types = {
            'funding_rate': EVENT_FUNDING_RATE,
            'price_volume': EVENT_PRICE_VOLUME,
            'trade_opportunity': EVENT_TRADE_OPPORTUNITY,
            'price_oi_outflow_retest': EVENT_PRICE_OI_OUTFLOW_RETEST,
        }
        if event_type in event_types:
            return evaluate_scheduled_rules(event_types[event_type])
    except Exception:
        logger.exception('市场通知评估异常: event_type=%s', event_type)
    return None


def _rss_summary_error(summary):
    """Build a concise task-page error from failed RSS sources."""
    errors = []
    for result in (summary or {}).get('summaries') or []:
        if result.get('status') != 'error' or not result.get('error'):
            continue
        source = result.get('subscription_name') or f"订阅 {result.get('subscription_id', '--')}"
        errors.append(f'{source}: {result["error"]}')
    return '; '.join(errors)[:500] or None


if RSS_ENABLED:
    @scheduled_job(
        'interval',
        seconds=RSS_POLL_INTERVAL,
        id='rss_monitor_job',
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(),
    )
    def scheduled_rss_monitor():
        """Fetch enabled RSS feeds and notify configured channels about new articles."""
        started_at = time.perf_counter()
        _mark_job_started('rss_monitor_job')
        try:
            from .rss_monitor import monitor_all_subscriptions
            summary = monitor_all_subscriptions()
            _mark_job_finished(
                'rss_monitor_job',
                status=summary.get('status') or 'success',
                summary=summary,
                error=_rss_summary_error(summary),
                started_at=started_at,
            )
            logger.info(
                'RSS 定时监控完成: sources=%s new=%s status=%s',
                summary.get('subscription_count', 0),
                summary.get('new_count', 0),
                summary.get('status'),
            )
        except Exception as exc:
            _mark_job_finished('rss_monitor_job', status='error', error=exc, started_at=started_at)
            logger.error('RSS 定时监控失败: %s', exc)
            logger.exception(exc)


def get_job_runtime_metadata(job_id):
    with JOB_METADATA_LOCK:
        return dict(JOB_METADATA.get(job_id) or {})


def get_all_job_runtime_metadata():
    with JOB_METADATA_LOCK:
        return {job_id: dict(metadata) for job_id, metadata in JOB_METADATA.items()}


_REPAIR_STAGE_SUMMARY_FIELDS = (
    'status',
    'mode',
    'period',
    'series_types',
    'exchanges',
    'success_count',
    'failure_count',
    'skipped_count',
    'precheck_skipped_count',
    'pending_task_count',
    'remaining_task_count',
    'unsupported_count',
    'expected_records',
    'api_records',
    'records',
    'missing_records',
    'written_records',
    'no_data_records',
    'affected',
    'duration_ms',
    'precheck_duration_ms',
    'duration_breakdown_ms',
    'duration_breakdown_by_exchange',
    'duration_breakdown_by_series_type',
    'history_missing_day_stats',
    'coverage_hours',
    'start_time',
    'end_time',
    'full_scan',
    'current_day_trimmed_end_time',
    'message',
    'stage',
)


def _sum_duration_breakdowns(summaries, field='duration_breakdown_ms'):
    total = {}
    for summary in summaries:
        breakdown = summary.get(field) or {}
        if field == 'duration_breakdown_ms':
            breakdowns = {'_total': breakdown}
        else:
            breakdowns = breakdown
        for group, values in breakdowns.items():
            group_total = total.setdefault(group, {})
            for key, value in (values or {}).items():
                group_total[key] = round(group_total.get(key, 0) + float(value or 0), 2)
    return total.get('_total', {}) if field == 'duration_breakdown_ms' else total


def _compact_repair_stage_summary(summary):
    """Keep task history useful without persisting every repair result."""
    results = summary.get('results') or []
    compact = {
        key: summary[key]
        for key in _REPAIR_STAGE_SUMMARY_FIELDS
        if key in summary
    }
    compact['symbol_count'] = len(summary.get('symbols') or [])
    compact['result_count'] = len(results)
    compact['result_status_counts'] = dict(
        Counter(item.get('status') or 'unknown' for item in results)
    )
    compact['skip_reason_counts'] = dict(
        Counter(
            item.get('reason') or 'unknown'
            for item in results
            if item.get('status') == 'skipped'
        )
    )
    compact['failure_reason_counts'] = dict(
        Counter(
            item.get('reason') or 'exception'
            for item in results
            if item.get('status') == 'error'
        )
    )

    exchange_summaries = {}
    for item in results:
        exchange = item.get('exchange') or 'unknown'
        exchange_summary = exchange_summaries.setdefault(
            exchange,
            {
                'exchange': exchange,
                'result_count': 0,
                'success_count': 0,
                'failure_count': 0,
                'skipped_count': 0,
            },
        )
        exchange_summary['result_count'] += 1
        status = item.get('status')
        if status == 'success':
            exchange_summary['success_count'] += 1
        elif status == 'error':
            exchange_summary['failure_count'] += 1
        elif status == 'skipped':
            exchange_summary['skipped_count'] += 1
    if exchange_summaries:
        compact['exchange_summaries'] = exchange_summaries

    progress = summary.get('exchange_progress') or {}
    if progress:
        compact['exchange_progress'] = {
            exchange: {
                key: stats.get(key, 0)
                for key in ('supported_symbols', 'complete', 'pending', 'unsupported')
            }
            for exchange, stats in progress.items()
        }
    return compact


def _merge_repair_summaries(stage_summaries):
    summaries = [summary for summary in stage_summaries if summary]
    compact_stages = [_compact_repair_stage_summary(summary) for summary in summaries]
    status = 'success'
    if any((summary.get('status') or 'success') == 'error' for summary in summaries):
        status = 'error'
    elif any(
        (summary.get('status') or 'success') in {'partial', 'partial_success'}
        or (summary.get('failure_count', 0) or 0) > 0
        or (
            summary.get('mode') == 'history'
            and (
                (summary.get('skipped_count', 0) or 0) > 0
                or (summary.get('remaining_task_count', 0) or 0) > 0
                or (summary.get('missing_records', 0) or 0) > 0
                or (
                    (summary.get('pending_task_count', 0) or 0) > 0
                    and (
                        (summary.get('records', 0) or 0) == 0
                        or (summary.get('written_records', 0) or 0) < (summary.get('records', 0) or 0)
                    )
                )
            )
        )
        for summary in summaries
    ):
        status = 'partial'

    symbols = {
        symbol
        for summary in summaries
        for symbol in (summary.get('symbols') or [])
    }
    merged = {
        'summary_version': 2,
        'status': status,
        'mode': summaries[0].get('mode') if summaries else None,
        'period': summaries[0].get('period') if summaries else None,
        'stages': compact_stages,
        'stage_count': len(compact_stages),
        'symbol_count': len(symbols),
        'result_count': sum(stage.get('result_count', 0) for stage in compact_stages),
        'result_status_counts': dict(
            sum((Counter(stage.get('result_status_counts') or {}) for stage in compact_stages), Counter())
        ),
        'skip_reason_counts': dict(
            sum((Counter(stage.get('skip_reason_counts') or {}) for stage in compact_stages), Counter())
        ),
        'failure_reason_counts': dict(
            sum((Counter(stage.get('failure_reason_counts') or {}) for stage in compact_stages), Counter())
        ),
        'success_count': sum(summary.get('success_count', 0) or 0 for summary in summaries),
        'failure_count': sum(summary.get('failure_count', 0) or 0 for summary in summaries),
        'skipped_count': sum(summary.get('skipped_count', 0) or 0 for summary in summaries),
        'precheck_skipped_count': sum(summary.get('precheck_skipped_count', 0) or 0 for summary in summaries),
        'remaining_task_count': sum(summary.get('remaining_task_count', 0) or 0 for summary in summaries),
        'duration_ms': sum(summary.get('duration_ms', 0) or 0 for summary in summaries),
    }
    merged['series_types'] = list(dict.fromkeys(
        series_type
        for summary in summaries
        for series_type in (summary.get('series_types') or [])
    ))
    merged['exchanges'] = list(dict.fromkeys(
        exchange
        for summary in summaries
        for exchange in (summary.get('exchanges') or [])
    ))
    for field in (
        'pending_task_count',
        'unsupported_count',
        'expected_records',
        'api_records',
        'records',
        'missing_records',
        'written_records',
        'no_data_records',
        'affected',
        'precheck_duration_ms',
    ):
        merged[field] = sum(summary.get(field, 0) or 0 for summary in summaries)
    merged['duration_breakdown_ms'] = _sum_duration_breakdowns(summaries)
    merged['duration_breakdown_by_exchange'] = _sum_duration_breakdowns(
        summaries,
        'duration_breakdown_by_exchange',
    )
    merged['duration_breakdown_by_series_type'] = _sum_duration_breakdowns(
        summaries,
        'duration_breakdown_by_series_type',
    )
    if status == 'partial' and any(summary.get('mode') == 'history' for summary in summaries):
        merged['message'] = '历史缺口未全部补齐'

    exchange_summaries = {}
    for stage in compact_stages:
        for exchange, stats in (stage.get('exchange_summaries') or {}).items():
            target = exchange_summaries.setdefault(
                exchange,
                {
                    'exchange': exchange,
                    'result_count': 0,
                    'success_count': 0,
                    'failure_count': 0,
                    'skipped_count': 0,
                },
            )
            for key in ('result_count', 'success_count', 'failure_count', 'skipped_count'):
                target[key] += stats.get(key, 0) or 0
    if exchange_summaries:
        merged['exchange_summaries'] = exchange_summaries
    return merged


def _iter_history_symbol_batches(symbols):
    """Yield bounded symbol batches so each repair batch can release its records."""
    normalized_symbols = list(symbols or [])
    batch_size = int(REPAIR_HISTORY_SYMBOL_BATCH_SIZE or 0)
    if batch_size <= 0 or len(normalized_symbols) <= batch_size:
        if normalized_symbols:
            yield normalized_symbols
        return
    for start in range(0, len(normalized_symbols), batch_size):
        yield normalized_symbols[start:start + batch_size]


def _run_history_repair_batches(symbols, series_types, max_workers, stage, progress_callback=None):
    summaries = []
    batches = list(_iter_history_symbol_batches(symbols))
    total_batches = len(batches)
    for batch_index, batch_symbols in enumerate(batches, start=1):
        logger.info(
            '历史修补批次: 阶段=%s 批次=%d/%d symbols=%d batch_size=%d',
            stage,
            batch_index,
            total_batches,
            len(batch_symbols),
            REPAIR_HISTORY_SYMBOL_BATCH_SIZE,
        )
        if progress_callback:
            progress_callback(
                stage=f'history_{stage}_batch_started',
                message=f'历史补齐{stage}阶段第{batch_index}/{total_batches}批开始',
                batch_index=batch_index,
                batch_count=total_batches,
                batch_symbols=len(batch_symbols),
            )
        batch_summary = run_history_repair_job(
            symbols=batch_symbols,
            series_types=series_types,
            full_scan=True,
            max_workers=max_workers,
        )
        batch_summary['stage'] = stage
        batch_summary['batch_index'] = batch_index
        batch_summary['batch_count'] = total_batches
        summaries.append(batch_summary)
        if progress_callback:
            progress_callback(
                stage=f'history_{stage}_batch_finished',
                message=f'历史补齐{stage}阶段第{batch_index}/{total_batches}批完成',
                batch_index=batch_index,
                batch_count=total_batches,
                batch_symbols=len(batch_symbols),
                status=batch_summary.get('status'),
                success_count=batch_summary.get('success_count', 0),
                failure_count=batch_summary.get('failure_count', 0),
                skipped_count=batch_summary.get('skipped_count', 0),
                remaining_task_count=batch_summary.get('remaining_task_count', 0),
            )
    return summaries


@scheduled_job(
    'interval',
    seconds=UPDATE_INTERVAL,
    id='market_rank_refresh_job',
    max_instances=1,
    coalesce=True
)
def scheduled_market_rank_refresh():
    """定时刷新行情榜快照数据"""
    started_at = time.perf_counter()
    _mark_job_started('market_rank_refresh_job')
    try:
        summary = refresh_market_tickers()
        _mark_job_finished('market_rank_refresh_job', status=summary.get('status') or 'success', summary=summary, started_at=started_at)
        if summary.get('status') == 'success':
            logger.info(
                '行情榜快照定时刷新完成: 状态=%s, 保存=%s, 快照时间=%s',
                summary.get('status'),
                summary.get('saved_count', 0),
                summary.get('snapshot_time'),
            )
        else:
            logger.warning(
                '行情榜快照定时刷新未成功: 状态=%s, 消息=%s',
                summary.get('status'),
                summary.get('message'),
            )
    except Exception as e:
        _mark_job_finished('market_rank_refresh_job', status='error', error=e, started_at=started_at)
        logger.error('行情榜快照定时刷新任务失败: %s', e)
        logger.exception(e)


if HOMEPAGE_SERIES_REPAIR_ENABLED:
    @scheduled_job(
        'interval',
        seconds=REPAIR_TRACKED_INTERVAL,
        id='repair_market_rolling_job',
        max_instances=1,
        coalesce=True
    )
    def scheduled_repair_market_rolling():
        """轻量滚动修补市场币种所需的多交易所最新序列"""
        started_at = time.perf_counter()
        _mark_job_started('repair_market_rolling_job')
        try:
            _mark_job_progress(
                'repair_market_rolling_job',
                'rolling_scope',
                '正在计算滚动修补范围',
            )
            tracked_symbols = list(dict.fromkeys(get_active_coins()))
            score_symbols = get_market_structure_score_symbols(
                top_volume_limit=FETCH_COINS_TOP_VOLUME_COUNT,
                top_gainers_limit=FETCH_COINS_TOP_GAINERS_COUNT,
                top_losers_limit=FETCH_COINS_TOP_LOSERS_COUNT,
            )
            tracked_symbol_set = set(tracked_symbols)
            top_symbols = [symbol for symbol in score_symbols if symbol not in tracked_symbol_set]
            worker_count = resolve_repair_worker_count(ENABLED_EXCHANGES)
            if not tracked_symbols and not top_symbols:
                _mark_job_finished(
                    'repair_market_rolling_job',
                    status='success',
                    summary={'status': 'success', 'message': 'no market symbols', 'symbols': []},
                    started_at=started_at,
                )
                logger.info('无市场币种，跳过市场滚动修补')
                return
            logger.info(
                '滚动修补阶段 1/2: 开始修补跟踪币种全类型: symbols=%d series_types=%s points=%s max_workers=%s',
                len(tracked_symbols),
                ','.join(HOMEPAGE_REQUIRED_SERIES_TYPES),
                REPAIR_ROLLING_POINTS,
                worker_count,
            )
            _mark_job_progress(
                'repair_market_rolling_job',
                'rolling_tracked_started',
                '开始修补跟踪币种全类型',
                tracked_symbols=len(tracked_symbols),
                top_symbols=len(top_symbols),
            )
            if tracked_symbols:
                tracked_summary = repair_rolling_tracked_symbols(
                    symbols=tracked_symbols,
                    series_types=list(HOMEPAGE_REQUIRED_SERIES_TYPES),
                    points=REPAIR_ROLLING_POINTS,
                    max_workers=worker_count,
                )
            else:
                tracked_summary = {'status': 'success', 'message': 'no tracked symbols', 'symbols': []}
            tracked_summary['stage'] = 'tracked'
            logger.info(
                '滚动修补阶段 1/2: 跟踪币种全类型完成: symbols=%d success=%d failure=%d skipped=%d duration=%s',
                len(tracked_symbols),
                tracked_summary.get('success_count', 0),
                tracked_summary.get('failure_count', 0),
                tracked_summary.get('skipped_count', 0),
                format_duration_ms(tracked_summary.get('duration_ms', 0.0)),
            )
            _mark_job_progress(
                'repair_market_rolling_job',
                'rolling_tracked_finished',
                '跟踪币种全类型修补完成',
                success_count=tracked_summary.get('success_count', 0),
                failure_count=tracked_summary.get('failure_count', 0),
                skipped_count=tracked_summary.get('skipped_count', 0),
            )
            logger.info(
                '滚动修补阶段 2/2: 开始修补 top 榜币种全类型: symbols=%d series_types=%s points=%s max_workers=%s',
                len(top_symbols),
                ','.join(HOMEPAGE_REQUIRED_SERIES_TYPES),
                REPAIR_ROLLING_POINTS,
                worker_count,
            )
            if top_symbols:
                top_summary = repair_rolling_tracked_symbols(
                    symbols=top_symbols,
                    series_types=list(HOMEPAGE_REQUIRED_SERIES_TYPES),
                    points=REPAIR_ROLLING_POINTS,
                    max_workers=worker_count,
                )
            else:
                top_summary = {'status': 'success', 'message': 'no top symbols', 'symbols': []}
            top_summary['stage'] = 'top'
            logger.info(
                '滚动修补阶段 2/2: top 榜币种全类型完成: symbols=%d success=%d failure=%d skipped=%d duration=%s',
                len(top_symbols),
                top_summary.get('success_count', 0),
                top_summary.get('failure_count', 0),
                top_summary.get('skipped_count', 0),
                format_duration_ms(top_summary.get('duration_ms', 0.0)),
            )
            _mark_job_progress(
                'repair_market_rolling_job',
                'rolling_top_finished',
                'top 榜币种全类型修补完成',
                success_count=top_summary.get('success_count', 0),
                failure_count=top_summary.get('failure_count', 0),
                skipped_count=top_summary.get('skipped_count', 0),
            )
            summary = _merge_repair_summaries([tracked_summary, top_summary])
            _mark_job_finished('repair_market_rolling_job', status=summary.get('status') or 'success', summary=summary, started_at=started_at)
            _evaluate_market_notifications('price_volume')
            _evaluate_market_notifications('trade_opportunity')
            _evaluate_market_notifications('price_oi_outflow_retest')
            precheck_complete = summary.get('precheck_skipped_count', 0)
            task_total = (
                (summary.get('success_count', 0) or 0)
                + (summary.get('failure_count', 0) or 0)
                + max(0, (summary.get('skipped_count', 0) or 0) - precheck_complete)
            )
            logger.info(
                '滚动修补市场币种最新点完成: symbols=%d precheck_complete=%d pending_tasks=%d '
                'success=%d failure=%d skipped=%d duration=%s',
                len(tracked_symbols) + len(top_symbols),
                precheck_complete,
                task_total,
                summary.get('success_count', 0),
                summary.get('failure_count', 0),
                summary.get('skipped_count', 0),
                format_duration_ms(summary.get('duration_ms', 0.0)),
            )
        except Exception as e:
            _mark_job_finished('repair_market_rolling_job', status='error', error=e, started_at=started_at)
            logger.error('滚动修补市场币种最新点失败: %s', e)
            logger.exception(e)


if REPAIR_HISTORY_ENABLED:
    @scheduled_job(
        'interval',
        seconds=REPAIR_HISTORY_INTERVAL,
        id='repair_market_history_job',
        max_instances=1,
        coalesce=True
    )
    def scheduled_repair_market_history():
        """Low-frequency historical gap repair."""
        started_at = time.perf_counter()
        _mark_job_started('repair_market_history_job')
        try:
            logger.info('开始执行低频历史补齐任务')
            _mark_job_progress(
                'repair_market_history_job',
                'history_scope',
                '正在计算历史补齐范围',
            )
            tracked_symbols = list(dict.fromkeys(get_active_coins()))
            top_symbols = []
            worker_count = resolve_repair_worker_count(ENABLED_EXCHANGES)
            smart_money_symbols = get_smart_money_scope_symbols()
            if FETCH_COINS_ENABLED:
                all_tickers = get_all_24hr_tickers()
                if all_tickers:
                    market_symbols = get_market_scope_symbols_from_tickers(
                        all_tickers,
                        tracked_symbols=tracked_symbols,
                        top_gainers_limit=FETCH_COINS_TOP_GAINERS_COUNT,
                        top_losers_limit=FETCH_COINS_TOP_LOSERS_COUNT,
                        top_volume_limit=FETCH_COINS_TOP_VOLUME_COUNT,
                    )
                    market_symbols = list(dict.fromkeys(market_symbols + smart_money_symbols))
                    tracked_symbol_set = set(tracked_symbols)
                    top_symbols = [symbol for symbol in market_symbols if symbol not in tracked_symbol_set]
                else:
                    market_symbols = list(dict.fromkeys(tracked_symbols + smart_money_symbols))
                    tracked_symbol_set = set(tracked_symbols)
                    top_symbols = [symbol for symbol in market_symbols if symbol not in tracked_symbol_set]
                    logger.warning(
                        '低频历史补齐获取行情排行失败，将修补跟踪币种和聪明钱范围: symbols=%d',
                        len(market_symbols),
                    )
            else:
                market_symbols = list(dict.fromkeys(tracked_symbols + smart_money_symbols))
                tracked_symbol_set = set(tracked_symbols)
                top_symbols = [symbol for symbol in market_symbols if symbol not in tracked_symbol_set]
            _mark_job_progress(
                'repair_market_history_job',
                'history_scope_ready',
                '历史补齐范围已确定',
                tracked_symbols=len(tracked_symbols),
                top_symbols=len(top_symbols),
                smart_money_symbols=len(smart_money_symbols),
            )
            logger.info(
                '历史修补阶段 1/2: 开始修补跟踪币种全类型: symbols=%d series_types=%s coverage_hours=%s max_workers=%s',
                len(tracked_symbols),
                ','.join(HOMEPAGE_REQUIRED_SERIES_TYPES),
                REPAIR_HISTORY_COVERAGE_HOURS,
                worker_count,
            )
            if tracked_symbols:
                tracked_batch_summaries = _run_history_repair_batches(
                    tracked_symbols,
                    list(HOMEPAGE_REQUIRED_SERIES_TYPES),
                    worker_count,
                    'tracked',
                    progress_callback=lambda **details: _mark_job_progress(
                        'repair_market_history_job', **details
                    ),
                )
                tracked_summary = _merge_repair_summaries(tracked_batch_summaries)
            else:
                tracked_batch_summaries = []
                tracked_summary = {'status': 'success', 'message': 'no tracked symbols', 'symbols': []}
            tracked_summary['stage'] = 'tracked'
            logger.info(
                '历史修补阶段 1/2: 跟踪币种全类型完成: symbols=%d success=%d failure=%d skipped=%d duration=%s',
                len(tracked_symbols),
                tracked_summary.get('success_count', 0),
                tracked_summary.get('failure_count', 0),
                tracked_summary.get('skipped_count', 0),
                format_duration_ms(tracked_summary.get('duration_ms', 0.0)),
            )
            _mark_job_progress(
                'repair_market_history_job',
                'history_tracked_finished',
                '跟踪币种历史补齐完成',
                success_count=tracked_summary.get('success_count', 0),
                failure_count=tracked_summary.get('failure_count', 0),
                skipped_count=tracked_summary.get('skipped_count', 0),
                remaining_task_count=tracked_summary.get('remaining_task_count', 0),
            )
            logger.info(
                '历史修补阶段 2/2: 开始修补 top 榜币种全类型: symbols=%d series_types=%s coverage_hours=%s max_workers=%s',
                len(top_symbols),
                ','.join(HOMEPAGE_REQUIRED_SERIES_TYPES),
                REPAIR_HISTORY_COVERAGE_HOURS,
                worker_count,
            )
            if top_symbols:
                top_batch_summaries = _run_history_repair_batches(
                    top_symbols,
                    list(HOMEPAGE_REQUIRED_SERIES_TYPES),
                    worker_count,
                    'top',
                    progress_callback=lambda **details: _mark_job_progress(
                        'repair_market_history_job', **details
                    ),
                )
                top_summary = _merge_repair_summaries(top_batch_summaries)
            else:
                top_batch_summaries = []
                top_summary = {'status': 'success', 'message': 'no top symbols', 'symbols': []}
            top_summary['stage'] = 'top'
            logger.info(
                '历史修补阶段 2/2: top 榜币种全类型完成: symbols=%d success=%d failure=%d skipped=%d duration=%s',
                len(top_symbols),
                top_summary.get('success_count', 0),
                top_summary.get('failure_count', 0),
                top_summary.get('skipped_count', 0),
                format_duration_ms(top_summary.get('duration_ms', 0.0)),
            )
            _mark_job_progress(
                'repair_market_history_job',
                'history_top_finished',
                'top 榜币种历史补齐完成',
                success_count=top_summary.get('success_count', 0),
                failure_count=top_summary.get('failure_count', 0),
                skipped_count=top_summary.get('skipped_count', 0),
                remaining_task_count=top_summary.get('remaining_task_count', 0),
            )
            summary = _merge_repair_summaries(tracked_batch_summaries + top_batch_summaries)
            _mark_job_finished('repair_market_history_job', status=summary.get('status') or 'success', summary=summary, started_at=started_at)
            logger.info(
                '低频历史补齐任务完成: status=%s symbols=%d success=%d failure=%d skipped=%d duration=%s',
                summary.get('status'),
                len(tracked_symbols) + len(top_symbols),
                summary.get('success_count', 0),
                summary.get('failure_count', 0),
                summary.get('skipped_count', 0),
                format_duration_ms(summary.get('duration_ms', 0.0)),
            )
        except Exception as e:
            _mark_job_finished('repair_market_history_job', status='error', error=e, started_at=started_at)
            logger.error('低频历史补齐任务失败: %s', e)
            logger.exception(e)


if FUNDING_RATE_COLLECT_ENABLED:
    @scheduled_job(
        'interval',
        seconds=REPAIR_TRACKED_INTERVAL,
        id='collect_funding_rates_job',
        max_instances=1,
        coalesce=True
    )
    def scheduled_collect_funding_rates():
        """定时采集资金费率快照（全量 Binance USDT 永续）"""
        started_at = time.perf_counter()
        _mark_job_started('collect_funding_rates_job')
        try:
            count = collect_funding_rates()
            _mark_job_finished(
                'collect_funding_rates_job',
                status='success',
                summary={'status': 'success', 'count': count},
                started_at=started_at,
            )
            _evaluate_market_notifications('funding_rate')
            logger.info('资金费率采集完成: 记录=%d', count)
        except Exception as e:
            _mark_job_finished('collect_funding_rates_job', status='error', error=e, started_at=started_at)
            logger.error('资金费率采集失败: %s', e)


if SMART_MONEY_ENABLED:
    @scheduled_job(
        'interval',
        seconds=SMART_MONEY_INTERVAL,
        id='collect_smart_money_signals_job',
        max_instances=1,
        coalesce=True,
    )
    def scheduled_collect_smart_money_signals():
        """Persist the complete Binance Futures Smart Money lists."""
        started_at = time.perf_counter()
        _mark_job_started('collect_smart_money_signals_job')
        try:
            snapshot_time = align_snapshot_time()
            summaries = [
                collect_and_save_smart_money_signals(
                    time_range=time_range,
                    snapshot_time=snapshot_time,
                )
                for time_range in SMART_MONEY_TIME_RANGES
            ]
            summary = {
                'status': 'success',
                'time_ranges': [item.get('time_range') for item in summaries],
                'list_rows': sum(item.get('list_rows', 0) for item in summaries),
                'list_written': sum(item.get('list_written', 0) for item in summaries),
                'snapshot_time': snapshot_time,
                'summaries': summaries,
            }
            _mark_job_finished(
                'collect_smart_money_signals_job',
                status=summary.get('status', 'success'),
                summary=summary,
                started_at=started_at,
            )
            logger.info(
                '聪明钱全量列表采集完成: ranges=%s rows=%s written=%s snapshot_time=%s',
                summary.get('time_ranges'),
                summary.get('list_rows'),
                summary.get('list_written'),
                summary.get('snapshot_time'),
            )
        except Exception as e:
            _mark_job_finished(
                'collect_smart_money_signals_job',
                status='error',
                error=e,
                started_at=started_at,
            )
            error_message = format_job_error(e)
            logger.error('聪明钱全量列表采集失败: %s', error_message)


@scheduled_job('cron', hour=0, minute=0, id='update_coins_config_job')
def scheduled_coins_config_update():
    """Refresh tracked coin configuration once per day."""
    started_at = time.perf_counter()
    _mark_job_started('update_coins_config_job')
    try:
        logger.info('开始执行定时币种配置刷新任务')
        update_coins_config()
        _mark_job_finished(
            'update_coins_config_job',
            status='success',
            summary={'status': 'success', 'message': 'coins config updated'},
            started_at=started_at,
        )
        logger.info('定时币种配置刷新任务完成')
    except Exception as e:
        _mark_job_finished('update_coins_config_job', status='error', error=e, started_at=started_at)
        logger.error('定时币种配置刷新任务失败: %s', e)
        logger.exception(e)


@scheduled_job('cron', hour=0, minute=20, id='cleanup_market_data_job')
def scheduled_cleanup_market_data():
    """Remove market records outside the market retention window."""
    started_at = time.perf_counter()
    _mark_job_started('cleanup_market_data_job')
    try:
        market_deleted = cleanup_expired_market_data(MARKET_DATA_RETENTION_DAYS)
        smart_money_deleted = cleanup_expired_smart_money_data(SMART_MONEY_RETENTION_DAYS)
        _mark_job_finished(
            'cleanup_market_data_job',
            status='success',
            summary={
                'status': 'success',
                'market_deleted_count': market_deleted,
                'smart_money_deleted_count': smart_money_deleted,
                'market_retention_days': MARKET_DATA_RETENTION_DAYS,
                'smart_money_retention_days': SMART_MONEY_RETENTION_DAYS,
            },
            started_at=started_at,
        )
        logger.info(
            '定时行情数据清理完成: market_deleted=%s market_retention_days=%s',
            market_deleted,
            MARKET_DATA_RETENTION_DAYS,
        )
    except Exception as e:
        _mark_job_finished('cleanup_market_data_job', status='error', error=e, started_at=started_at)
        logger.error('定时行情数据清理失败: %s', e)
        logger.exception(e)


@scheduled_job('cron', hour=0, minute=25, id='cleanup_task_run_history_job')
def scheduled_cleanup_task_run_history():
    """Remove task execution records outside the task-history retention window."""
    started_at = time.perf_counter()
    _mark_job_started('cleanup_task_run_history_job')
    try:
        deleted = delete_expired_job_runs(TASK_RUN_HISTORY_RETENTION_DAYS)
        _mark_job_finished(
            'cleanup_task_run_history_job',
            status='success',
            summary={
                'status': 'success',
                'deleted_count': deleted,
                'task_history_retention_days': TASK_RUN_HISTORY_RETENTION_DAYS,
            },
            started_at=started_at,
        )
        logger.info(
            '定时任务历史清理完成: deleted=%s task_history_retention_days=%s',
            deleted,
            TASK_RUN_HISTORY_RETENTION_DAYS,
        )
    except Exception as e:
        _mark_job_finished('cleanup_task_run_history_job', status='error', error=e, started_at=started_at)
        logger.error('定时任务历史清理失败: %s', e)
        logger.exception(e)


def start_scheduler():
    """Start the background scheduler."""
    if not SCHEDULER_ENABLED:
        logger.info('调度器已禁用（SCHEDULER_ENABLED=false），任务已注册但不会定时执行')
        return
    logger.info('开始启动调度器')
    try:
        paused_job_ids = _get_persisted_paused_job_ids()
        scheduler.start(paused=True)
        try:
            _restore_persisted_paused_jobs(paused_job_ids)
        finally:
            scheduler.resume()
        logger.info('调度器启动成功')
    except Exception as e:
        logger.error('调度器启动失败: %s', e)
        logger.exception(e)
