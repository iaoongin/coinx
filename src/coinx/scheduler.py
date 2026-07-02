import threading
import time

from apscheduler.schedulers.background import BackgroundScheduler

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
    FETCH_COINS_TOP_VOLUME_COUNT,
    FUNDING_RATE_COLLECT_ENABLED,
    ENABLED_EXCHANGES,
    HOMEPAGE_SERIES_REPAIR_ENABLED,
    REPAIR_HISTORY_COVERAGE_HOURS,
    SCHEDULER_ENABLED,
    UPDATE_INTERVAL,
    REPAIR_HISTORY_ENABLED,
    REPAIR_HISTORY_INTERVAL,
    REPAIR_ROLLING_POINTS,
    REPAIR_TRACKED_INTERVAL,
)
from .repositories.funding_rate import collect_funding_rates
from .repositories.homepage_series import HOMEPAGE_REQUIRED_SERIES_TYPES
from .repositories.market_structure_score import get_market_structure_score_symbols
from .collector.timing import format_duration_ms
from .utils import logger


scheduler = BackgroundScheduler()
JOB_METADATA_LOCK = threading.Lock()
JOB_METADATA = {}


def _update_job_metadata(job_id, **fields):
    with JOB_METADATA_LOCK:
        metadata = JOB_METADATA.setdefault(job_id, {})
        metadata.update(fields)
        metadata['job_id'] = job_id
        return dict(metadata)


def _mark_job_started(job_id):
    return _update_job_metadata(
        job_id,
        running=True,
        last_started_at_ms=int(time.time() * 1000),
        last_finished_at_ms=None,
        last_duration_ms=None,
        last_status='running',
        last_summary=None,
        last_error=None,
    )


def _mark_job_finished(job_id, status='success', summary=None, error=None, started_at=None):
    finished_at_ms = int(time.time() * 1000)
    duration_ms = None
    if started_at is not None:
        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
    return _update_job_metadata(
        job_id,
        running=False,
        last_finished_at_ms=finished_at_ms,
        last_duration_ms=duration_ms,
        last_status=status,
        last_summary=summary,
        last_error=str(error) if error else None,
    )


def get_job_runtime_metadata(job_id):
    with JOB_METADATA_LOCK:
        return dict(JOB_METADATA.get(job_id) or {})


def get_all_job_runtime_metadata():
    with JOB_METADATA_LOCK:
        return {job_id: dict(metadata) for job_id, metadata in JOB_METADATA.items()}


def _merge_repair_summaries(stage_summaries):
    summaries = [summary for summary in stage_summaries if summary]
    status = 'success'
    if any((summary.get('status') or 'success') == 'error' for summary in summaries):
        status = 'error'
    elif any((summary.get('failure_count', 0) or 0) > 0 for summary in summaries):
        status = 'partial'

    return {
        'status': status,
        'stages': summaries,
        'success_count': sum(summary.get('success_count', 0) or 0 for summary in summaries),
        'failure_count': sum(summary.get('failure_count', 0) or 0 for summary in summaries),
        'skipped_count': sum(summary.get('skipped_count', 0) or 0 for summary in summaries),
        'precheck_skipped_count': sum(summary.get('precheck_skipped_count', 0) or 0 for summary in summaries),
        'duration_ms': sum(summary.get('duration_ms', 0) or 0 for summary in summaries),
    }


@scheduler.scheduled_job(
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
    @scheduler.scheduled_job(
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
            tracked_symbols = get_active_coins()
            score_symbols = get_market_structure_score_symbols()
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
            summary = _merge_repair_summaries([tracked_summary, top_summary])
            _mark_job_finished('repair_market_rolling_job', status=summary.get('status') or 'success', summary=summary, started_at=started_at)
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
    @scheduler.scheduled_job(
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
            tracked_symbols = get_active_coins()
            top_symbols = []
            worker_count = resolve_repair_worker_count(ENABLED_EXCHANGES)
            if FETCH_COINS_ENABLED:
                all_tickers = get_all_24hr_tickers()
                if all_tickers:
                    top_volume_symbols = [
                        t['symbol'] for t in sorted(
                            all_tickers,
                            key=lambda x: x.get('quoteVolume', 0),
                            reverse=True
                        )[:FETCH_COINS_TOP_VOLUME_COUNT]
                    ]
                    tracked_symbol_set = set(tracked_symbols)
                    top_symbols = [symbol for symbol in top_volume_symbols if symbol not in tracked_symbol_set]
                else:
                    logger.warning('低频历史补齐获取成交额排行失败，仅修补跟踪币种')
            logger.info(
                '历史修补阶段 1/2: 开始修补跟踪币种全类型: symbols=%d series_types=%s coverage_hours=%s max_workers=%s',
                len(tracked_symbols),
                ','.join(HOMEPAGE_REQUIRED_SERIES_TYPES),
                REPAIR_HISTORY_COVERAGE_HOURS,
                worker_count,
            )
            if tracked_symbols:
                tracked_summary = run_history_repair_job(
                    symbols=tracked_symbols,
                    series_types=list(HOMEPAGE_REQUIRED_SERIES_TYPES),
                    max_workers=worker_count,
                )
            else:
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
            logger.info(
                '历史修补阶段 2/2: 开始修补 top 榜币种全类型: symbols=%d series_types=%s coverage_hours=%s max_workers=%s',
                len(top_symbols),
                ','.join(HOMEPAGE_REQUIRED_SERIES_TYPES),
                REPAIR_HISTORY_COVERAGE_HOURS,
                worker_count,
            )
            if top_symbols:
                top_summary = run_history_repair_job(
                    symbols=top_symbols,
                    series_types=list(HOMEPAGE_REQUIRED_SERIES_TYPES),
                    max_workers=worker_count,
                )
            else:
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
            summary = _merge_repair_summaries([tracked_summary, top_summary])
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
    @scheduler.scheduled_job(
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
            logger.info('资金费率采集完成: 记录=%d', count)
        except Exception as e:
            _mark_job_finished('collect_funding_rates_job', status='error', error=e, started_at=started_at)
            logger.error('资金费率采集失败: %s', e)


@scheduler.scheduled_job('cron', hour=0, minute=0, id='update_coins_config_job')
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


def start_scheduler():
    """Start the background scheduler."""
    if not SCHEDULER_ENABLED:
        logger.info('调度器已禁用（SCHEDULER_ENABLED=false），所有定时任务不会执行')
        return
    logger.info('开始启动调度器')
    try:
        scheduler.start()
        logger.info('调度器启动成功')
    except Exception as e:
        logger.error('调度器启动失败: %s', e)
        logger.exception(e)
