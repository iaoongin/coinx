from apscheduler.schedulers.background import BackgroundScheduler

from .collector import (
    get_all_24hr_tickers,
    refresh_market_tickers,
    repair_rolling_tracked_symbols,
    run_history_repair_job,
    run_series_repair_job,
)
from .coin_manager import get_active_coins, update_coins_config
from .config import (
    BINANCE_SERIES_REPAIR_INTERVAL,
    FETCH_COINS_ENABLED,
    FETCH_COINS_INTERVAL,
    FETCH_COINS_TOP_VOLUME_COUNT,
    UPDATE_INTERVAL,
    REPAIR_HISTORY_ENABLED,
    REPAIR_HISTORY_INTERVAL,
    REPAIR_ROLLING_MAX_WORKERS,
    REPAIR_ROLLING_POINTS,
    REPAIR_TRACKED_INTERVAL,
)
from .repositories.homepage_series import HOMEPAGE_REQUIRED_SERIES_TYPES
from .repositories.market_structure_score import get_market_structure_score_symbols
from .utils import logger


scheduler = BackgroundScheduler()


@scheduler.scheduled_job(
    'interval',
    seconds=UPDATE_INTERVAL,
    id='market_rank_refresh_job',
    max_instances=1,
    coalesce=True
)
def scheduled_market_rank_refresh():
    """定时刷新行情榜快照数据"""
    try:
        summary = refresh_market_tickers()
        if summary.get('status') == 'success':
            logger.info(
                f"行情榜快照定时刷新完成: 状态={summary.get('status')}, "
                f"保存={summary.get('saved_count', 0)}, 快照时间={summary.get('snapshot_time')}"
            )
        else:
            logger.warning(
                f"行情榜快照定时刷新未成功: 状态={summary.get('status')}, "
                f"消息={summary.get('message')}"
            )
    except Exception as e:
        logger.error(f'行情榜快照定时刷新任务失败: {e}')
        logger.exception(e)


@scheduler.scheduled_job(
    'interval',
    seconds=REPAIR_TRACKED_INTERVAL,
    id='repair_market_rolling_job',
    max_instances=1,
    coalesce=True
)
def scheduled_repair_market_rolling():
    """轻量滚动修补市场币种所需的多交易所最新序列"""
    try:
        score_symbols = get_market_structure_score_symbols()
        if not score_symbols:
            logger.info('无市场币种，跳过市场滚动修补')
            return
        logger.info(
            '开始滚动修补市场币种最新点: symbols=%d series_types=%s points=%s max_workers=%s',
            len(score_symbols),
            ','.join(HOMEPAGE_REQUIRED_SERIES_TYPES),
            REPAIR_ROLLING_POINTS,
            REPAIR_ROLLING_MAX_WORKERS,
        )
        summary = repair_rolling_tracked_symbols(
            symbols=score_symbols,
            series_types=list(HOMEPAGE_REQUIRED_SERIES_TYPES),
            points=REPAIR_ROLLING_POINTS,
            max_workers=REPAIR_ROLLING_MAX_WORKERS,
        )
        precheck_complete = summary.get('precheck_skipped_count', 0)
        task_total = (
            (summary.get('success_count', 0) or 0)
            + (summary.get('failure_count', 0) or 0)
            + max(0, (summary.get('skipped_count', 0) or 0) - precheck_complete)
        )
        logger.info(
            '滚动修补市场币种最新点完成: symbols=%d precheck_complete=%d pending_tasks=%d '
            'success=%d failure=%d skipped=%d duration_ms=%.2f',
            len(score_symbols),
            precheck_complete,
            task_total,
            summary.get('success_count', 0),
            summary.get('failure_count', 0),
            summary.get('skipped_count', 0),
            summary.get('duration_ms', 0.0),
        )
    except Exception as e:
        logger.error(f'滚动修补市场币种最新点失败: {e}')
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
        try:
            logger.info('开始执行低频历史补齐任务')
            symbols = get_active_coins()
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
                    symbols = list(dict.fromkeys([*symbols, *top_volume_symbols]))
                else:
                    logger.warning('低频历史补齐获取成交额排行失败，仅修补跟踪币种')
            logger.info(
                '开始低频历史补齐: symbols=%d series_types=%s coverage_hours=%s max_workers=%s',
                len(symbols),
                ','.join(HOMEPAGE_REQUIRED_SERIES_TYPES),
                REPAIR_HISTORY_COVERAGE_HOURS,
                REPAIR_HISTORY_MAX_WORKERS,
            )
            summary = run_history_repair_job(
                symbols=symbols,
                series_types=list(HOMEPAGE_REQUIRED_SERIES_TYPES),
            )
            logger.info(
                '低频历史补齐任务完成: status=%s symbols=%d success=%d failure=%d skipped=%d duration_ms=%.2f',
                summary.get('status'),
                len(symbols),
                summary.get('success_count', 0),
                summary.get('failure_count', 0),
                summary.get('skipped_count', 0),
                summary.get('duration_ms', 0.0),
            )
        except Exception as e:
            logger.error(f'低频历史补齐任务失败: {e}')
            logger.exception(e)


@scheduler.scheduled_job('interval', seconds=BINANCE_SERIES_REPAIR_INTERVAL, id='binance_series_repair_job')
def scheduled_binance_series_repair_update():
    """Run the configured generic Binance series repair job."""
    try:
        summary = run_series_repair_job()
        logger.info(
            f"Binance 历史序列定时修补完成: 状态={summary.get('status')}, "
            f"成功={summary.get('success_count', 0)}, 失败={summary.get('failure_count', 0)}"
        )
    except Exception as e:
        logger.error(f'Binance 历史序列定时修补任务失败: {e}')
        logger.exception(e)


@scheduler.scheduled_job('cron', hour=0, minute=0, id='update_coins_config_job')
def scheduled_coins_config_update():
    """Refresh tracked coin configuration once per day."""
    try:
        logger.info('开始执行定时币种配置刷新任务')
        update_coins_config()
        logger.info('定时币种配置刷新任务完成')
    except Exception as e:
        logger.error(f'定时币种配置刷新任务失败: {e}')
        logger.exception(e)


def start_scheduler():
    """Start the background scheduler."""
    logger.info('开始启动调度器')
    try:
        scheduler.start()
        logger.info('调度器启动成功')
    except Exception as e:
        logger.error(f'调度器启动失败: {e}')
        logger.exception(e)
