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
    id='repair_tracked_job',
    max_instances=1,
    coalesce=True
)
def scheduled_repair_tracked():
    """轻量修补跟踪币种最新首页序列"""
    try:
        tracked_coins = get_active_coins()
        if not tracked_coins:
            logger.info('无跟踪币种，跳过修补')
            return
        logger.info(f'开始修补跟踪币种最新点，共 {len(tracked_coins)} 个')
        repair_rolling_tracked_symbols(
            symbols=tracked_coins,
            series_types=list(HOMEPAGE_REQUIRED_SERIES_TYPES),
            points=REPAIR_ROLLING_POINTS,
            max_workers=REPAIR_ROLLING_MAX_WORKERS,
        )
        logger.info('修补跟踪币种最新点完成')
    except Exception as e:
        logger.error(f'修补跟踪币种最新点失败: {e}')
        logger.exception(e)


if FETCH_COINS_ENABLED:
    @scheduler.scheduled_job(
        'interval',
        seconds=FETCH_COINS_INTERVAL,
        id='repair_top_volume_job',
        max_instances=1,
        coalesce=True
    )
    def scheduled_repair_top_volume():
        """修补成交额前N币种历史序列"""
        try:
            all_tickers = get_all_24hr_tickers()
            if not all_tickers:
                logger.warning('获取成交额排行失败')
                return
            top_volume_symbols = [
                t['symbol'] for t in sorted(
                    all_tickers,
                    key=lambda x: x.get('quoteVolume', 0),
                    reverse=True
                )[:FETCH_COINS_TOP_VOLUME_COUNT]
            ]
            logger.info(f'开始修补成交额前{FETCH_COINS_TOP_VOLUME_COUNT}，共 {len(top_volume_symbols)} 个')
            repair_rolling_tracked_symbols(
                symbols=top_volume_symbols,
                series_types=list(HOMEPAGE_REQUIRED_SERIES_TYPES),
                points=REPAIR_ROLLING_POINTS,
                max_workers=REPAIR_ROLLING_MAX_WORKERS,
            )
            logger.info('修补成交额前N完成')
        except Exception as e:
            logger.error(f'修补成交额前N失败: {e}')
            logger.exception(e)


if REPAIR_HISTORY_ENABLED:
    @scheduler.scheduled_job(
        'interval',
        seconds=REPAIR_HISTORY_INTERVAL,
        id='repair_history_job',
        max_instances=1,
        coalesce=True
    )
    def scheduled_repair_history():
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
            summary = run_history_repair_job(
                symbols=symbols,
                series_types=list(HOMEPAGE_REQUIRED_SERIES_TYPES),
            )
            logger.info(
                f"低频历史补齐任务完成: 状态={summary.get('status')}, "
                f"成功={summary.get('success_count', 0)}, 失败={summary.get('failure_count', 0)}, "
                f"跳过={summary.get('skipped_count', 0)}"
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
