from apscheduler.schedulers.background import BackgroundScheduler

from .collector import repair_tracked_symbols, run_series_repair_job, get_all_24hr_tickers
from .coin_manager import get_active_coins, update_coins_config
from .config import (
    BINANCE_SERIES_REPAIR_INTERVAL,
    FETCH_COINS_ENABLED,
    FETCH_COINS_INTERVAL,
    FETCH_COINS_TOP_VOLUME_COUNT,
    REPAIR_TRACKED_INTERVAL,
)
from .repositories.homepage_series import HOMEPAGE_REQUIRED_SERIES_TYPES
from .utils import logger


scheduler = BackgroundScheduler()


@scheduler.scheduled_job(
    'interval',
    seconds=REPAIR_TRACKED_INTERVAL,
    id='repair_tracked_job',
    max_instances=1,
    coalesce=True
)
def scheduled_repair_tracked():
    """修补跟踪币种历史序列"""
    try:
        tracked_coins = get_active_coins()
        if not tracked_coins:
            logger.info('无跟踪币种，跳过修补')
            return
        logger.info(f'开始修补跟踪币种，共 {len(tracked_coins)} 个')
        repair_tracked_symbols(
            symbols=tracked_coins,
            series_types=list(HOMEPAGE_REQUIRED_SERIES_TYPES),
        )
        logger.info('修补跟踪币种完成')
    except Exception as e:
        logger.error(f'修补跟踪币种失败: {e}')
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
            repair_tracked_symbols(
                symbols=top_volume_symbols,
                series_types=list(HOMEPAGE_REQUIRED_SERIES_TYPES),
            )
            logger.info('修补成交额前N完成')
        except Exception as e:
            logger.error(f'修补成交额前N失败: {e}')
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