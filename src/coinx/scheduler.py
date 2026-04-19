from apscheduler.schedulers.background import BackgroundScheduler

from .collector import repair_tracked_symbols, run_series_repair_job, update_market_tickers
from .coin_manager import get_active_coins, update_coins_config
from .config import BINANCE_SERIES_REPAIR_INTERVAL, UPDATE_INTERVAL
from .repositories.homepage_series import (
    HOMEPAGE_REQUIRED_SERIES_TYPES,
    should_refresh_homepage_series,
)
from .utils import logger


scheduler = BackgroundScheduler()


@scheduler.scheduled_job('interval', seconds=UPDATE_INTERVAL, id='update_data_job')
def scheduled_update():
    """Refresh homepage-required series and update market tickers."""
    try:
        logger.info('开始执行定时首页历史序列刷新任务')
        symbols = get_active_coins()

        if should_refresh_homepage_series(symbols):
            repair_tracked_symbols(
                symbols=symbols,
                series_types=list(HOMEPAGE_REQUIRED_SERIES_TYPES),
            )
        else:
            logger.info('本轮首页历史序列已是最新，跳过修补')

        update_market_tickers()
        logger.info('定时首页历史序列刷新任务完成')
    except Exception as e:
        logger.error(f'定时首页历史序列刷新任务失败: {e}')
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
