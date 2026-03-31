from apscheduler.schedulers.background import BackgroundScheduler

from .collector import (
    update_all_data,
    should_update_cache,
    update_drop_list_data,
    run_series_repair_job,
)
from .coin_manager import update_coins_config, get_active_coins
from .utils import logger
from .config import (
    UPDATE_INTERVAL,
    BINANCE_SERIES_REPAIR_INTERVAL,
)


scheduler = BackgroundScheduler()


@scheduler.scheduled_job('interval', seconds=UPDATE_INTERVAL, id='update_data_job')
def scheduled_update():
    """定时更新展示数据。"""
    try:
        logger.info("开始执行定时数据更新任务")
        symbols = get_active_coins()

        if not should_update_cache():
            logger.info("当前周期内已有缓存数据，跳过展示数据更新")
            return

        update_all_data(symbols=symbols)
        update_drop_list_data()
        logger.info("定时数据更新任务执行完成")
    except Exception as e:
        logger.error(f"定时任务执行失败: {e}")
        logger.exception(e)


@scheduler.scheduled_job('interval', seconds=BINANCE_SERIES_REPAIR_INTERVAL, id='binance_series_repair_job')
def scheduled_binance_series_repair_update():
    """定时修补 Binance 历史序列数据。"""
    try:
        summary = run_series_repair_job()
        logger.info(
            f"Binance 历史序列修补任务完成: status={summary.get('status')}, "
            f"success={summary.get('success_count', 0)}, failure={summary.get('failure_count', 0)}"
        )
    except Exception as e:
        logger.error(f"Binance 历史序列修补任务失败: {e}")
        logger.exception(e)


@scheduler.scheduled_job('cron', hour=0, minute=0, id='update_coins_config_job')
def scheduled_coins_config_update():
    """定时更新币种配置，每天凌晨执行。"""
    try:
        logger.info("开始执行币种配置更新任务")
        update_coins_config()
        logger.info("币种配置更新任务执行完成")
    except Exception as e:
        logger.error(f"币种配置更新任务执行失败: {e}")
        logger.exception(e)


def start_scheduler():
    """启动调度器。"""
    logger.info("启动数据更新调度器")
    try:
        scheduler.start()
        logger.info("调度器启动成功")
    except Exception as e:
        logger.error(f"调度器启动失败: {e}")
        logger.exception(e)
