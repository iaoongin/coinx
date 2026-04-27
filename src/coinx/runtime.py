import threading
import time

from coinx.coin_manager import get_active_coins
from coinx.scheduler import scheduler, scheduled_repair_tracked, start_scheduler
from coinx.utils import logger


def log_startup_self_check():
    tracked_coins = get_active_coins()
    logger.info(
        '启动自检: 调度器运行=%s, 跟踪币种数=%d',
        scheduler.running,
        len(tracked_coins),
    )
    if not tracked_coins:
        logger.warning('启动自检: 当前没有任何跟踪币种，历史序列补全任务不会产生首页数据')
    return tracked_coins


def start_startup_repair():
    logger.info('启动启动期首页序列补全任务')
    repair_thread = threading.Thread(target=scheduled_repair_tracked, daemon=True)
    repair_thread.start()
    return repair_thread


def start_runtime_services(with_startup_repair=True, startup_delay_seconds=1):
    logger.info('开始启动运行时服务')
    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
    scheduler_thread.start()

    if startup_delay_seconds:
        time.sleep(startup_delay_seconds)

    tracked_coins = log_startup_self_check()

    repair_thread = None
    if with_startup_repair:
        repair_thread = start_startup_repair()

    return {
        'scheduler_thread': scheduler_thread,
        'repair_thread': repair_thread,
        'tracked_coins': tracked_coins,
    }
