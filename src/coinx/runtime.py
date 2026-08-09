import threading
import time

from coinx.coin_manager import get_active_coins
from coinx.config import HOMEPAGE_SERIES_REPAIR_ENABLED, SCHEDULER_ENABLED
from coinx.scheduler import initialize_job_run_history, scheduler, start_scheduler
from coinx.utils import logger

# 条件性导入主页序列修复函数
if HOMEPAGE_SERIES_REPAIR_ENABLED:
    from coinx.scheduler import scheduled_repair_market_rolling


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
    if not HOMEPAGE_SERIES_REPAIR_ENABLED:
        logger.info('主页序列修复任务已禁用，跳过启动修复')
        return None
    logger.info('启动启动期首页序列补全任务')
    repair_thread = threading.Thread(target=scheduled_repair_market_rolling, daemon=True)
    repair_thread.start()
    return repair_thread


def start_runtime_services(with_startup_repair=True, startup_delay_seconds=1):
    from coinx.write_backend import is_clickhouse_write, market_write_health
    if is_clickhouse_write():
        write_health = market_write_health()
        logger.info('ClickHouse market write preflight: %s', write_health)
        if not write_health.get('healthy'):
            raise RuntimeError(f"ClickHouse market write preflight failed: {write_health}")

    if not SCHEDULER_ENABLED:
        # RSS is MySQL control-plane data and must be initialized even when
        # the scheduler is intentionally disabled for a manual test run.
        try:
            from coinx.rss_monitor import ensure_rss_schema
            ensure_rss_schema()
        except Exception as exc:
            logger.warning('RSS 数据表初始化失败，将在 RSS 任务执行时重试: %s', exc)
        logger.info('调度器已禁用（SCHEDULER_ENABLED=false），跳过自动调度与启动补采')
        return {
            'scheduler_thread': None,
            'repair_thread': None,
            'tracked_coins': [],
        }
    initialize_job_run_history()
    logger.info('开始启动运行时服务')
    try:
        from coinx.rss_monitor import ensure_rss_schema
        ensure_rss_schema()
    except Exception as exc:
        logger.warning('RSS 数据表初始化失败，将在 RSS 任务执行时重试: %s', exc)

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
