from apscheduler.schedulers.background import BackgroundScheduler
from .binance_api import update_all_data
from .coin_manager import update_coins_config
from .utils import logger
from .config import UPDATE_INTERVAL

# 创建调度器
scheduler = BackgroundScheduler()

@scheduler.scheduled_job('interval', seconds=UPDATE_INTERVAL, id='update_data_job')
def scheduled_update():
    """定时更新数据的任务"""
    try:
        logger.info("开始执行定时数据更新任务...")
        # 获取活跃币种列表（用于Web展示）
        from .coin_manager import get_active_coins
        symbols = get_active_coins()
        
        # 检查是否需要更新缓存（基于自然5分钟间隔）
        from .binance_api import should_update_cache
        if not should_update_cache():
            logger.info("当前5分钟周期内已有缓存数据，跳过定时更新")
            return
        
        # 更新已启用跟踪的币种数据（仅限活跃币种）
        from .binance_api import update_all_data, update_drop_list_data
        update_all_data(symbols=symbols)
        
        # 更新跌幅榜数据
        update_drop_list_data()
        
        logger.info("定时数据更新任务执行完成")
    except Exception as e:
        logger.error(f"定时任务执行失败: {e}")
        logger.exception(e)  # 记录详细的异常信息

@scheduler.scheduled_job('cron', hour=0, minute=0, id='update_coins_config_job')
def scheduled_coins_config_update():
    """定时更新币种配置的任务（每天凌晨执行）"""
    try:
        logger.info("开始执行币种配置更新任务...")
        update_coins_config()
        logger.info("币种配置更新任务执行完成")
    except Exception as e:
        logger.error(f"币种配置更新任务执行失败: {e}")
        logger.exception(e)  # 记录详细的异常信息

def start_scheduler():
    """启动调度器"""
    logger.info("启动数据更新调度器...")
    try:
        scheduler.start()
        logger.info("调度器启动成功")
    except Exception as e:
        logger.error(f"调度器启动失败: {e}")
        logger.exception(e)