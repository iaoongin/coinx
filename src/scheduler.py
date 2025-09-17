from apscheduler.schedulers.blocking import BlockingScheduler
from .binance_api import update_all_data
from .coin_manager import update_coins_config
from .utils import logger
from .config import UPDATE_INTERVAL

# 创建调度器
scheduler = BlockingScheduler()

@scheduler.scheduled_job('interval', seconds=UPDATE_INTERVAL, id='update_data_job')
def scheduled_update():
    """定时更新数据的任务"""
    try:
        # 获取活跃币种列表
        from .coin_manager import get_active_coins
        symbols = get_active_coins()
        
        # 更新数据
        update_all_data(symbols)
    except Exception as e:
        logger.error(f"定时任务执行失败: {e}")

@scheduler.scheduled_job('cron', hour=0, minute=0, id='update_coins_config_job')
def scheduled_coins_config_update():
    """定时更新币种配置的任务（每天凌晨执行）"""
    try:
        logger.info("开始执行币种配置更新任务...")
        update_coins_config()
        logger.info("币种配置更新任务执行完成")
    except Exception as e:
        logger.error(f"币种配置更新任务执行失败: {e}")

def start_scheduler():
    """启动调度器"""
    logger.info("启动数据更新调度器...")
    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("调度器已停止")
        scheduler.shutdown()

if __name__ == "__main__":
    start_scheduler()