from apscheduler.schedulers.blocking import BlockingScheduler
from .binance_api import update_all_data
from .utils import logger
from .config import UPDATE_INTERVAL

# 创建调度器
scheduler = BlockingScheduler()

@scheduler.scheduled_job('interval', seconds=UPDATE_INTERVAL, id='update_data_job')
def scheduled_update():
    """定时更新数据的任务"""
    try:
        update_all_data()
    except Exception as e:
        logger.error(f"定时任务执行失败: {e}")

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