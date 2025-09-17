import os

# 数据存储路径
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')

# 确保目录存在
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# 币安API端点 (使用U本位合约API)
BINANCE_BASE_URL = 'https://fapi.binance.com'

# 定时任务间隔（秒）
UPDATE_INTERVAL = 300  # 5分钟更新一次

# 支持的时间间隔 (根据币安API文档)
TIME_INTERVALS = ['5m', '15m', '30m', '1h', '2h', '4h', '6h', '12h', '1d']

# 代理配置
PROXY_HOST = '127.0.0.1'
PROXY_PORT = 7897
PROXY_URL = f'http://{PROXY_HOST}:{PROXY_PORT}'
USE_PROXY = True  # 是否使用代理

# HTTPS代理配置
HTTPS_PROXY_URL = f'http://{PROXY_HOST}:{PROXY_PORT}'