import os
from dotenv import load_dotenv

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(ROOT_DIR, '.env'))
DATA_DIR = os.path.join(ROOT_DIR, 'data')
LOGS_DIR = os.path.join(ROOT_DIR, 'logs')

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)


def get_env(key, default=None, type_func=None):
    val = os.getenv(key)
    if val is not None:
        if type_func:
            try:
                if type_func == bool:
                    return val.strip().lower() in ('1', 'true', 'yes', 'y', 'on')
                if type_func == list:
                    return [v.strip() for v in val.split(',') if v.strip()]
                return type_func(val)
            except:
                pass
        return val
    return default


WEB_HOST = get_env('WEB_HOST', '0.0.0.0')
WEB_PORT = get_env('WEB_PORT', 5500, int)
WEB_DEBUG = get_env('WEB_DEBUG', False, bool)
WEB_USERNAME = get_env('WEB_USERNAME', 'admin')
WEB_PASSWORD = get_env('WEB_PASSWORD')

# JWT 认证配置
WEB_JWT_SECRET_KEY = get_env('WEB_JWT_SECRET_KEY')
WEB_JWT_ACCESS_TOKEN_EXPIRES_MINUTES = get_env('WEB_JWT_ACCESS_TOKEN_EXPIRES_MINUTES', 1440, int)
WEB_JWT_REFRESH_TOKEN_EXPIRES_DAYS = get_env('WEB_JWT_REFRESH_TOKEN_EXPIRES_DAYS', 30, int)
WEB_JWT_COOKIE_SECURE = get_env('WEB_JWT_COOKIE_SECURE', False, bool)
WEB_JWT_COOKIE_DOMAIN = get_env('WEB_JWT_COOKIE_DOMAIN')

UPDATE_INTERVAL = get_env('UPDATE_INTERVAL', 300, int)
TIME_INTERVALS = get_env(
    'TIME_INTERVALS',
    '5m,15m,30m,1h,4h,12h,24h,48h,72h,168h',
    list
)

HOMEPAGE_SERIES_REPAIR_ENABLED = get_env('HOMEPAGE_SERIES_REPAIR_ENABLED', True, bool)
HOMEPAGE_SERIES_REPAIR_PERIOD = get_env('HOMEPAGE_SERIES_REPAIR_PERIOD', '5m')
HOMEPAGE_SERIES_REPAIR_PAGE_LIMIT = get_env(
    'HOMEPAGE_SERIES_REPAIR_PAGE_LIMIT',
    500,
    int
)
HOMEPAGE_WINDOW_HEALTH_THRESHOLD = get_env('HOMEPAGE_WINDOW_HEALTH_THRESHOLD', 95, int)

HOMEPAGE_SERIES_TYPES = get_env(
    'HOMEPAGE_SERIES_TYPES',
    'klines,open_interest_hist,taker_buy_sell_vol',
    list
)

PROXY_HOST = get_env('PROXY_HOST', '127.0.0.1')
PROXY_PORT = get_env('PROXY_PORT', 7897, int)
PROXY_URL = f'http://{PROXY_HOST}:{PROXY_PORT}'
USE_PROXY = get_env('USE_PROXY', False, bool)
HTTPS_PROXY_URL = f'http://{PROXY_HOST}:{PROXY_PORT}'
USE_PROXY_POOL = get_env('USE_PROXY_POOL', False, bool)
PROXY_POOL_URLS = get_env('PROXY_POOL_URLS', '')
PROXY_POOL_STRATEGY = get_env('PROXY_POOL_STRATEGY', 'round_robin')
PROXY_POOL_FAIL_COOLDOWN_SECONDS = get_env('PROXY_POOL_FAIL_COOLDOWN_SECONDS', 30, int)

DB_HOST = get_env('DB_HOST', 'localhost')
DB_PORT = get_env('DB_PORT', 3306, int)
DB_USER = get_env('DB_USER', 'root')
DB_PASSWORD = get_env('DB_PASSWORD', '')
DB_NAME = get_env('DB_NAME', 'coinx')
DB_CHARSET = get_env('DB_CHARSET', 'utf8mb4')

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset={DB_CHARSET}"

BINANCE_BASE_URL = get_env('BINANCE_BASE_URL', 'https://proxy.yffjglcms.com/fapi.binance.com')

ENABLED_EXCHANGES = get_env('ENABLED_EXCHANGES', 'binance,okx,gate', list)
PRIMARY_PRICE_EXCHANGE = get_env('PRIMARY_PRICE_EXCHANGE', 'binance')
OKX_BASE_URL = get_env('OKX_BASE_URL', 'https://proxy.yffjglcms.com/www.okx.com')
OKX_RUBIK_MIN_INTERVAL_MS = get_env('OKX_RUBIK_MIN_INTERVAL_MS', 500, int)
OKX_429_RETRY_FALLBACK_SECONDS = get_env('OKX_429_RETRY_FALLBACK_SECONDS', 5, int)
BYBIT_BASE_URL = get_env('BYBIT_BASE_URL', 'https://proxy.yffjglcms.com/api.bybit.com')
BYBIT_CATEGORY = get_env('BYBIT_CATEGORY', 'linear')
GATE_BASE_URL = get_env('GATE_BASE_URL', 'https://proxy.yffjglcms.com/api.gateio.ws')
GATE_SETTLE = get_env('GATE_SETTLE', 'usdt')
GATE_MIN_INTERVAL_MS = get_env('GATE_MIN_INTERVAL_MS', 60, int)
GATE_403_RETRY_FALLBACK_SECONDS = get_env('GATE_403_RETRY_FALLBACK_SECONDS', 8, int)

# 币种拉取任务配置
FETCH_COINS_ENABLED = get_env('FETCH_COINS_ENABLED', True, bool)
FETCH_COINS_INTERVAL = get_env('FETCH_COINS_INTERVAL', 600, int)
FETCH_COINS_TOP_VOLUME_COUNT = get_env('FETCH_COINS_TOP_VOLUME_COUNT', 100, int)

# 跟踪币种修补任务配置
REPAIR_TRACKED_INTERVAL = get_env('REPAIR_TRACKED_INTERVAL', 300, int)
REPAIR_ROLLING_POINTS = get_env('REPAIR_ROLLING_POINTS', 5, int)
REPAIR_ROLLING_MAX_WORKERS = get_env('REPAIR_ROLLING_MAX_WORKERS', 6, int)
REPAIR_ROLLING_WRITE_BATCH_SIZE = get_env('REPAIR_ROLLING_WRITE_BATCH_SIZE', 500, int)
REPAIR_HISTORY_ENABLED = get_env('REPAIR_HISTORY_ENABLED', True, bool)
REPAIR_HISTORY_INTERVAL = get_env('REPAIR_HISTORY_INTERVAL', 3600, int)
REPAIR_HISTORY_MAX_WORKERS = get_env('REPAIR_HISTORY_MAX_WORKERS', 2, int)
REPAIR_HISTORY_WRITE_BATCH_SIZE = get_env('REPAIR_HISTORY_WRITE_BATCH_SIZE', 2000, int)
REPAIR_HISTORY_SYMBOL_BATCH_SIZE = get_env('REPAIR_HISTORY_SYMBOL_BATCH_SIZE', 0, int)
REPAIR_HISTORY_COVERAGE_HOURS = get_env('REPAIR_HISTORY_COVERAGE_HOURS', 168, int)

# 资金费率配置
FUNDING_RATE_COLLECT_ENABLED = get_env('FUNDING_RATE_COLLECT_ENABLED', True, bool)
FUNDING_RATE_ABNORMAL_THRESHOLD = get_env(
    'FUNDING_RATE_ABNORMAL_THRESHOLD',
    0.001,  # 0.1%
    float
)
