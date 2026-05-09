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
WEB_SESSION_SECRET = get_env('WEB_SESSION_SECRET')

UPDATE_INTERVAL = get_env('UPDATE_INTERVAL', 300, int)
TIME_INTERVALS = get_env(
    'TIME_INTERVALS',
    '5m,15m,30m,1h,4h,12h,24h,48h,72h,168h',
    list
)

BINANCE_SERIES_LIMIT = get_env('BINANCE_SERIES_LIMIT', 30, int)
BINANCE_SERIES_TYPES = get_env(
    'BINANCE_SERIES_TYPES',
    'top_long_short_position_ratio,top_long_short_account_ratio,open_interest_hist,klines,global_long_short_account_ratio,taker_buy_sell_vol',
    list
)
BINANCE_SERIES_PERIODS = get_env(
    'BINANCE_SERIES_PERIODS',
    TIME_INTERVALS,
    list
)

BINANCE_SERIES_REPAIR_ENABLED = get_env('BINANCE_SERIES_REPAIR_ENABLED', False, bool)
BINANCE_SERIES_REPAIR_INTERVAL = get_env('BINANCE_SERIES_REPAIR_INTERVAL', 900, int)
BINANCE_SERIES_REPAIR_PERIOD = get_env('BINANCE_SERIES_REPAIR_PERIOD', '5m')
BINANCE_SERIES_REPAIR_BOOTSTRAP_DAYS = get_env('BINANCE_SERIES_REPAIR_BOOTSTRAP_DAYS', 7, int)
BINANCE_SERIES_REPAIR_COVERAGE_HOURS = get_env('BINANCE_SERIES_REPAIR_COVERAGE_HOURS', 168, int)
BINANCE_SERIES_REPAIR_KLINES_PAGE_LIMIT = get_env('BINANCE_SERIES_REPAIR_KLINES_PAGE_LIMIT', 1000, int)
BINANCE_SERIES_REPAIR_FUTURES_PAGE_LIMIT = get_env('BINANCE_SERIES_REPAIR_FUTURES_PAGE_LIMIT', 500, int)
BINANCE_SERIES_REPAIR_SLEEP_MS = get_env('BINANCE_SERIES_REPAIR_SLEEP_MS', 500, int)

HOMEPAGE_SERIES_REPAIR_PERIOD = get_env('HOMEPAGE_SERIES_REPAIR_PERIOD', BINANCE_SERIES_REPAIR_PERIOD)
HOMEPAGE_SERIES_REPAIR_PAGE_LIMIT = get_env(
    'HOMEPAGE_SERIES_REPAIR_PAGE_LIMIT',
    BINANCE_SERIES_REPAIR_FUTURES_PAGE_LIMIT,
    int
)
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

DB_HOST = get_env('DB_HOST', 'localhost')
DB_PORT = get_env('DB_PORT', 3306, int)
DB_USER = get_env('DB_USER', 'root')
DB_PASSWORD = get_env('DB_PASSWORD', '')
DB_NAME = get_env('DB_NAME', 'coinx')
DB_CHARSET = get_env('DB_CHARSET', 'utf8mb4')

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset={DB_CHARSET}"

BINANCE_BASE_URL = get_env('BINANCE_BASE_URL', 'https://api.binance.com')

ENABLED_EXCHANGES = get_env('ENABLED_EXCHANGES', 'binance,okx', list)
PRIMARY_PRICE_EXCHANGE = get_env('PRIMARY_PRICE_EXCHANGE', 'binance')
OKX_BASE_URL = get_env('OKX_BASE_URL', 'https://www.okx.com')
OKX_RUBIK_MIN_INTERVAL_MS = get_env('OKX_RUBIK_MIN_INTERVAL_MS', 1200, int)
OKX_429_RETRY_FALLBACK_SECONDS = get_env('OKX_429_RETRY_FALLBACK_SECONDS', 5, int)
BYBIT_BASE_URL = get_env('BYBIT_BASE_URL', 'https://api.bybit.com')
BYBIT_CATEGORY = get_env('BYBIT_CATEGORY', 'linear')

# 币种拉取任务配置
FETCH_COINS_ENABLED = get_env('FETCH_COINS_ENABLED', True, bool)
FETCH_COINS_INTERVAL = get_env('FETCH_COINS_INTERVAL', 600, int)
FETCH_COINS_TOP_VOLUME_COUNT = get_env('FETCH_COINS_TOP_VOLUME_COUNT', 100, int)

# 跟踪币种修补任务配置
REPAIR_TRACKED_INTERVAL = get_env('REPAIR_TRACKED_INTERVAL', 300, int)
REPAIR_ROLLING_POINTS = get_env('REPAIR_ROLLING_POINTS', 5, int)
REPAIR_ROLLING_MAX_WORKERS = get_env('REPAIR_ROLLING_MAX_WORKERS', 6, int)
REPAIR_HISTORY_ENABLED = get_env('REPAIR_HISTORY_ENABLED', True, bool)
REPAIR_HISTORY_INTERVAL = get_env('REPAIR_HISTORY_INTERVAL', 3600, int)
REPAIR_HISTORY_MAX_WORKERS = get_env('REPAIR_HISTORY_MAX_WORKERS', 2, int)
REPAIR_HISTORY_SYMBOL_BATCH_SIZE = get_env('REPAIR_HISTORY_SYMBOL_BATCH_SIZE', 20, int)
REPAIR_HISTORY_COVERAGE_HOURS = get_env('REPAIR_HISTORY_COVERAGE_HOURS', 168, int)
