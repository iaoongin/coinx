import os
import re
from pathlib import Path

from dotenv import dotenv_values

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _load_env_profiles(root_dir, environment=None):
    """Load .env plus an optional profile without overriding shell variables."""
    root = Path(root_dir)
    base_path = root / '.env'
    base_values = dotenv_values(base_path) if base_path.exists() else {}
    selected = str(
        environment
        or os.environ.get('COINX_ENV')
        or base_values.get('COINX_ENV')
        or 'local'
    ).strip().lower()
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]*', selected):
        raise ValueError('COINX_ENV must contain only lowercase letters, digits, underscores or hyphens')

    merged = {}
    for path in (base_path, root / f'.env.{selected}'):
        if path.exists():
            merged.update(dotenv_values(path))

    # Explicit process variables always win over profile files.
    for key, value in merged.items():
        if key not in os.environ and value is not None:
            os.environ[key] = str(value)
    os.environ.setdefault('COINX_ENV', selected)
    return selected


COINX_ENV = _load_env_profiles(ROOT_DIR)
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
WEB_AUTH_DISABLED = get_env('WEB_AUTH_DISABLED', False, bool)

# JWT 认证配置
WEB_JWT_SECRET_KEY = get_env('WEB_JWT_SECRET_KEY')
WEB_JWT_ACCESS_TOKEN_EXPIRES_MINUTES = get_env('WEB_JWT_ACCESS_TOKEN_EXPIRES_MINUTES', 1440, int)
WEB_JWT_REFRESH_TOKEN_EXPIRES_DAYS = get_env('WEB_JWT_REFRESH_TOKEN_EXPIRES_DAYS', 30, int)
WEB_JWT_COOKIE_SECURE = get_env('WEB_JWT_COOKIE_SECURE', False, bool)
WEB_JWT_COOKIE_DOMAIN = get_env('WEB_JWT_COOKIE_DOMAIN')

# 定时任务总开关；关闭后不启动 APScheduler 或执行启动期首页数据补采，但仍保留任务定义供管理页展示和手动执行。
SCHEDULER_ENABLED = get_env('SCHEDULER_ENABLED', True, bool)
# When enabled, collection endpoints are read-only and only APScheduler jobs
# may fetch external market data or write collection results.
COLLECTION_SCHEDULER_ONLY = get_env('COLLECTION_SCHEDULER_ONLY', True, bool)

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

DB_TYPE = get_env('DB_TYPE', 'mysql')  # 'mysql' | 'starrocks'
DB_HOST = get_env('DB_HOST', 'localhost')
DB_PORT = get_env('DB_PORT', 3306, int)
DB_USER = get_env('DB_USER', 'root')
DB_PASSWORD = get_env('DB_PASSWORD', '')
DB_NAME = get_env('DB_NAME', 'coinx')
DB_CHARSET = get_env('DB_CHARSET', 'utf8mb4')

# ClickHouse read-shadow configuration. This is deliberately separate from the
# SQLAlchemy/MySQL connection. Shadow reads are disabled unless explicitly
# enabled and never affect the MySQL response path.
CLICKHOUSE_URL = get_env('CLICKHOUSE_URL')
CLICKHOUSE_DATABASE = get_env('CLICKHOUSE_DATABASE', 'coinx')
CLICKHOUSE_USER = get_env('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = get_env('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_READ_SHADOW = get_env('CLICKHOUSE_READ_SHADOW', False, bool)
CLICKHOUSE_READ_TIMEOUT_SECONDS = get_env('CLICKHOUSE_READ_TIMEOUT_SECONDS', 120, int)
CLICKHOUSE_QUERY_MAX_THREADS = get_env('CLICKHOUSE_QUERY_MAX_THREADS', 2, int)
CLICKHOUSE_HOMEPAGE_MAX_WORKERS = get_env('CLICKHOUSE_HOMEPAGE_MAX_WORKERS', 2, int)
CLICKHOUSE_MAX_CONCURRENT_QUERIES = get_env('CLICKHOUSE_MAX_CONCURRENT_QUERIES', 2, int)
CLICKHOUSE_MAX_MEMORY_USAGE_BYTES = get_env('CLICKHOUSE_MAX_MEMORY_USAGE_BYTES', 0, int)


_VALID_MARKET_BACKENDS = ('mysql', 'clickhouse')


def _normalise_market_backend(value, variable_name):
    value = str(value).strip().lower()
    if value not in _VALID_MARKET_BACKENDS:
        choices = ', '.join(repr(item) for item in _VALID_MARKET_BACKENDS)
        raise ValueError(f"{variable_name} must be one of {choices}")
    return value


def resolve_market_backends(
    market_backend=None,
    read_backend=None,
    market_write_backend=None,
):
    """Resolve the unified market backend and optional compatibility overrides.

    MARKET_BACKEND is the normal production switch. The two legacy names
    remain useful for running separate MySQL/ClickHouse instances during a
    migration, so an explicitly supplied legacy value takes precedence over
    the unified value for that direction only.
    """
    unified = _normalise_market_backend(
        'mysql' if market_backend is None else market_backend,
        'MARKET_BACKEND',
    )
    read = _normalise_market_backend(
        unified if read_backend is None else read_backend,
        'READ_BACKEND',
    )
    write = _normalise_market_backend(
        unified if market_write_backend is None else market_write_backend,
        'MARKET_WRITE_BACKEND',
    )
    return unified, read, write


# DB_TYPE remains the control-plane database selection. Market data can use a
# single backend switch while the legacy read/write variables remain explicit
# per-direction overrides for dual-instance verification and rollback.
MARKET_BACKEND, READ_BACKEND, MARKET_WRITE_BACKEND = resolve_market_backends(
    os.getenv('MARKET_BACKEND'),
    os.getenv('READ_BACKEND'),
    os.getenv('MARKET_WRITE_BACKEND'),
)
CLICKHOUSE_WRITE_TIMEOUT_SECONDS = get_env('CLICKHOUSE_WRITE_TIMEOUT_SECONDS', 120, int)
CLICKHOUSE_WRITE_RETRIES = get_env('CLICKHOUSE_WRITE_RETRIES', 3, int)
CLICKHOUSE_WRITE_BATCH_SIZE = get_env('CLICKHOUSE_WRITE_BATCH_SIZE', 500, int)

if DB_TYPE == 'starrocks':
    SR_HOST = get_env('SR_HOST', DB_HOST)
    SR_PORT = get_env('SR_PORT', 9030, int)
    SR_USER = get_env('SR_USER', DB_USER)
    SR_PASSWORD = get_env('SR_PASSWORD', DB_PASSWORD)
    SR_DB = get_env('SR_DB', DB_NAME)
    DATABASE_URI = f"mysql+pymysql://{SR_USER}:{SR_PASSWORD}@{SR_HOST}:{SR_PORT}/{SR_DB}?charset={DB_CHARSET}"
else:
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
FETCH_COINS_TOP_GAINERS_COUNT = get_env('FETCH_COINS_TOP_GAINERS_COUNT', 25, int)
FETCH_COINS_TOP_LOSERS_COUNT = get_env('FETCH_COINS_TOP_LOSERS_COUNT', 25, int)

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

# 定时任务运行记录保留配置
TASK_RUN_HISTORY_RETENTION_DAYS = get_env('TASK_RUN_HISTORY_RETENTION_DAYS', 90, int)

# 资金费率配置
FUNDING_RATE_COLLECT_ENABLED = get_env('FUNDING_RATE_COLLECT_ENABLED', True, bool)
FUNDING_RATE_ABNORMAL_THRESHOLD = get_env(
    'FUNDING_RATE_ABNORMAL_THRESHOLD',
    0.001,  # 0.1%
    float
)

# 通知配置。渠道 URL 加密存入数据库，主密钥只保存在部署环境中。
NOTIFICATIONS_ENABLED = get_env('NOTIFICATIONS_ENABLED', False, bool)
NOTIFICATION_ENCRYPTION_KEY = get_env('NOTIFICATION_ENCRYPTION_KEY')
NOTIFICATION_ENCRYPTION_KEY_VERSION = get_env('NOTIFICATION_ENCRYPTION_KEY_VERSION', 'v1')
NOTIFICATION_TIMEOUT_SECONDS = get_env('NOTIFICATION_TIMEOUT_SECONDS', 5, int)

# RSS 订阅监控。订阅源和文章保存在数据库，monitor_enabled 由管理页面单独控制推送。
RSS_ENABLED = get_env('RSS_ENABLED', True, bool)
RSS_POLL_INTERVAL = get_env('RSS_POLL_INTERVAL', 600, int)
RSS_REQUEST_TIMEOUT_SECONDS = get_env('RSS_REQUEST_TIMEOUT_SECONDS', 20, int)
RSS_USER_AGENT = get_env('RSS_USER_AGENT', 'CoinX RSS Monitor/1.0')
RSS_PROXY_BASE_URL = get_env('RSS_PROXY_BASE_URL', 'https://proxy.yffjglcms.com')
