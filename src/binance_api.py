import sys
import os

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, project_root)

# Re-export everything from the new src.binance package to maintain backward compatibility
from src.binance.client import (
    get_session,
    request_with_retry as _request_with_retry
)
from src.binance.cache import (
    CACHE_FILE,
    get_cache_key,
    load_cached_data,
    save_cached_data,
    should_update_cache,
    load_drop_list_cache,
    save_drop_list_cache,
    should_update_drop_list_cache
)
from src.binance.market import (
    get_futures_kline_latest as _get_futures_kline_latest,
    aggregate_futures_kline as _aggregate_futures_kline,
    get_latest_price,
    get_24hr_ticker,
    get_all_24hr_tickers,
    get_open_interest,
    get_open_interest_history,
    get_funding_rate,
    get_long_short_ratio
)
from src.binance.indicators import (
    get_net_inflow_data,
    get_exchange_distribution_real
)
from src.binance.service import (
    get_all_coins_list,
    update_all_data,
    update_single_coin_data,
    update_drop_list_data
)
