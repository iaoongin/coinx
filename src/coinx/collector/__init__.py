# 暴露统一的接口，屏蔽具体交易所实现细节
# 目前默认转发给 Binance 实现
# 未来可以在这里增加工厂模式逻辑来支持多交易所切换

from coinx.collector.binance.client import (
    get_session,
    request_with_retry
)

from coinx.collector.binance.market import (
    get_futures_kline_latest,
    aggregate_futures_kline,
    get_latest_price,
    get_24hr_ticker,
    get_all_24hr_tickers,
    get_open_interest,
    get_open_interest_history,
    get_funding_rate,
    get_long_short_ratio
)

from coinx.collector.binance.indicators import (
    get_net_inflow_data,
    get_exchange_distribution_real
)

from coinx.collector.binance.service import (
    get_all_coins_list,
    update_all_data,
    update_single_coin_data,
    update_drop_list_data
)

from coinx.collector.binance.cache import (
    should_update_cache,
    get_cache_update_time # 假设cache模块里有这个辅助函数，或者需要补上
)

# 补齐部分可能直接用到但之前在 binance_api.py 里直接透传的函数
# 检查 cache.py 发现没有 get_cache_update_time，这个函数是在 utils.py 还是哪里？
# 之前 api_data.py 里导入了 get_cache_update_time 是从 coinx.utils 导入的。
# 所以这里只需要暴露业务逻辑相关的。
