"""统一暴露采集层接口。"""

from coinx.collector.binance.client import get_session, request_with_retry
from coinx.collector.binance.market import (
    get_futures_kline_latest,
    aggregate_futures_kline,
    get_latest_price,
    get_24hr_ticker,
    get_all_24hr_tickers,
    get_open_interest,
    get_open_interest_history,
    get_funding_rate,
    get_long_short_ratio,
    get_exchange_info,
)
from coinx.collector.binance.indicators import (
    get_exchange_distribution_real,
)
from coinx.collector.binance.service import (
    get_all_coins_list,
    update_all_data,
    update_single_coin_data,
    update_market_tickers,
)
from coinx.collector.binance.cache import should_update_cache, get_cache_update_time
from coinx.collector.binance.series import (
    fetch_top_long_short_position_ratio,
    fetch_top_long_short_account_ratio,
    fetch_open_interest_hist,
    fetch_klines,
    fetch_global_long_short_account_ratio,
    fetch_taker_buy_sell_vol,
    parse_top_long_short_position_ratio,
    parse_top_long_short_account_ratio,
    parse_open_interest_hist,
    parse_klines,
    parse_global_long_short_account_ratio,
    parse_taker_buy_sell_vol,
    fetch_series_payload,
    parse_series_payload,
    collect_and_store_series,
    collect_series_batch,
)
from coinx.collector.binance.repair import (
    floor_to_completed_5m,
    build_repair_window,
    repair_single_series,
    repair_tracked_symbols,
    run_series_repair_job,
)
