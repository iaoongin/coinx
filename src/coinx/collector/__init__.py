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
    repair_tracked_symbols as repair_binance_tracked_symbols,
    run_series_repair_job as run_binance_series_repair_job,
)
from coinx.config import (
    ENABLED_EXCHANGES,
    HOMEPAGE_SERIES_REPAIR_PAGE_LIMIT,
    HOMEPAGE_SERIES_REPAIR_PERIOD,
    HOMEPAGE_SERIES_TYPES,
)


def repair_tracked_symbols(symbols=None, series_types=None, now_ms=None, http_session=None, db_session=None):
    """修补已启用交易所的首页序列数据。

    Binance 继续使用现有的缺口修补逻辑；OKX 当前采集最新首页分页并写入带交易所维度的通用表。
    """
    summary = repair_binance_tracked_symbols(
        symbols=symbols,
        series_types=series_types,
        now_ms=now_ms,
        http_session=http_session,
        db_session=db_session,
    )

    if 'okx' not in [exchange.lower() for exchange in ENABLED_EXCHANGES]:
        return summary

    from coinx.collector.okx.series import collect_series_batch

    okx_series_types = [
        series_type
        for series_type in (series_types or HOMEPAGE_SERIES_TYPES)
        if series_type in ('klines', 'open_interest_hist', 'taker_buy_sell_vol')
    ]
    target_symbols = symbols or summary.get('symbols') or []
    okx_summary = collect_series_batch(
        symbols=target_symbols,
        periods=[HOMEPAGE_SERIES_REPAIR_PERIOD],
        series_types=okx_series_types,
        limit=HOMEPAGE_SERIES_REPAIR_PAGE_LIMIT,
        http_session=http_session,
        db_session=db_session,
    )
    summary['exchange_summaries'] = {
        'binance': dict(summary),
        'okx': okx_summary,
    }
    summary['success_count'] = summary.get('success_count', 0) + okx_summary.get('success_count', 0)
    summary['failure_count'] = summary.get('failure_count', 0) + okx_summary.get('failure_count', 0)
    return summary


def run_series_repair_job():
    return run_binance_series_repair_job()
