"""Unified collector-layer public API."""

from coinx.collector.binance.client import get_session, request_with_retry
from coinx.collector.binance.market import (
    aggregate_futures_kline,
    get_24hr_ticker,
    get_all_24hr_tickers,
    get_exchange_info,
    get_funding_rate,
    get_futures_kline_latest,
    get_latest_price,
    get_long_short_ratio,
    get_open_interest,
    get_open_interest_history,
)
from coinx.collector.binance.indicators import get_exchange_distribution_real
from coinx.collector.binance.service import (
    get_all_coins_list,
    update_all_data,
    update_market_tickers,
    update_single_coin_data,
)
from coinx.collector.binance.cache import get_cache_update_time, should_update_cache
from coinx.collector.binance.series import (
    collect_and_store_series,
    collect_series_batch,
    fetch_global_long_short_account_ratio,
    fetch_klines,
    fetch_open_interest_hist,
    fetch_series_payload,
    fetch_taker_buy_sell_vol,
    fetch_top_long_short_account_ratio,
    fetch_top_long_short_position_ratio,
    parse_global_long_short_account_ratio,
    parse_klines,
    parse_open_interest_hist,
    parse_series_payload,
    parse_taker_buy_sell_vol,
    parse_top_long_short_account_ratio,
    parse_top_long_short_position_ratio,
)
from coinx.collector.binance.repair import (
    build_repair_window,
    floor_to_completed_5m,
    repair_single_series,
    run_series_repair_job as run_binance_series_repair_job,
)
from coinx.collector.exchange_repair import (
    repair_history_symbols,
    repair_rolling_symbols,
)


def repair_tracked_symbols(
    symbols=None,
    series_types=None,
    now_ms=None,
    http_session=None,
    db_session=None,
    max_workers=1,
    coverage_hours=None,
    symbol_batch_size=None,
    exchanges=None,
):
    """Repair homepage market series for enabled exchanges."""
    return repair_history_symbols(
        symbols=symbols,
        series_types=series_types,
        exchanges=exchanges,
        now_ms=now_ms,
        full_scan=symbol_batch_size is None,
        max_workers=max_workers,
        coverage_hours=coverage_hours,
        http_session=http_session,
        db_session=db_session,
    )


def repair_latest_tracked_symbols(symbols=None, series_types=None, now_ms=None, http_session=None, db_session=None):
    """Compatibility wrapper for a small rolling repair."""
    return repair_rolling_symbols(
        symbols=symbols,
        series_types=series_types,
        now_ms=now_ms,
        points=2,
        http_session=http_session,
        db_session=db_session,
    )


def repair_rolling_tracked_symbols(
    symbols=None,
    series_types=None,
    now_ms=None,
    points=None,
    max_workers=None,
    http_session=None,
    db_session=None,
    exchanges=None,
):
    """Repair a rolling 5m window for enabled exchanges."""
    return repair_rolling_symbols(
        symbols=symbols,
        series_types=series_types,
        exchanges=exchanges,
        now_ms=now_ms,
        points=points,
        max_workers=max_workers,
        http_session=http_session,
        db_session=db_session,
    )


def run_history_repair_job(symbols=None, series_types=None, full_scan=False, exchanges=None):
    return repair_history_symbols(
        symbols=symbols,
        series_types=series_types,
        exchanges=exchanges,
        full_scan=full_scan,
    )


def run_series_repair_job():
    return run_binance_series_repair_job()
