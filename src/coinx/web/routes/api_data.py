import threading
import time

from flask import Blueprint, jsonify, request

from coinx.coin_manager import get_active_coins
from coinx.collector import (
    collect_and_store_series,
    collect_series_batch,
    get_24hr_ticker,
    get_exchange_distribution_real,
    get_funding_rate,
    get_latest_price,
    get_long_short_ratio,
    get_open_interest,
    repair_latest_tracked_symbols,
    repair_tracked_symbols,
    update_market_tickers,
)
from coinx.repositories.market_tickers import get_market_tickers, get_latest_close_time
from coinx.config import (
    BINANCE_SERIES_LIMIT,
    BINANCE_SERIES_PERIODS,
    BINANCE_SERIES_REPAIR_BOOTSTRAP_DAYS,
    BINANCE_SERIES_REPAIR_ENABLED,
    BINANCE_SERIES_REPAIR_FUTURES_PAGE_LIMIT,
    BINANCE_SERIES_REPAIR_INTERVAL,
    BINANCE_SERIES_REPAIR_KLINES_PAGE_LIMIT,
    BINANCE_SERIES_REPAIR_PERIOD,
    BINANCE_SERIES_REPAIR_SLEEP_MS,
    BINANCE_SERIES_TYPES,
    REPAIR_HISTORY_COVERAGE_HOURS,
    REPAIR_HISTORY_ENABLED,
    REPAIR_HISTORY_INTERVAL,
    REPAIR_HISTORY_MAX_WORKERS,
    REPAIR_HISTORY_SYMBOL_BATCH_SIZE,
    REPAIR_ROLLING_MAX_WORKERS,
    REPAIR_ROLLING_POINTS,
    TIME_INTERVALS,
)
from coinx.repositories.homepage_series import (
    HOMEPAGE_REQUIRED_SERIES_TYPES,
    get_homepage_series_data,
    get_homepage_series_snapshot,
    get_homepage_series_update_time,
    latest_closed_5m_open_time,
    should_refresh_homepage_series,
)
from coinx.utils import logger


api_data_bp = Blueprint('api_data', __name__)
HOME_PAGE_REFRESH_LOCK = threading.Lock()
HOMEPAGE_SNAPSHOT_CACHE_LOCK = threading.Lock()
HOMEPAGE_SNAPSHOT_CACHE = {}

SUPPORTED_SERIES_TYPES = {
    'top_long_short_position_ratio',
    'top_long_short_account_ratio',
    'open_interest_hist',
    'klines',
    'global_long_short_account_ratio',
}


def _run_homepage_refresh(symbols, series_types, latest_only=False):
    if not HOME_PAGE_REFRESH_LOCK.acquire(blocking=False):
        logger.info('首页历史序列补全正在执行，跳过重复触发')
        return {
            'status': 'skipped',
            'message': 'homepage series refresh already running',
            'symbols': symbols,
            'series_types': series_types,
            'success_count': 0,
            'failure_count': 0,
            'skipped_count': 0,
            'results': [],
        }

    try:
        repair_func = repair_latest_tracked_symbols if latest_only else repair_tracked_symbols
        return repair_func(symbols=symbols, series_types=series_types)
    finally:
        HOME_PAGE_REFRESH_LOCK.release()


def _is_complete_homepage_payload(coins_data):
    if not coins_data:
        return False

    coin = coins_data[0]
    changes = coin.get('changes') or {}
    if isinstance(changes, list):
        changes = {item.get('interval'): item for item in changes if item.get('interval')}

    for field in (
        'current_open_interest_formatted',
        'current_open_interest_value_formatted',
        'current_price_formatted',
    ):
        if not coin.get(field) or coin.get(field) == 'N/A':
            return False

    for interval in TIME_INTERVALS:
        change = changes.get(interval)
        if not change:
            return False
        if change.get('current_price_formatted') == 'N/A':
            return False
        if change.get('open_interest_formatted') == 'N/A':
            return False
        if change.get('open_interest_value_formatted') == 'N/A':
            return False

    return True


def _start_homepage_refresh_async(symbols, series_types=None, latest_only=False):
    if not symbols:
        return False

    refresh_thread = threading.Thread(
        target=_run_homepage_refresh,
        kwargs={
            'symbols': symbols,
            'series_types': series_types or list(HOMEPAGE_REQUIRED_SERIES_TYPES),
            'latest_only': latest_only,
        },
    )
    refresh_thread.daemon = True
    refresh_thread.start()
    return True


def _get_homepage_cache_anchor():
    return latest_closed_5m_open_time(int(time.time() * 1000))


def _get_homepage_cache_key(symbols, anchor_time):
    # 测试中会 monkeypatch 仓储函数，把函数 id 放入 key 可避免跨测试串缓存。
    return (tuple(symbols or []), anchor_time, id(get_homepage_series_snapshot))


def _get_cached_homepage_payload(cache_key):
    with HOMEPAGE_SNAPSHOT_CACHE_LOCK:
        return HOMEPAGE_SNAPSHOT_CACHE.get(cache_key)


def _set_cached_homepage_payload(cache_key, payload):
    with HOMEPAGE_SNAPSHOT_CACHE_LOCK:
        HOMEPAGE_SNAPSHOT_CACHE.clear()
        HOMEPAGE_SNAPSHOT_CACHE[cache_key] = payload


def _clear_homepage_snapshot_cache():
    with HOMEPAGE_SNAPSHOT_CACHE_LOCK:
        HOMEPAGE_SNAPSHOT_CACHE.clear()


def _format_homepage_coins_payload(coins_data):
    formatted_data = []
    for coin in coins_data:
        formatted_coin = {
            'symbol': coin['symbol'],
            'source_exchanges': coin.get('source_exchanges', []),
            'missing_exchanges': coin.get('missing_exchanges', []),
            'exchange_open_interest': coin.get('exchange_open_interest', []),
            'current_open_interest': coin['current_open_interest'],
            'current_open_interest_formatted': coin['current_open_interest_formatted'],
            'current_open_interest_value': coin['current_open_interest_value'],
            'current_open_interest_value_formatted': coin['current_open_interest_value_formatted'],
            'current_price': coin['current_price'],
            'current_price_formatted': coin['current_price_formatted'],
            'price_change': coin['price_change'],
            'price_change_percent': coin['price_change_percent'],
            'price_change_formatted': coin['price_change_formatted'],
            'net_inflow': coin.get('net_inflow', {}),
        }

        changes = []
        for interval, data in (coin.get('changes') or {}).items():
            changes.append(
                {
                    'interval': interval,
                    'ratio': data['ratio'],
                    'value_ratio': data['value_ratio'],
                    'open_interest': data['open_interest'],
                    'open_interest_formatted': data['open_interest_formatted'],
                    'open_interest_value': data['open_interest_value'],
                    'open_interest_value_formatted': data['open_interest_value_formatted'],
                    'price_change': data['price_change'],
                    'price_change_percent': data['price_change_percent'],
                    'price_change_formatted': data['price_change_formatted'],
                    'current_price': data['current_price'],
                    'current_price_formatted': data['current_price_formatted'],
                }
            )

        changes.sort(
            key=lambda x: (
                x['interval'].endswith('m') and int(x['interval'][:-1])
                or x['interval'].endswith('h') and int(x['interval'][:-1]) * 60
                or x['interval'].endswith('d') and int(x['interval'][:-1]) * 1440
                or 0
            )
        )
        formatted_coin['changes'] = changes
        formatted_data.append(formatted_coin)

    return formatted_data


def _validate_series_types(series_types):
    if series_types is None:
        return None
    if not isinstance(series_types, list):
        return 'series_types must be a list'

    invalid_types = [series_type for series_type in series_types if series_type not in SUPPORTED_SERIES_TYPES]
    if invalid_types:
        return f'unsupported series_type values: {invalid_types}'

    return None


@api_data_bp.route('/api/binance-series/config')
def get_binance_series_config():
    return jsonify(
        {
            'status': 'success',
            'message': 'binance series config loaded',
            'data': {
                'collect': {
                    'limit': BINANCE_SERIES_LIMIT,
                    'series_types': BINANCE_SERIES_TYPES,
                    'periods': BINANCE_SERIES_PERIODS,
                },
                'repair': {
                    'enabled': BINANCE_SERIES_REPAIR_ENABLED,
                    'interval': BINANCE_SERIES_REPAIR_INTERVAL,
                    'period': BINANCE_SERIES_REPAIR_PERIOD,
                    'bootstrap_days': BINANCE_SERIES_REPAIR_BOOTSTRAP_DAYS,
                    'klines_page_limit': BINANCE_SERIES_REPAIR_KLINES_PAGE_LIMIT,
                    'futures_page_limit': BINANCE_SERIES_REPAIR_FUTURES_PAGE_LIMIT,
                    'sleep_ms': BINANCE_SERIES_REPAIR_SLEEP_MS,
                    'rolling_points': REPAIR_ROLLING_POINTS,
                    'rolling_max_workers': REPAIR_ROLLING_MAX_WORKERS,
                    'history_enabled': REPAIR_HISTORY_ENABLED,
                    'history_interval': REPAIR_HISTORY_INTERVAL,
                    'history_max_workers': REPAIR_HISTORY_MAX_WORKERS,
                    'history_symbol_batch_size': REPAIR_HISTORY_SYMBOL_BATCH_SIZE,
                    'history_coverage_hours': REPAIR_HISTORY_COVERAGE_HOURS,
                },
            },
        }
    )


@api_data_bp.route('/api/coins')
def get_coins():
    request_start = time.perf_counter()
    logger.info('开始从历史序列加载首页数据')
    try:
        active_coins = get_active_coins()
        cache_anchor = _get_homepage_cache_anchor()
        cache_key = _get_homepage_cache_key(active_coins, cache_anchor)
        cached_payload = _get_cached_homepage_payload(cache_key)
        if cached_payload is not None:
            elapsed_ms = (time.perf_counter() - request_start) * 1000
            logger.info(f'首页数据命中缓存: 币种数={len(active_coins)}, 锚点={cache_anchor}, 耗时={elapsed_ms:.2f}ms')
            return jsonify(cached_payload)

        snapshot_start = time.perf_counter()
        snapshot = get_homepage_series_snapshot(active_coins)
        snapshot_ms = (time.perf_counter() - snapshot_start) * 1000

        if active_coins and not _is_complete_homepage_payload(snapshot.get('data') or []):
            logger.info('首页历史序列不完整，开始后台轻量补全最新点')
            _start_homepage_refresh_async(
                active_coins,
                series_types=list(HOMEPAGE_REQUIRED_SERIES_TYPES),
                latest_only=True,
            )

        formatted_data = _format_homepage_coins_payload(snapshot['data'])
        payload = {
            'status': 'success',
            'message': 'homepage data loaded',
            'data': formatted_data,
            'cache_update_time': snapshot['cache_update_time'],
            'homepage_complete': _is_complete_homepage_payload(snapshot.get('data') or []),
        }
        _set_cached_homepage_payload(cache_key, payload)

        elapsed_ms = (time.perf_counter() - request_start) * 1000
        logger.info(
            f'首页数据加载完成: 币种数={len(active_coins)}, 数据行={len(formatted_data)}, '
            f'锚点={cache_anchor}, 聚合耗时={snapshot_ms:.2f}ms, 总耗时={elapsed_ms:.2f}ms'
        )
        return jsonify(payload)
    except Exception as e:
        logger.error(f'加载首页数据失败: {e}')
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'failed to load homepage data: {str(e)}'}), 500


@api_data_bp.route('/api/update')
def update_data():
    logger.info('开始触发首页历史序列刷新')
    try:
        force = request.args.get('force', 'false').lower() == 'true'
        wait_for_completion = request.args.get('wait', 'false').lower() == 'true'
        try:
            symbols = get_active_coins()
        except Exception as e:
            logger.error(f'加载首页刷新所需的跟踪币种失败: {e}')
            symbols = []

        if not force:
            if not should_refresh_homepage_series(symbols):
                logger.info('首页历史序列已是最新，无需刷新')
                return jsonify({'status': 'success', 'message': 'homepage series already up to date'})

            if not wait_for_completion:
                _start_homepage_refresh_async(symbols, series_types=list(HOMEPAGE_REQUIRED_SERIES_TYPES))
                return jsonify({'status': 'success', 'message': 'homepage series refresh triggered'})

        refresh_kwargs = {
            'symbols': symbols,
            'series_types': list(HOMEPAGE_REQUIRED_SERIES_TYPES),
        }

        if wait_for_completion:
            summary = _run_homepage_refresh(**refresh_kwargs)
            _clear_homepage_snapshot_cache()
            return jsonify(
                {
                    'status': 'success',
                    'message': 'homepage series refresh completed',
                    'data': summary,
                }
            )

        update_thread = threading.Thread(target=_run_homepage_refresh, kwargs=refresh_kwargs)
        update_thread.daemon = True
        update_thread.start()
        _clear_homepage_snapshot_cache()

        return jsonify({'status': 'success', 'message': 'homepage series refresh triggered'})
    except Exception as e:
        logger.error(f'触发首页历史序列刷新失败: {e}')
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'failed to trigger homepage refresh: {str(e)}'}), 500


@api_data_bp.route('/api/coin-detail/<symbol>')
def get_coin_detail(symbol):
    logger.info(f'开始加载币种详情: {symbol}')
    try:
        detail_data = {
            'symbol': symbol,
            'latest_price': get_latest_price(symbol),
            'funding_rate': get_funding_rate(symbol),
            'ticker_data': get_24hr_ticker(symbol),
            'open_interest_data': get_open_interest(symbol),
            'long_short_ratio': get_long_short_ratio(symbol),
            'exchange_distribution': get_exchange_distribution_real(symbol),
        }
        return jsonify({'status': 'success', 'message': 'coin detail loaded', 'data': detail_data})
    except Exception as e:
        logger.error(f'加载币种详情失败: {symbol}, 错误: {e}')
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'failed to load coin detail: {str(e)}'}), 500


@api_data_bp.route('/api/market-rank')
def get_market_rank():
    """获取行情排行数据"""
    logger.info('开始加载行情榜数据')
    try:
        rank_type = request.args.get('type', 'price_change')
        direction = request.args.get('direction', 'down')
        limit = int(request.args.get('limit', 100))
        
        data = get_market_tickers(rank_type=rank_type, direction=direction, limit=limit)
        
        formatted_data = []
        for idx, item in enumerate(data, 1):
            formatted_data.append({
                'symbol': item.symbol,
                'rank_index': idx,
                'price': float(item.last_price) if item.last_price else None,
                'price_change_percent': float(item.price_change_percent) if item.price_change_percent else None,
                'volume': float(item.volume) if item.volume else None,
                'quote_volume': float(item.quote_volume) if item.quote_volume else None,
            })
        
        close_time = get_latest_close_time()
        
        return jsonify({
            'status': 'success',
            'message': 'market rank data loaded',
            'data': formatted_data,
            'snapshot_time': close_time,
        })
    except Exception as e:
        logger.error(f'加载行情榜数据失败: {e}')
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'failed to load market rank data: {str(e)}'}), 500


@api_data_bp.route('/api/binance-series/collect', methods=['POST'])
def collect_binance_series():
    payload = request.get_json(silent=True) or {}
    series_type = payload.get('series_type')
    symbol = payload.get('symbol')
    period = payload.get('period')
    limit = payload.get('limit')

    if not series_type or not symbol or not period or limit is None:
        return jsonify(
            {
                'status': 'error',
                'message': 'missing required fields: series_type, symbol, period, limit',
            }
        ), 400

    if series_type not in SUPPORTED_SERIES_TYPES:
        return jsonify({'status': 'error', 'message': f'unsupported series_type: {series_type}'}), 400

    try:
        result = collect_and_store_series(
            series_type=series_type,
            symbol=symbol,
            period=period,
            limit=int(limit),
        )
        return jsonify({'status': 'success', 'message': 'series collected', 'data': result})
    except Exception as e:
        logger.error(f'采集历史序列失败: {e}')
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'failed to collect series: {str(e)}'}), 500


@api_data_bp.route('/api/binance-series/batch-collect', methods=['POST'])
def batch_collect_binance_series():
    payload = request.get_json(silent=True) or {}
    symbols = payload.get('symbols')
    periods = payload.get('periods')
    series_types = payload.get('series_types')
    limit = payload.get('limit')

    if not symbols or not periods or limit is None:
        return jsonify(
            {
                'status': 'error',
                'message': 'missing required fields: symbols, periods, limit',
            }
        ), 400

    if not isinstance(symbols, list) or not isinstance(periods, list):
        return jsonify({'status': 'error', 'message': 'symbols and periods must be lists'}), 400

    error = _validate_series_types(series_types)
    if error:
        return jsonify({'status': 'error', 'message': error}), 400

    try:
        result = collect_series_batch(
            symbols=symbols,
            periods=periods,
            series_types=series_types,
            limit=int(limit),
        )
        return jsonify({'status': 'success', 'message': 'batch series collected', 'data': result})
    except Exception as e:
        logger.error(f'批量采集历史序列失败: {e}')
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'failed to batch collect series: {str(e)}'}), 500


@api_data_bp.route('/api/binance-series/repair-tracked', methods=['POST'])
def repair_tracked_binance_series():
    payload = request.get_json(silent=True) or {}
    symbols = payload.get('symbols')
    exchanges = payload.get('exchanges')
    series_types = payload.get('series_types')
    mode = payload.get('mode', 'history')
    full_scan = bool(payload.get('full_scan', False))
    coverage_hours = payload.get('coverage_hours')
    max_workers = payload.get('max_workers')

    error = _validate_series_types(series_types)
    if error:
        return jsonify({'status': 'error', 'message': error}), 400
    if symbols is not None and not isinstance(symbols, list):
        return jsonify({'status': 'error', 'message': 'symbols must be a list'}), 400
    if exchanges is not None and not isinstance(exchanges, list):
        return jsonify({'status': 'error', 'message': 'exchanges must be a list'}), 400
    if mode not in ('history', 'legacy'):
        return jsonify({'status': 'error', 'message': 'mode must be history or legacy'}), 400

    try:
        if symbols is None and coverage_hours is None and max_workers is None and not full_scan:
            repair_kwargs = {'series_types': series_types}
            if exchanges is not None:
                repair_kwargs['exchanges'] = exchanges
            result = repair_tracked_symbols(**repair_kwargs)
        else:
            result = repair_tracked_symbols(
                symbols=symbols,
                series_types=series_types,
                exchanges=exchanges,
                coverage_hours=int(coverage_hours) if coverage_hours is not None else None,
                max_workers=int(max_workers) if max_workers is not None else 1,
                symbol_batch_size=None if full_scan else REPAIR_HISTORY_SYMBOL_BATCH_SIZE,
            )
        return jsonify({'status': 'success', 'message': 'tracked series repaired', 'data': result})
    except Exception as e:
        logger.error(f'修补已跟踪币种历史序列失败: {e}')
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'failed to repair tracked series: {str(e)}'}), 500
