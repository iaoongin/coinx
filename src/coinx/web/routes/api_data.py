import threading
import time
import re
from datetime import datetime

from flask import Blueprint, jsonify, request

from coinx.coin_manager import get_active_coins
from coinx.collector import (
    refresh_market_tickers,
    repair_rolling_tracked_symbols,
    repair_latest_tracked_symbols,
    repair_tracked_symbols,
)
from coinx.collector.exchange_repair import resolve_repair_worker_count
from coinx.repositories.market_tickers import get_market_tickers, get_latest_close_time
from coinx.repositories.contract_detail import RANGE_HOURS, get_contract_detail, get_contract_structure_score, load_contract_chart_series
from coinx.config import (
    ENABLED_EXCHANGES,
    HOMEPAGE_SERIES_REPAIR_ENABLED,
    REPAIR_HISTORY_COVERAGE_HOURS,
    REPAIR_HISTORY_ENABLED,
    REPAIR_HISTORY_INTERVAL,
    REPAIR_HISTORY_SYMBOL_BATCH_SIZE,
    REPAIR_ROLLING_POINTS,
    COLLECTION_SCHEDULER_ONLY,
    SCHEDULER_ENABLED,
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
from coinx.repositories.job_runs import get_job_run_count, get_job_runs, get_latest_job_runtime_metadata
from coinx.repositories.market_structure_score import (
    get_market_structure_score_snapshot,
    get_market_structure_score_symbols,
)
from coinx.repositories.trade_opportunities import get_trade_opportunity_snapshot
from coinx.scheduler import (
    get_all_job_runtime_metadata,
    scheduler,
)
from coinx.utils import logger
from coinx.web.time_params import request_as_of_ms


api_data_bp = Blueprint('api_data', __name__)
HOME_PAGE_REFRESH_LOCK = threading.Lock()
MARKET_STRUCTURE_REFRESH_LOCK = threading.Lock()
HOMEPAGE_SNAPSHOT_CACHE_LOCK = threading.Lock()
HOMEPAGE_SNAPSHOT_CACHE = {}
HOME_PAGE_LAST_REFRESH_SUMMARY = None
MARKET_STRUCTURE_LAST_REFRESH_SUMMARY = None
MANUAL_TASK_JOB_LOCK = threading.Lock()
MANUAL_TASK_JOB_LOCKS = {}

MARKET_STRUCTURE_MARKET_SERIES_TYPES = {
    'klines',
    'open_interest_hist',
    'taker_buy_sell_vol',
}

TASK_JOB_ACTIONS = {'run', 'pause', 'resume'}
CONTRACT_SYMBOL_PATTERN = re.compile(r'[\w-]{2,50}', flags=re.UNICODE)
TASK_JOB_LABELS = {
    'market_rank_refresh_job': '行情榜快照刷新',
    'rss_monitor_job': 'RSS 订阅监控',
    'repair_market_rolling_job': '市场滚动补齐',
    'repair_market_history_job': '低频历史补齐',
    'update_coins_config_job': '币种配置刷新',
    'cleanup_task_run_history_job': '任务运行记录清理',
}


def _default_exchange_repair_workers(exchanges=None):
    return resolve_repair_worker_count(exchanges or ENABLED_EXCHANGES)


def _collection_scheduler_only_response(operation):
    return jsonify(
        {
            'status': 'error',
            'code': 'COLLECTION_SCHEDULER_ONLY',
            'message': f'{operation} is disabled: collection is scheduler-only',
        }
    ), 409


def _format_scheduler_job(job, runtime=None):
    runtime = runtime or {}
    scheduler_running = bool(scheduler.running)
    next_run_time = getattr(job, 'next_run_time', None)
    paused = scheduler_running and next_run_time is None
    display_name = TASK_JOB_LABELS.get(job.id, job.name)
    return {
        'id': job.id,
        'name': job.name,
        'display_name': display_name,
        'trigger': str(job.trigger),
        'executor': getattr(job, 'executor', None),
        'max_instances': getattr(job, 'max_instances', None),
        'coalesce': getattr(job, 'coalesce', None),
        'misfire_grace_time': getattr(job, 'misfire_grace_time', None),
        'next_run_time_ms': int(next_run_time.timestamp() * 1000) if next_run_time else None,
        'registered': True,
        'paused': paused,
        'scheduler_running': scheduler_running,
        'runtime': runtime,
    }


def _list_scheduler_jobs():
    scheduler_jobs = scheduler.get_jobs()
    job_ids = [job.id for job in scheduler_jobs]
    try:
        persisted_runtime = get_latest_job_runtime_metadata(job_ids)
    except Exception:
        logger.exception('读取持久化任务运行记录失败')
        persisted_runtime = {}
    memory_runtime = get_all_job_runtime_metadata()
    return [
        _format_scheduler_job(job, {**persisted_runtime.get(job.id, {}), **memory_runtime.get(job.id, {})})
        for job in scheduler_jobs
    ]


def _start_manual_task_job(job):
    with MANUAL_TASK_JOB_LOCK:
        run_lock = MANUAL_TASK_JOB_LOCKS.setdefault(job.id, threading.Lock())
    if not run_lock.acquire(blocking=False):
        return False

    def run_job():
        try:
            job.func(*getattr(job, 'args', ()), **getattr(job, 'kwargs', {}))
        except Exception:
            logger.exception('手动执行任务失败: job_id=%s', job.id)
        finally:
            run_lock.release()

    threading.Thread(target=run_job, daemon=True, name=f'coinx-manual-{job.id}').start()
    return True


def _log_market_structure_refresh_component(component_name, summary):
    if not summary:
        logger.info('市场结构评分补齐组件跳过: component=%s', component_name)
        return

    component_stats = _summarize_market_structure_refresh_results(summary)

    logger.info(
        '市场结构评分补齐组件完成: component=%s mode=%s success=%s failure=%s skipped=%s symbols=%s series_types=%s affected=%s records=%s no_data=%s latest_event_time=%s',
        component_name,
        summary.get('mode') or 'history',
        summary.get('success_count', 0),
        summary.get('failure_count', 0),
        summary.get('skipped_count', 0),
        len(summary.get('symbols') or []),
        summary.get('series_types') or [],
        component_stats['affected'],
        component_stats['records'],
        component_stats['no_data_count'],
        component_stats['latest_event_time'],
    )


def _summarize_market_structure_refresh_results(summary):
    results = (summary or {}).get('results') or []
    latest_event_time = None
    no_data_count = 0
    affected = 0
    records = 0

    for item in results:
        affected += item.get('affected') or 0
        records += item.get('records') or 0
        if item.get('reason') == 'no_data':
            no_data_count += 1
        item_latest_event_time = item.get('latest_event_time')
        if item_latest_event_time is not None:
            latest_event_time = max(latest_event_time or item_latest_event_time, item_latest_event_time)

    return {
        'affected': affected,
        'records': records,
        'no_data_count': no_data_count,
        'latest_event_time': latest_event_time,
    }


def _run_homepage_refresh(symbols, series_types, latest_only=False):
    global HOME_PAGE_LAST_REFRESH_SUMMARY
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
        points = 2 if latest_only else REPAIR_ROLLING_POINTS
        summary = repair_rolling_tracked_symbols(
            symbols=symbols,
            series_types=series_types,
            points=points,
            max_workers=_default_exchange_repair_workers(),
        )
        HOME_PAGE_LAST_REFRESH_SUMMARY = summary

        return summary
    finally:
        HOME_PAGE_REFRESH_LOCK.release()


def _is_complete_homepage_payload(coins_data):
    if not coins_data:
        return False

    for coin in coins_data:
        # ``status=partial`` may only mean an optional exchange does not list
        # the symbol.  The homepage is complete when its required aggregated
        # series are present; exchange coverage remains visible in each coin's
        # ``missing_exchanges`` and ``exchange_statuses`` fields.
        if coin.get('status') == 'empty':
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


def _start_market_structure_refresh_async(symbols, series_types=None, exchanges=None):
    if not symbols:
        return False

    refresh_lock = MARKET_STRUCTURE_REFRESH_LOCK
    refresh_thread = threading.Thread(
        target=_run_market_structure_refresh,
        kwargs={
            'symbols': symbols,
            'series_types': series_types or list(MARKET_STRUCTURE_MARKET_SERIES_TYPES),
            'exchanges': exchanges or list(ENABLED_EXCHANGES),
            '_refresh_lock': refresh_lock,
        },
    )
    refresh_thread.daemon = True
    refresh_thread.start()
    return True


def _wait_for_refresh_completion(refresh_lock, summary_getter, message, poll_interval=0.2, timeout_seconds=120):
    deadline = time.time() + timeout_seconds
    while refresh_lock.locked() and time.time() < deadline:
        time.sleep(poll_interval)

    summary = summary_getter()
    if summary is not None:
        return summary

    return {
        'status': 'success' if not refresh_lock.locked() else 'timeout',
        'message': message if not refresh_lock.locked() else f'{message} (timeout)',
        'results': [],
        'success_count': 0,
        'failure_count': 0,
        'skipped_count': 0,
    }


def _run_market_structure_refresh(symbols, series_types, exchanges=None, _refresh_lock=None):
    global MARKET_STRUCTURE_LAST_REFRESH_SUMMARY
    # Capture the lock object for this run. Test harnesses and process reloads
    # may replace the module global while a daemon refresh is still finishing;
    # releasing the current global in ``finally`` can then unlock another run.
    refresh_lock = _refresh_lock or MARKET_STRUCTURE_REFRESH_LOCK
    if not refresh_lock.acquire(blocking=False):
        logger.info('市场结构评分补齐正在执行，跳过重复触发')
        return {
            'status': 'skipped',
            'message': 'market structure score refresh already running',
            'symbols': symbols,
            'series_types': series_types,
            'exchanges': exchanges,
            'success_count': 0,
            'failure_count': 0,
            'skipped_count': 0,
            'results': [],
        }

    try:
        normalized_series_types = _normalize_series_types(series_types)
        market_series_types = [
            series_type
            for series_type in normalized_series_types
            if series_type in MARKET_STRUCTURE_MARKET_SERIES_TYPES
        ]

        market_summary = None

        logger.info(
            '开始执行市场结构评分补齐: symbols=%s exchanges=%s market_series=%s',
            len(symbols or []),
            exchanges or [],
            market_series_types,
        )

        if market_series_types:
            market_started_at = time.perf_counter()
            market_summary = repair_rolling_tracked_symbols(
                symbols=symbols,
                series_types=market_series_types,
                exchanges=exchanges,
            )
            logger.info(
                '市场结构评分行情序列补齐耗时=%.2fs',
                time.perf_counter() - market_started_at,
            )
            _log_market_structure_refresh_component('market_series', market_summary)

        component_results = []
        if market_summary:
            market_stats = _summarize_market_structure_refresh_results(market_summary)
            component_results.append(
                {
                    'component': 'market_series',
                    'mode': market_summary.get('mode') or 'history',
                    'summary': market_summary,
                    'stats': market_stats,
                }
            )

        success_count = market_summary.get('success_count', 0) if market_summary else 0
        failure_count = market_summary.get('failure_count', 0) if market_summary else 0
        skipped_count = market_summary.get('skipped_count', 0) if market_summary else 0
        merged_results = []
        if market_summary:
            merged_results.extend(market_summary.get('results') or [])
        total_stats = _summarize_market_structure_refresh_results({'results': merged_results})

        if failure_count == 0:
            status = 'success'
        elif success_count > 0 or skipped_count > 0:
            status = 'partial_success'
        else:
            status = 'error'

        logger.info(
            '市场结构评分补齐完成: status=%s success=%s failure=%s skipped=%s affected=%s records=%s no_data=%s latest_event_time=%s components=%s',
            status,
            success_count,
            failure_count,
            skipped_count,
            total_stats['affected'],
            total_stats['records'],
            total_stats['no_data_count'],
            total_stats['latest_event_time'],
            [item['component'] for item in component_results],
        )

        summary = {
            'status': status,
            'message': 'market structure score refresh completed',
            'symbols': symbols,
            'series_types': normalized_series_types,
            'exchanges': exchanges,
            'success_count': success_count,
            'failure_count': failure_count,
            'skipped_count': skipped_count,
            'components': component_results,
            'stats': total_stats,
            'results': merged_results,
        }
        MARKET_STRUCTURE_LAST_REFRESH_SUMMARY = summary
        return summary
    finally:
        refresh_lock.release()


def _get_homepage_cache_anchor(as_of_ms=None):
    anchor_source = int(as_of_ms) if as_of_ms is not None else int(time.time() * 1000)
    return latest_closed_5m_open_time(anchor_source)


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
        included_exchanges = coin.get('included_exchanges')
        if included_exchanges is None:
            included_exchanges = coin.get('source_exchanges', [])

        formatted_coin = {
            'symbol': coin['symbol'],
            'source_exchanges': included_exchanges,
            'included_exchanges': included_exchanges,
            'missing_exchanges': coin.get('missing_exchanges', []),
            'status': coin.get('status', 'complete' if included_exchanges else 'empty'),
            'exchange_open_interest': coin.get('exchange_open_interest', []),
            'exchange_statuses': coin.get('exchange_statuses', []),
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
            'net_inflow_value': coin.get('net_inflow_value', {}),
            'net_inflow_value_formatted': coin.get('net_inflow_value_formatted', {}),
            'funding_rate': coin.get('funding_rate'),
            'funding_rate_formatted': coin.get('funding_rate_formatted'),
            'predicted_rate': coin.get('predicted_rate'),
            'predicted_rate_formatted': coin.get('predicted_rate_formatted'),
            'next_funding_time': coin.get('next_funding_time'),
            'next_funding_time_formatted': coin.get('next_funding_time_formatted'),
            'latest_time': coin.get('latest_time'),
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


def _normalize_series_types(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [item for item in value if item]
    if isinstance(value, str):
        return [item.strip() for item in value.split(',') if item.strip()]
    return [item for item in value if item]


@api_data_bp.route('/api/market-structure-score')
def get_market_structure_score():
    logger.info('开始加载合约市场结构评分')
    try:
        symbol = request.args.get('symbol')
        limit = request.args.get('limit', 100)

        if symbol:
            symbols = [symbol.strip().upper()]
        else:
            symbols = get_market_structure_score_symbols()
            try:
                symbols = symbols[:max(1, min(int(limit), 200))]
            except Exception:
                symbols = symbols[:100]

        as_of_ms = request_as_of_ms()
        snapshot_kwargs = {'symbols': symbols}
        if as_of_ms is not None:
            snapshot_kwargs['now_ms'] = as_of_ms
        snapshot = get_market_structure_score_snapshot(**snapshot_kwargs)
        return jsonify(
            {
                'status': 'success',
                'message': 'market structure score loaded',
                'data': snapshot.get('data') or [],
                'cache_update_time': snapshot.get('cache_update_time'),
                'summary': snapshot.get('summary') or {},
            }
        )
    except ValueError as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400
    except Exception as e:
        logger.error(f'加载合约市场结构评分失败: {e}')
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'failed to load market structure score: {str(e)}'}), 500


@api_data_bp.route('/api/trade-opportunities')
def get_trade_opportunities():
    try:
        scope = request.args.get('scope', 'candidates')
        limit = max(1, min(int(request.args.get('limit', 100)), 200))
        as_of_ms = request_as_of_ms()
        snapshot_kwargs = {'symbols': get_market_structure_score_symbols()[:limit]}
        if as_of_ms is not None:
            snapshot_kwargs['now_ms'] = as_of_ms
        snapshot = get_trade_opportunity_snapshot(**snapshot_kwargs)
        data = snapshot.get('data') or []
        if scope != 'all':
            candidates = {'可做多', '可做空', '等待回踩', '等待反弹'}
            data = [
                item for item in data
                if item.get('entry_state') in candidates and item.get('trend_state') != '震荡'
            ]
        return jsonify({'status': 'success', 'data': data, 'cache_update_time': snapshot.get('cache_update_time'), 'summary': snapshot.get('summary') or {}})
    except ValueError as exc:
        return jsonify({'status': 'error', 'message': str(exc)}), 400
    except Exception as exc:
        logger.exception('加载交易机会失败')
        return jsonify({'status': 'error', 'message': str(exc)}), 500


@api_data_bp.route('/api/market-structure-score/refresh', methods=['POST'])
def refresh_market_structure_score():
    logger.info('开始触发合约市场结构评分滚动修补')
    if COLLECTION_SCHEDULER_ONLY:
        return _collection_scheduler_only_response('market structure refresh')
    try:
        payload = request.get_json(silent=True) or {}
        force = request.args.get('force', 'false').lower() == 'true' or bool(payload.get('force', False))
        wait_for_completion = request.args.get('wait', 'false').lower() == 'true' or bool(payload.get('wait', False))
        try:
            symbols = get_market_structure_score_symbols()
        except Exception as e:
            logger.error(f'加载评分补齐所需的评分币种失败: {e}')
            symbols = []

        refresh_kwargs = {
            'symbols': symbols,
            'series_types': list(MARKET_STRUCTURE_MARKET_SERIES_TYPES),
            'exchanges': list(ENABLED_EXCHANGES),
        }

        if MARKET_STRUCTURE_REFRESH_LOCK.locked():
            summary = _wait_for_refresh_completion(
                MARKET_STRUCTURE_REFRESH_LOCK,
                lambda: MARKET_STRUCTURE_LAST_REFRESH_SUMMARY,
                'market structure score refresh reused existing rolling repair',
            )
            return jsonify({'status': 'success', 'message': summary.get('message'), 'data': summary})

        if wait_for_completion or force:
            summary = _run_market_structure_refresh(**refresh_kwargs)
            return jsonify(
                {
                    'status': 'success',
                    'message': 'market structure score refresh completed',
                    'data': summary,
                }
            )

        _start_market_structure_refresh_async(**refresh_kwargs)
        return jsonify({'status': 'success', 'message': 'market structure score rolling repair triggered'})
    except Exception as e:
        logger.error(f'触发合约市场结构评分滚动修补失败: {e}')
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'failed to trigger market structure refresh: {str(e)}'}), 500


@api_data_bp.route('/api/coins')
def get_coins():
    request_start = time.perf_counter()
    logger.info('开始从历史序列加载首页数据')
    try:
        active_coins = get_active_coins()
        as_of_ms = request_as_of_ms()
        cache_anchor = (
            _get_homepage_cache_anchor(as_of_ms)
            if as_of_ms is not None
            else _get_homepage_cache_anchor()
        )
        cache_key = _get_homepage_cache_key(active_coins, cache_anchor)

        # 检查是否强制跳过缓存
        force_refresh = request.args.get('nocache', '').lower() == '1'
        if not force_refresh:
            cached_payload = _get_cached_homepage_payload(cache_key)
            if cached_payload is not None:
                elapsed_ms = (time.perf_counter() - request_start) * 1000
                logger.info(f'首页数据命中缓存: 币种数={len(active_coins)}, 锚点={cache_anchor}, 耗时={elapsed_ms:.2f}ms')
                return jsonify(cached_payload)
        else:
            logger.info('强制跳过缓存')

        snapshot_start = time.perf_counter()
        snapshot_kwargs = {'symbols': active_coins}
        if as_of_ms is not None:
            snapshot_kwargs['now_ms'] = as_of_ms
        snapshot = get_homepage_series_snapshot(**snapshot_kwargs)
        snapshot_ms = (time.perf_counter() - snapshot_start) * 1000

        if active_coins and not _is_complete_homepage_payload(snapshot.get('data') or []):
            logger.info('首页历史序列不完整，跳过后台补全，返回现有数据')
            # Historical replay is read-only; live auto-repair is also disabled
            # when collection is restricted to scheduler jobs.
            if as_of_ms is None and HOMEPAGE_SERIES_REPAIR_ENABLED and not COLLECTION_SCHEDULER_ONLY:
                try:
                    homepage_refresh_started = False
                    if should_refresh_homepage_series(active_coins):
                        homepage_refresh_started = bool(_start_homepage_refresh_async(
                            active_coins, series_types=list(HOMEPAGE_REQUIRED_SERIES_TYPES)
                        ))
                    if homepage_refresh_started:
                        score_symbols = get_market_structure_score_symbols()
                        score_only_symbols = [
                            symbol for symbol in score_symbols if symbol not in set(active_coins)
                        ]
                        if score_only_symbols:
                            _start_market_structure_refresh_async(
                                score_only_symbols,
                                series_types=list(MARKET_STRUCTURE_MARKET_SERIES_TYPES),
                            )
                except Exception:
                    logger.exception('首页不完整时触发后台补全失败')
            elif as_of_ms is None and HOMEPAGE_SERIES_REPAIR_ENABLED and COLLECTION_SCHEDULER_ONLY:
                logger.info('采集仅允许由调度任务触发，跳过首页请求补采')

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
    except ValueError as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400
    except Exception as e:
        logger.error(f'加载首页数据失败: {e}')
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'failed to load homepage data: {str(e)}'}), 500


@api_data_bp.route('/api/update')
def update_data():
    logger.info('开始触发首页滚动修补')
    if COLLECTION_SCHEDULER_ONLY:
        return _collection_scheduler_only_response('homepage refresh')
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
                return jsonify({'status': 'success', 'message': 'homepage rolling repair triggered'})

        if HOME_PAGE_REFRESH_LOCK.locked():
            summary = _wait_for_refresh_completion(
                HOME_PAGE_REFRESH_LOCK,
                lambda: HOME_PAGE_LAST_REFRESH_SUMMARY,
                'homepage rolling repair reused existing run',
            )
            _clear_homepage_snapshot_cache()
            return jsonify({'status': 'success', 'message': summary.get('message'), 'data': summary})

        refresh_kwargs = {
            'symbols': symbols,
            'series_types': list(HOMEPAGE_REQUIRED_SERIES_TYPES),
            'latest_only': not force,
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

        _start_homepage_refresh_async(**refresh_kwargs)
        _clear_homepage_snapshot_cache()

        return jsonify({'status': 'success', 'message': 'homepage rolling repair triggered'})
    except Exception as e:
        logger.error(f'触发首页滚动修补失败: {e}')
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'failed to trigger homepage refresh: {str(e)}'}), 500


@api_data_bp.route('/api/coin-detail/<symbol>')
def get_coin_detail(symbol):
    normalized_symbol = symbol.strip().upper()
    if not CONTRACT_SYMBOL_PATTERN.fullmatch(normalized_symbol):
        return jsonify({'status': 'error', 'message': 'invalid contract symbol'}), 400

    logger.info('开始加载合约详情: %s', normalized_symbol)
    try:
        as_of_ms = request_as_of_ms()
        detail_kwargs = {'symbol': normalized_symbol}
        if as_of_ms is not None:
            detail_kwargs['now_ms'] = as_of_ms
        detail_data = get_contract_detail(**detail_kwargs)
        if detail_data is None:
            return jsonify({'status': 'error', 'message': 'contract detail not found'}), 404
        return jsonify({'status': 'success', 'message': 'coin detail loaded', 'data': detail_data})
    except ValueError as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400
    except Exception as e:
        logger.error(f'加载币种详情失败: {symbol}, 错误: {e}')
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'failed to load coin detail: {str(e)}'}), 500


@api_data_bp.route('/api/coin-detail/<symbol>/series')
def get_coin_detail_series(symbol):
    normalized_symbol = symbol.strip().upper()
    range_key = request.args.get('range', '24h')
    if not CONTRACT_SYMBOL_PATTERN.fullmatch(normalized_symbol):
        return jsonify({'status': 'error', 'message': 'invalid contract symbol'}), 400
    if range_key not in RANGE_HOURS:
        return jsonify({'status': 'error', 'message': 'invalid range'}), 400
    try:
        as_of_ms = request_as_of_ms()
        chart_kwargs = {'range_key': range_key}
        if as_of_ms is not None:
            chart_kwargs['as_of_ms'] = as_of_ms
        data = load_contract_chart_series(normalized_symbol, **chart_kwargs)
        return jsonify({'status': 'success', 'message': 'contract series loaded', 'data': data})
    except ValueError as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400
    except Exception as e:
        logger.error('加载合约趋势失败: %s, 错误: %s', normalized_symbol, e)
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'failed to load contract series: {str(e)}'}), 500


@api_data_bp.route('/api/coin-detail/<symbol>/structure-score')
def get_coin_detail_structure_score(symbol):
    normalized_symbol = symbol.strip().upper()
    if not CONTRACT_SYMBOL_PATTERN.fullmatch(normalized_symbol):
        return jsonify({'status': 'error', 'message': 'invalid contract symbol'}), 400
    try:
        as_of_ms = request_as_of_ms()
        score_kwargs = {'symbol': normalized_symbol}
        if as_of_ms is not None:
            score_kwargs['now_ms'] = as_of_ms
        data = get_contract_structure_score(**score_kwargs)
        return jsonify({'status': 'success', 'message': 'contract structure score loaded', 'data': data})
    except ValueError as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400
    except Exception as e:
        logger.error('加载合约结构评分失败: %s, 错误: %s', normalized_symbol, e)
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'failed to load contract structure score: {str(e)}'}), 500


@api_data_bp.route('/api/coin-detail/<symbol>/trade-opportunity')
def get_coin_detail_trade_opportunity(symbol):
    normalized_symbol = symbol.strip().upper()
    if not CONTRACT_SYMBOL_PATTERN.fullmatch(normalized_symbol):
        return jsonify({'status': 'error', 'message': 'invalid contract symbol'}), 400
    try:
        as_of_ms = request_as_of_ms()
        snapshot_kwargs = {'symbols': [normalized_symbol]}
        if as_of_ms is not None:
            snapshot_kwargs['now_ms'] = as_of_ms
        snapshot = get_trade_opportunity_snapshot(**snapshot_kwargs)
        opportunity = next(
            (item for item in snapshot.get('data') or [] if item.get('symbol') == normalized_symbol),
            None,
        )
        return jsonify({
            'status': 'success',
            'message': 'contract trade opportunity loaded',
            'data': {
                'symbol': normalized_symbol,
                'as_of': snapshot.get('cache_update_time'),
                'opportunity': opportunity,
            },
        })
    except ValueError as exc:
        return jsonify({'status': 'error', 'message': str(exc)}), 400
    except Exception as exc:
        logger.exception('加载合约交易机会失败: %s', normalized_symbol)
        return jsonify({'status': 'error', 'message': f'failed to load contract trade opportunity: {str(exc)}'}), 500


@api_data_bp.route('/api/market-rank')
def get_market_rank():
    """获取行情排行数据"""
    logger.info('开始加载行情榜数据')
    try:
        rank_type = request.args.get('type', 'price_change')
        direction = request.args.get('direction', 'down')
        limit = int(request.args.get('limit', 100))
        as_of_ms = request_as_of_ms()
        
        ticker_kwargs = {'rank_type': rank_type, 'direction': direction, 'limit': limit}
        if as_of_ms is not None:
            ticker_kwargs['as_of_ms'] = as_of_ms
        data = get_market_tickers(**ticker_kwargs)
        
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
        
        # The ranked rows already carry the snapshot close_time. Reusing it
        # avoids a second ClickHouse round trip on every market-rank request;
        # retain the database lookup for empty/legacy row objects.
        close_time = getattr(data[0], 'close_time', None) if data else None
        if close_time is None:
            close_time = (
                get_latest_close_time(as_of_ms=as_of_ms)
                if as_of_ms is not None
                else get_latest_close_time()
            )
        
        return jsonify({
            'status': 'success',
            'message': 'market rank data loaded',
            'data': formatted_data,
            'snapshot_time': close_time,
        })
    except ValueError as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400
    except Exception as e:
        logger.error(f'加载行情榜数据失败: {e}')
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'failed to load market rank data: {str(e)}'}), 500


@api_data_bp.route('/api/market-rank/refresh', methods=['POST'])
def refresh_market_rank():
    """手动触发行情榜快照刷新"""
    logger.info('开始触发行情榜快照刷新')
    if COLLECTION_SCHEDULER_ONLY:
        return _collection_scheduler_only_response('market rank refresh')
    try:
        summary = refresh_market_tickers()
        if summary.get('status') != 'success':
            status = 409 if summary.get('status') == 'skipped' else 500
            message = summary.get('message', 'market rank refresh failed')
            if summary.get('status') == 'error':
                message = f'failed to refresh market rank: {message}'
            return (
                jsonify(
                    {
                        'status': summary.get('status', 'error'),
                        'message': message,
                        'data': summary,
                    }
                ),
                status,
            )

        return jsonify(
            {
                'status': 'success',
                'message': 'market rank snapshot refreshed',
                'data': summary,
            }
        )
    except Exception as e:
        logger.error(f'触发行情榜快照刷新失败: {e}')
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'failed to refresh market rank: {str(e)}'}), 500


@api_data_bp.route('/api/task-jobs')
def list_task_jobs():
    try:
        jobs = _list_scheduler_jobs()
        return jsonify(
            {
                'status': 'success',
                'message': '任务列表加载成功',
                'data': {
                    'scheduler_enabled': SCHEDULER_ENABLED,
                    'collection_scheduler_only': COLLECTION_SCHEDULER_ONLY,
                    'scheduler_running': bool(scheduler.running),
                    'jobs': jobs,
                },
            }
        )
    except Exception as e:
        logger.error(f'加载任务列表失败: {e}')
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'加载任务列表失败：{str(e)}'}), 500


@api_data_bp.route('/api/task-jobs/<job_id>/runs')
def list_task_job_runs(job_id):
    job = scheduler.get_job(job_id)
    if job is None:
        return jsonify({'status': 'error', 'message': f'未找到任务：{job_id}'}), 404
    try:
        limit = int(request.args.get('limit', 5))
        offset = int(request.args.get('offset', 0))
    except (TypeError, ValueError):
        return jsonify({'status': 'error', 'message': 'limit 和 offset 必须是整数'}), 400
    if limit < 1 or limit > 50:
        return jsonify({'status': 'error', 'message': 'limit 必须在 1 到 50 之间'}), 400
    if offset < 0:
        return jsonify({'status': 'error', 'message': 'offset 不能为负数'}), 400
    try:
        total = get_job_run_count(job_id)
        return jsonify(
            {
                'status': 'success',
                'message': '任务执行记录加载成功',
                'data': {
                    'job_id': job_id,
                    'runs': get_job_runs(job_id, limit=limit, offset=offset),
                    'total': total,
                    'limit': limit,
                    'offset': offset,
                },
            }
        )
    except Exception as e:
        logger.error('加载任务运行记录失败: job_id=%s error=%s', job_id, e)
        logger.exception(e)
        return jsonify({'status': 'error', 'message': f'加载任务执行记录失败：{str(e)}'}), 500


@api_data_bp.route('/api/task-jobs/<job_id>/action', methods=['POST'])
def control_task_job(job_id):
    payload = request.get_json(silent=True) or {}
    action = (payload.get('action') or '').strip().lower()
    if action not in TASK_JOB_ACTIONS:
        return jsonify({'status': 'error', 'message': f'不支持的操作：{action}'}), 400
    job = scheduler.get_job(job_id)
    if job is None:
        return jsonify({'status': 'error', 'message': f'未找到任务：{job_id}'}), 404
    if not SCHEDULER_ENABLED and action in {'pause', 'resume'}:
        return jsonify({'status': 'error', 'message': '调度器已由 SCHEDULER_ENABLED=false 禁用，不能暂停或恢复任务'}), 409
    try:
        if action == 'run':
            if SCHEDULER_ENABLED:
                scheduler.modify_job(job_id, next_run_time=datetime.now())
                scheduler.wakeup()
                message = f'任务已触发执行: {job_id}'
            elif _start_manual_task_job(job):
                message = f'任务已在后台手动执行: {job_id}'
            else:
                return jsonify({'status': 'error', 'message': f'任务已在运行：{job_id}'}), 409
        elif action == 'pause':
            job.pause()
            message = f'任务已暂停: {job_id}'
        elif action == 'resume':
            job.resume()
            message = f'任务已恢复: {job_id}'

        return jsonify(
            {
                'status': 'success',
                'message': message,
                'data': {
                    'job_id': job_id,
                    'action': action,
                    'jobs': _list_scheduler_jobs(),
                },
            }
        )
    except Exception as e:
        logger.error(f'执行任务操作失败: job_id={job_id} action={action} error={e}')
        logger.exception(e)
        action_labels = {'run': '执行', 'pause': '暂停', 'resume': '恢复'}
        action_label = action_labels.get(action, action)
        return jsonify({'status': 'error', 'message': f'{action_label}任务失败：{str(e)}'}), 500
