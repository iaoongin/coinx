from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
import time

from sqlalchemy import func, or_

from coinx.coin_manager import get_active_coins
from coinx.collector.binance.market import get_all_funding_rates as get_all_binance_funding_rates
from coinx.collector.bybit.series import get_all_funding_rates as get_all_bybit_funding_rates
from coinx.collector.binance.repair import latest_closed_5m_open_time
from coinx.collector.exchange_adapters import get_exchange_adapter, get_supported_exchange_ids
from coinx.collector.okx.series import get_all_funding_rates as get_all_okx_funding_rates
from coinx.config import ENABLED_EXCHANGES, FETCH_COINS_TOP_VOLUME_COUNT
from coinx.database import get_session
from coinx.models import (
    BinanceGlobalLongShortAccountRatio,
    BinanceTopLongShortAccountRatio,
    BinanceTopLongShortPositionRatio,
)
from coinx.repositories.market_tickers import get_market_ticker_symbols
from coinx.repositories.market_structure_series import load_market_structure_exchange_maps
from coinx.utils import logger


EMA_FAST_PERIOD = 20
EMA_SLOW_PERIOD = 60
ATR_PERIOD = 14
SCORE_HISTORY_POINTS = 120
VOLUME_RATIO_LOOKBACK_POINTS = 288

TREND_THRESHOLD = 10
MOMENTUM_THRESHOLD = 10
VOLUME_BOOST_THRESHOLD = 1.2
FUNDING_HOT_THRESHOLD = 0.0008
FUNDING_COLD_THRESHOLD = -0.0008
PRICE_MOVE_RISK_THRESHOLD = 0.02
ATR_RISK_THRESHOLD = 0.02
MARKET_STRUCTURE_EXCHANGE_MAX_WORKERS = 4
EXCHANGE_FUNDING_LOADERS = {
    'binance': get_all_binance_funding_rates,
    'okx': get_all_okx_funding_rates,
    'bybit': get_all_bybit_funding_rates,
}
BINANCE_CONTEXT_SERIES_LABELS = {
    'top_long_short_position_ratio': '大户持仓比',
    'top_long_short_account_ratio': '大户账户比',
    'global_long_short_account_ratio': '全市场账户比',
}
EXCHANGE_DIAGNOSTIC_REASON_LABELS = {
    'included': '已纳入',
    'missing_oi': '缺少 OI',
    'missing_kline': '缺少 K 线',
    'missing_oi_kline': '缺少 OI/K 线',
    'no_common_anchor': '无共同时间点',
    'anchor_missing_oi': '锚点缺少 OI',
    'anchor_missing_kline': '锚点缺少 K 线',
    'anchor_missing_oi_kline': '锚点缺少 OI/K 线',
    'metric_unavailable': '指标不可用',
}


@dataclass(frozen=True)
class SeriesPoint:
    time: int
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    close_price: Optional[float] = None
    quote_volume: Optional[float] = None
    taker_buy_quote_volume: Optional[float] = None
    open_interest: Optional[float] = None
    open_interest_value: Optional[float] = None


def _normalize_exchange_list(exchanges):
    normalized = []
    seen = set()
    for exchange in exchanges or []:
        key = (exchange or '').strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        normalized.append(key)
    return normalized


def get_market_structure_score_symbols(session=None, top_volume_limit=FETCH_COINS_TOP_VOLUME_COUNT):
    tracked_symbols = get_active_coins()
    own_session = session is None
    db = session or get_session()

    try:
        top_volume_symbols = get_market_ticker_symbols(rank_type='quote_volume', limit=top_volume_limit, session=db)
        return list(dict.fromkeys([*tracked_symbols, *top_volume_symbols]))
    finally:
        if own_session:
            db.close()


def _safe_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _point_float(point, *field_names):
    if point is None:
        return None
    for field_name in field_names:
        if hasattr(point, field_name):
            value = _safe_float(getattr(point, field_name, None))
            if value is not None:
                return value
    return None


def _series_point_from_kline(row, time_field='open_time'):
    point_time = int(getattr(row, time_field))
    return SeriesPoint(
        time=point_time,
        open_price=_safe_float(getattr(row, 'open_price', None)),
        high_price=_safe_float(getattr(row, 'high_price', None)),
        low_price=_safe_float(getattr(row, 'low_price', None)),
        close_price=_safe_float(getattr(row, 'close_price', None)),
        quote_volume=_safe_float(getattr(row, 'quote_volume', None)),
        taker_buy_quote_volume=_safe_float(getattr(row, 'taker_buy_quote_volume', None)),
    )


def _series_point_from_open_interest(row, time_field='event_time'):
    point_time = int(getattr(row, time_field))
    return SeriesPoint(
        time=point_time,
        open_interest=_safe_float(getattr(row, 'sum_open_interest', None)),
        open_interest_value=_safe_float(getattr(row, 'sum_open_interest_value', None)),
    )


def _estimate_open_interest_value(open_interest, reference_price, open_interest_value=None):
    if open_interest_value is not None:
        return _safe_float(open_interest_value)
    if open_interest is None or reference_price in (None, 0):
        return None
    return float(open_interest) * float(reference_price)


def _get_latest_time(records_by_time, upper_bound=None):
    if not records_by_time:
        return None
    available_times = [timestamp for timestamp in records_by_time if upper_bound is None or timestamp <= upper_bound]
    if not available_times:
        return None
    return max(available_times)


def _get_time_series(records_by_time, anchor_time, lookback_points, step_ms=5 * 60 * 1000):
    times = []
    current_time = int(anchor_time)
    for offset in range(max(1, lookback_points)):
        timestamp = current_time - offset * step_ms
        if timestamp in records_by_time:
            times.append(timestamp)
    return [records_by_time[timestamp] for timestamp in sorted(times)]


def _ema(values, period):
    if not values:
        return None

    multiplier = 2 / (period + 1)
    ema = float(values[0])
    for value in values[1:]:
        ema = ((float(value) - ema) * multiplier) + ema
    return ema


def _true_range(current_high, current_low, previous_close):
    if current_high is None or current_low is None:
        return None
    if previous_close is None:
        return float(current_high) - float(current_low)
    return max(
        float(current_high) - float(current_low),
        abs(float(current_high) - float(previous_close)),
        abs(float(current_low) - float(previous_close)),
    )


def _atr(points, period=ATR_PERIOD):
    if len(points) < 2:
        return None

    tr_values = []
    previous_close = points[0].close_price
    for point in points[1:]:
        tr = _true_range(point.high_price, point.low_price, previous_close)
        if tr is not None:
            tr_values.append(tr)
        previous_close = point.close_price

    if not tr_values:
        return None

    recent = tr_values[-period:]
    return sum(recent) / len(recent)


def _calc_trend_score(current_price, ema_fast, ema_slow):
    if current_price is None or ema_fast is None or ema_slow is None:
        return 0, '震荡'

    if current_price > ema_fast and ema_fast > ema_slow:
        return 30, '多头趋势'
    if current_price < ema_fast and ema_fast < ema_slow:
        return -30, '空头趋势'
    return 0, '震荡'


def _calc_momentum_score(taker_net_pressure_ratio, volume_ratio):
    if taker_net_pressure_ratio is None or volume_ratio is None:
        return 0, '弱'

    if taker_net_pressure_ratio > 0.10 and volume_ratio >= VOLUME_BOOST_THRESHOLD:
        return 25, '多'
    if taker_net_pressure_ratio < -0.10 and volume_ratio >= VOLUME_BOOST_THRESHOLD:
        return -25, '空'
    return 0, '弱'


def _calc_position_score(current_price, previous_price, current_open_interest_value, previous_open_interest_value):
    if current_price is None or previous_price is None:
        return 0, '换手'
    if current_open_interest_value is None or previous_open_interest_value is None:
        return 0, '换手'

    price_change_ratio = 0 if previous_price in (None, 0) else (current_price - previous_price) / previous_price
    oi_change = current_open_interest_value - previous_open_interest_value

    if price_change_ratio > 0 and oi_change > 0:
        return 25, '多头开仓推动'
    if price_change_ratio > 0 and oi_change < 0:
        return 10, '空头回补推动'
    if price_change_ratio < 0 and oi_change > 0:
        return -25, '空头开仓推动'
    if price_change_ratio < 0 and oi_change < 0:
        return -10, '多头平仓/止损'
    if oi_change > 0:
        return 10, '蓄势增仓'
    if oi_change < 0:
        return -10, '降温减仓'
    return 0, '换手'


def _calc_sentiment_score(top_ratio_delta, global_ratio_delta):
    if top_ratio_delta is None or global_ratio_delta is None:
        return 0, '中性'

    if top_ratio_delta > 0 and global_ratio_delta < 0:
        return 10, '大户偏多，散户偏空/离场'
    if top_ratio_delta < 0 and global_ratio_delta > 0:
        return -10, '大户偏空，散户偏多/接盘'
    if top_ratio_delta > 0 and global_ratio_delta > 0:
        return 5, '多头共识增强'
    if top_ratio_delta < 0 and global_ratio_delta < 0:
        return -5, '空头共识增强'
    return 0, '中性'


def _calc_risk_score(direction_hint, funding_rate, price_move_ratio, atr_ratio):
    score = 0
    reasons = []

    if direction_hint == 'long' and funding_rate is not None and funding_rate >= FUNDING_HOT_THRESHOLD:
        score -= 10
        reasons.append('资金费率过热')
    if direction_hint == 'short' and funding_rate is not None and funding_rate <= FUNDING_COLD_THRESHOLD:
        score -= 10
        reasons.append('资金费率过冷')
    if price_move_ratio is not None and abs(price_move_ratio) >= PRICE_MOVE_RISK_THRESHOLD:
        score -= 10
        reasons.append('短周期波动偏大')
    if atr_ratio is not None and atr_ratio >= ATR_RISK_THRESHOLD:
        score -= 10
        reasons.append('ATR 过大')

    if score < -30:
        score = -30

    if score >= -10:
        level = '低'
    elif score >= -20:
        level = '中'
    else:
        level = '高'

    return score, level, reasons


def _build_metric_weights(exchange_metrics):
    total_open_interest_value = sum(float(metric.get('open_interest_value') or 0) for metric in exchange_metrics)
    if total_open_interest_value <= 0:
        if not exchange_metrics:
            return {}
        equal_weight = 1 / len(exchange_metrics)
        return {metric['exchange']: equal_weight for metric in exchange_metrics}
    return {
        metric['exchange']: float(metric.get('open_interest_value') or 0) / total_open_interest_value
        for metric in exchange_metrics
    }


def _weighted_metric_value(exchange_metrics, field_name, weight_field='weight'):
    weighted_values = []
    total_weight = 0.0
    for metric in exchange_metrics:
        value = _safe_float(metric.get(field_name))
        weight = _safe_float(metric.get(weight_field))
        if value is None or weight in (None, 0):
            continue
        weighted_values.append(value * weight)
        total_weight += weight

    if total_weight <= 0:
        values = [_safe_float(metric.get(field_name)) for metric in exchange_metrics]
        values = [value for value in values if value is not None]
        if not values:
            return None
        return sum(values) / len(values)

    return sum(weighted_values) / total_weight


def _load_exchange_funding_rate_maps(exchanges, target_symbols):
    funding_maps = {}
    symbol_set = set(target_symbols or [])
    normalized_exchanges = [exchange for exchange in exchanges if exchange]

    def _load_single_exchange(exchange):
        loader = EXCHANGE_FUNDING_LOADERS.get((exchange or '').strip().lower())
        if loader is None:
            return exchange, {}

        started_at = time.perf_counter()
        raw_map = {}
        try:
            raw_map = loader() or {}
        except Exception as exc:
            logger.warning('批量加载交易所资金费率失败: exchange=%s error=%s', exchange, exc)
            raw_map = {}

        filtered_map = {}
        for symbol, payload in raw_map.items():
            if symbol_set and symbol not in symbol_set:
                continue
            if not isinstance(payload, dict):
                continue
            rate = _safe_float(payload.get('lastFundingRate'))
            if rate is None:
                rate = _safe_float(payload.get('fundingRate'))
            filtered_map[symbol] = rate

        logger.info(
            '评分资金费率加载完成: exchange=%s total=%d matched=%d 耗时=%.2fs',
            exchange,
            len(raw_map),
            len(filtered_map),
            time.perf_counter() - started_at,
        )
        return exchange, filtered_map

    max_workers = min(MARKET_STRUCTURE_EXCHANGE_MAX_WORKERS, len(normalized_exchanges) or 1)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_exchange = {
            executor.submit(_load_single_exchange, exchange): exchange
            for exchange in normalized_exchanges
        }
        for future in as_completed(future_to_exchange):
            exchange, filtered_map = future.result()
            funding_maps[exchange] = filtered_map

    return funding_maps


def _load_exchange_map_for_score(exchange, symbols, upper_bound=None):
    session = get_session()
    try:
        return load_market_structure_exchange_maps(session, exchange, symbols, upper_bound=upper_bound)
    finally:
        session.close()


def _score_signal(total_score):
    if total_score >= 60:
        return '强多', '只找回踩做多，不追高'
    if total_score >= 30:
        return '偏多', '轻仓做多，或等待回踩确认'
    if total_score > -30:
        return '震荡', '不做，或只做区间'
    if total_score > -60:
        return '偏空', '轻仓做空，或等待反抽确认'
    return '强空', '只找反抽做空，不追空'


def _load_binance_context(session, symbol, anchor_time):
    context_map = _load_binance_context_map(session, [symbol], anchor_time)
    return context_map.get(symbol, {})


def _log_binance_context_freshness(context_map, anchor_time):
    if not context_map:
        logger.info('评分 Binance 情绪数据为空: anchor_time=%s', anchor_time)
        return

    for series_type in (
        'top_long_short_position_ratio',
        'top_long_short_account_ratio',
        'global_long_short_account_ratio',
    ):
        matched = 0
        stale = 0
        missing = 0
        latest_event_time = None
        max_lag_bars = 0

        for symbol_context in context_map.values():
            current = ((symbol_context or {}).get(series_type) or {}).get('current') or {}
            event_time = current.get('event_time')
            if event_time is None:
                missing += 1
                continue

            matched += 1
            latest_event_time = max(latest_event_time or event_time, event_time)
            lag_bars = 0
            if anchor_time is not None and event_time <= anchor_time:
                lag_bars = max(0, int((anchor_time - event_time) // (5 * 60 * 1000)))
            max_lag_bars = max(max_lag_bars, lag_bars)
            if lag_bars > 0:
                stale += 1

        logger.info(
            '评分 Binance 情绪新鲜度: series_type=%s matched=%s missing=%s stale=%s latest_event_time=%s anchor_time=%s max_lag_bars=%s',
            series_type,
            matched,
            missing,
            stale,
            latest_event_time,
            anchor_time,
            max_lag_bars,
        )


def _summarize_binance_context_health(context_map, anchor_time, symbol_count):
    dimensions = []
    latest_event_time = None
    max_lag_bars = 0
    ready_symbols = 0
    available_symbols = 0

    for series_type, label in BINANCE_CONTEXT_SERIES_LABELS.items():
        matched = 0
        stale = 0
        missing = 0
        dimension_latest_event_time = None
        dimension_max_lag_bars = 0

        for symbol_context in context_map.values():
            current = ((symbol_context or {}).get(series_type) or {}).get('current') or {}
            event_time = current.get('event_time')
            if event_time is None:
                missing += 1
                continue

            matched += 1
            dimension_latest_event_time = max(dimension_latest_event_time or event_time, event_time)
            lag_bars = 0
            if anchor_time is not None and event_time <= anchor_time:
                lag_bars = max(0, int((anchor_time - event_time) // (5 * 60 * 1000)))
            dimension_max_lag_bars = max(dimension_max_lag_bars, lag_bars)
            if lag_bars > 0:
                stale += 1

        latest_event_time = max(latest_event_time or dimension_latest_event_time, dimension_latest_event_time or 0) if dimension_latest_event_time is not None else latest_event_time
        max_lag_bars = max(max_lag_bars, dimension_max_lag_bars)
        coverage_percent = 0.0 if symbol_count <= 0 else (matched / symbol_count) * 100
        dimensions.append(
            {
                'series_type': series_type,
                'label': label,
                'matched': matched,
                'missing': missing,
                'stale': stale,
                'coverage_percent': coverage_percent,
                'latest_event_time': dimension_latest_event_time,
                'max_lag_bars': dimension_max_lag_bars,
            }
        )

    for symbol_context in context_map.values():
        available = False
        ready = True
        for series_type in BINANCE_CONTEXT_SERIES_LABELS:
            current = ((symbol_context or {}).get(series_type) or {}).get('current') or {}
            event_time = current.get('event_time')
            if event_time is not None:
                available = True
            if event_time is None or (anchor_time is not None and event_time != anchor_time):
                ready = False
        if available:
            available_symbols += 1
        if ready:
            ready_symbols += 1

    worst_dimension = None
    if dimensions:
        worst_dimension = min(dimensions, key=lambda item: (item['coverage_percent'], item['matched'], item['label']))

    overall_coverage_percent = 0.0
    if dimensions:
        overall_coverage_percent = sum(item['coverage_percent'] for item in dimensions) / len(dimensions)

    return {
        'symbol_count': symbol_count,
        'ready_symbols': ready_symbols,
        'available_symbols': available_symbols,
        'overall_coverage_percent': overall_coverage_percent,
        'latest_event_time': latest_event_time,
        'max_lag_bars': max_lag_bars,
        'lag_minutes': max_lag_bars * 5,
        'worst_dimension': worst_dimension,
        'dimensions': dimensions,
    }


def _build_exchange_diagnostic(exchange, included, reason, detail=None):
    return {
        'exchange': exchange,
        'included': included,
        'reason': reason,
        'reason_label': EXCHANGE_DIAGNOSTIC_REASON_LABELS.get(reason, reason or '未知'),
        'detail': detail,
    }


def _build_symbol_exchange_diagnostics(exchange_maps, symbol, symbol_anchor_time=None, included_exchanges=None, enabled_exchanges=None):
    diagnostics = []
    included_exchange_set = set(_normalize_exchange_list(included_exchanges or []))
    target_exchanges = _normalize_exchange_list(enabled_exchanges or exchange_maps.keys())

    for exchange in target_exchanges:
        oi_map, kline_map, _, _ = exchange_maps.get(exchange, ({}, {}, {}, {}))
        symbol_oi = oi_map.get(symbol, {})
        symbol_kline = kline_map.get(symbol, {})
        latest_oi_time = max(symbol_oi) if symbol_oi else None
        latest_kline_time = max(symbol_kline) if symbol_kline else None
        common_times = sorted(set(symbol_oi).intersection(symbol_kline))
        detail = {
            'latest_oi_time': latest_oi_time,
            'latest_kline_time': latest_kline_time,
            'latest_common_time': common_times[-1] if common_times else None,
            'symbol_anchor_time': symbol_anchor_time,
        }

        if exchange in included_exchange_set:
            diagnostics.append(_build_exchange_diagnostic(exchange, True, 'included', detail=detail))
            continue

        if not symbol_oi and not symbol_kline:
            diagnostics.append(_build_exchange_diagnostic(exchange, False, 'missing_oi_kline', detail=detail))
            continue
        if not symbol_oi:
            diagnostics.append(_build_exchange_diagnostic(exchange, False, 'missing_oi', detail=detail))
            continue
        if not symbol_kline:
            diagnostics.append(_build_exchange_diagnostic(exchange, False, 'missing_kline', detail=detail))
            continue
        if not common_times:
            diagnostics.append(_build_exchange_diagnostic(exchange, False, 'no_common_anchor', detail=detail))
            continue

        if symbol_anchor_time is not None:
            missing_oi_at_anchor = symbol_anchor_time not in symbol_oi
            missing_kline_at_anchor = symbol_anchor_time not in symbol_kline
            if missing_oi_at_anchor and missing_kline_at_anchor:
                diagnostics.append(_build_exchange_diagnostic(exchange, False, 'anchor_missing_oi_kline', detail=detail))
                continue
            if missing_oi_at_anchor:
                diagnostics.append(_build_exchange_diagnostic(exchange, False, 'anchor_missing_oi', detail=detail))
                continue
            if missing_kline_at_anchor:
                diagnostics.append(_build_exchange_diagnostic(exchange, False, 'anchor_missing_kline', detail=detail))
                continue

        diagnostics.append(_build_exchange_diagnostic(exchange, False, 'metric_unavailable', detail=detail))

    return diagnostics


def _load_binance_context_map(session, symbols, anchor_time):
    context_map = {symbol: {} for symbol in symbols}
    models = {
        'top_long_short_position_ratio': BinanceTopLongShortPositionRatio,
        'top_long_short_account_ratio': BinanceTopLongShortAccountRatio,
        'global_long_short_account_ratio': BinanceGlobalLongShortAccountRatio,
    }

    if not symbols:
        return context_map

    for key, model in models.items():
        ranked_rows = (
            session.query(
                model.symbol.label('symbol'),
                model.event_time.label('event_time'),
                model.long_short_ratio.label('long_short_ratio'),
                model.long_account.label('long_account'),
                model.short_account.label('short_account'),
                func.row_number().over(
                    partition_by=model.symbol,
                    order_by=model.event_time.desc(),
                ).label('rn'),
            )
            .filter(model.symbol.in_(symbols), model.period == '5m')
        )
        if anchor_time is not None:
            ranked_rows = ranked_rows.filter(model.event_time <= anchor_time)

        subquery = ranked_rows.subquery()
        rows = (
            session.query(subquery)
            .filter(subquery.c.rn <= 2)
            .order_by(subquery.c.symbol.asc(), subquery.c.rn.asc())
            .all()
        )

        rows_by_symbol = {}
        for row in rows:
            rows_by_symbol.setdefault(row.symbol, []).append(row)

        for symbol in symbols:
            symbol_rows = rows_by_symbol.get(symbol, [])
            current = symbol_rows[0] if symbol_rows else None
            previous = symbol_rows[1] if len(symbol_rows) > 1 else None

            context_map[symbol][key] = {
                'current': {
                    'event_time': int(current.event_time) if current is not None else None,
                    'long_short_ratio': _safe_float(getattr(current, 'long_short_ratio', None)) if current is not None else None,
                    'long_account': _safe_float(getattr(current, 'long_account', None)) if current is not None else None,
                    'short_account': _safe_float(getattr(current, 'short_account', None)) if current is not None else None,
                },
                'previous': {
                    'event_time': int(previous.event_time) if previous is not None else None,
                    'long_short_ratio': _safe_float(getattr(previous, 'long_short_ratio', None)) if previous is not None else None,
                    'long_account': _safe_float(getattr(previous, 'long_account', None)) if previous is not None else None,
                    'short_account': _safe_float(getattr(previous, 'short_account', None)) if previous is not None else None,
                },
            }

    _log_binance_context_freshness(context_map, anchor_time)
    return context_map


def _build_exchange_metric(
    exchange,
    symbol,
    oi_by_time,
    kline_by_time,
    taker_maps_by_period,
    anchor_time,
    funding_rate=None,
    quote_volume_24h=None,
):
    common_times = sorted(set(oi_by_time).intersection(kline_by_time))
    if anchor_time is None:
        if not common_times:
            return None
        anchor_time = common_times[-1]
    elif anchor_time not in oi_by_time or anchor_time not in kline_by_time:
        return None

    current_kline = kline_by_time.get(anchor_time)
    current_oi = oi_by_time.get(anchor_time)
    previous_time = anchor_time - 5 * 60 * 1000
    previous_kline = kline_by_time.get(previous_time)
    previous_oi = oi_by_time.get(previous_time)

    if current_kline is None or current_oi is None:
        return None

    kline_points = _get_time_series(kline_by_time, anchor_time, SCORE_HISTORY_POINTS)
    closes = [point.close_price for point in kline_points if point.close_price is not None]
    ema_fast = _ema(closes[-EMA_FAST_PERIOD:], EMA_FAST_PERIOD) if len(closes) >= 2 else None
    ema_slow = _ema(closes[-EMA_SLOW_PERIOD:], EMA_SLOW_PERIOD) if len(closes) >= 2 else None
    atr_value = _atr(kline_points, period=ATR_PERIOD)

    current_price = _safe_float(current_kline.close_price)
    previous_price = _safe_float(previous_kline.close_price) if previous_kline is not None else None

    current_open_interest = _point_float(current_oi, 'open_interest', 'sum_open_interest')
    current_open_interest_value = _estimate_open_interest_value(
        current_open_interest,
        current_price,
        _point_float(current_oi, 'open_interest_value', 'sum_open_interest_value'),
    )
    previous_open_interest = _point_float(previous_oi, 'open_interest', 'sum_open_interest')
    previous_open_interest_value = _estimate_open_interest_value(
        previous_open_interest,
        _safe_float(previous_price),
        _point_float(previous_oi, 'open_interest_value', 'sum_open_interest_value') if previous_oi is not None else None,
    )

    quote_volume = _safe_float(current_kline.quote_volume)
    taker_period = '5m'
    if exchange:
        try:
            taker_period = get_exchange_adapter(exchange).taker_period_for_interval('5m') or '5m'
        except Exception:
            taker_period = '5m'
    taker_map = (taker_maps_by_period or {}).get(taker_period, {})
    taker_point = taker_map.get(anchor_time)
    if taker_point is None:
        taker_times = [timestamp for timestamp in taker_map if timestamp <= anchor_time]
        if taker_times:
            taker_point = taker_map[max(taker_times)]

    taker_buy_quote_volume = _safe_float(getattr(taker_point, 'buy_vol', None))
    if taker_buy_quote_volume is None:
        taker_buy_quote_volume = _safe_float(current_kline.taker_buy_quote_volume)
    taker_net_pressure = None
    taker_net_pressure_ratio = None
    if quote_volume not in (None, 0) and taker_buy_quote_volume is not None:
        taker_net_pressure = (2 * taker_buy_quote_volume) - quote_volume
        taker_net_pressure_ratio = taker_net_pressure / quote_volume

    volume_ratio = None
    if quote_volume is not None and quote_volume_24h not in (None, 0):
        avg_volume = quote_volume_24h / VOLUME_RATIO_LOOKBACK_POINTS
        if avg_volume:
            volume_ratio = quote_volume / avg_volume

    price_move_ratio = None
    if previous_price not in (None, 0) and current_price is not None:
        price_move_ratio = (current_price - previous_price) / previous_price

    open_interest_change_ratio = None
    if previous_open_interest_value not in (None, 0) and current_open_interest_value is not None:
        open_interest_change_ratio = abs(current_open_interest_value - previous_open_interest_value) / previous_open_interest_value

    trend_score, trend_direction = _calc_trend_score(current_price, ema_fast, ema_slow)
    momentum_score, momentum_direction = _calc_momentum_score(taker_net_pressure_ratio, volume_ratio)
    position_score, position_structure = _calc_position_score(
        current_price,
        previous_price,
        current_open_interest_value,
        previous_open_interest_value,
    )
    atr_ratio = None
    if atr_value is not None and current_price not in (None, 0):
        atr_ratio = atr_value / current_price

    score_direction_hint = 'neutral'
    if trend_direction == '多头趋势' or momentum_direction == '多' or position_score > 0:
        score_direction_hint = 'long'
    elif trend_direction == '空头趋势' or momentum_direction == '空' or position_score < 0:
        score_direction_hint = 'short'

    exchange_metric = {
        'exchange': exchange,
        'symbol': symbol,
        'current_time': anchor_time,
        'current_price': current_price,
        'previous_price': previous_price,
        'open_interest': current_open_interest,
        'previous_open_interest': previous_open_interest,
        'open_interest_value': current_open_interest_value,
        'previous_open_interest_value': previous_open_interest_value,
        'quote_volume': quote_volume,
        'quote_volume_24h': quote_volume_24h,
        'taker_buy_quote_volume': taker_buy_quote_volume,
        'taker_net_pressure': taker_net_pressure,
        'taker_net_pressure_ratio': taker_net_pressure_ratio,
        'volume_ratio': volume_ratio,
        'price_move_ratio': price_move_ratio,
        'open_interest_change_ratio': open_interest_change_ratio,
        'ema20': ema_fast,
        'ema60': ema_slow,
        'atr': atr_value,
        'atr_ratio': atr_ratio,
        'trend_score': trend_score,
        'trend_direction': trend_direction,
        'momentum_score': momentum_score,
        'momentum_direction': momentum_direction,
        'position_score': position_score,
        'position_structure': position_structure,
        'score_direction_hint': score_direction_hint,
        'taker_period': taker_period,
        'funding_rate': funding_rate,
    }
    return exchange_metric


def _aggregate_weighted_scores(exchange_metrics, sentiment_score, sentiment_state, risk_score, risk_level, risk_reasons):
    valid_metrics = [metric for metric in exchange_metrics if metric is not None]
    if not valid_metrics:
        return None

    weights = _build_metric_weights(valid_metrics)

    for metric in valid_metrics:
        metric_weight = weights.get(metric['exchange'], 0)
        metric['weight'] = metric_weight
        metric['weight_percent'] = metric_weight * 100
        metric['sentiment_score'] = sentiment_score
        metric['sentiment_state'] = sentiment_state
        metric['risk_score'] = risk_score
        metric['risk_level'] = risk_level
        metric['risk_reasons'] = list(risk_reasons)
        metric['total_score'] = (
            metric['trend_score']
            + metric['momentum_score']
            + metric['position_score']
            + sentiment_score
            + risk_score
        )
        metric['weighted_total_score'] = metric['total_score'] * metric_weight

    total_score = sum(metric['weighted_total_score'] for metric in valid_metrics)
    trend_score = sum(metric['trend_score'] * metric['weight'] for metric in valid_metrics)
    momentum_score = sum(metric['momentum_score'] * metric['weight'] for metric in valid_metrics)
    position_score = sum(metric['position_score'] * metric['weight'] for metric in valid_metrics)

    if trend_score > TREND_THRESHOLD:
        trend_direction = '多头趋势'
    elif trend_score < -TREND_THRESHOLD:
        trend_direction = '空头趋势'
    else:
        trend_direction = '震荡'

    if momentum_score > MOMENTUM_THRESHOLD:
        momentum_direction = '多'
    elif momentum_score < -MOMENTUM_THRESHOLD:
        momentum_direction = '空'
    else:
        momentum_direction = '弱'

    if position_score > MOMENTUM_THRESHOLD:
        position_structure = '多头开仓推动'
    elif position_score < -MOMENTUM_THRESHOLD:
        position_structure = '空头开仓推动'
    elif position_score > 0:
        position_structure = '蓄势增仓'
    elif position_score < 0:
        position_structure = '降温减仓'
    else:
        position_structure = '换手'

    trade_signal, operation_advice = _score_signal(total_score)

    current_price = sum((metric['current_price'] or 0) * metric['weight'] for metric in valid_metrics) if valid_metrics else None
    current_open_interest = sum((metric['open_interest'] or 0) for metric in valid_metrics)
    current_open_interest_value = sum((metric['open_interest_value'] or 0) for metric in valid_metrics)
    quote_volume = sum((metric['quote_volume'] or 0) for metric in valid_metrics)

    primary_metric = max(valid_metrics, key=lambda metric: metric.get('weight', 0))

    return {
        'exchange_scores': sorted(valid_metrics, key=lambda metric: metric.get('weight', 0), reverse=True),
        'weights': weights,
        'total_score': total_score,
        'trend_score': trend_score,
        'momentum_score': momentum_score,
        'position_score': position_score,
        'sentiment_score': sentiment_score,
        'risk_score': risk_score,
        'trend_direction': trend_direction,
        'momentum_direction': momentum_direction,
        'position_structure': position_structure,
        'sentiment_state': sentiment_state,
        'risk_level': risk_level,
        'risk_reasons': list(risk_reasons),
        'trade_signal': trade_signal,
        'operation_advice': operation_advice,
        'current_price': current_price,
        'current_open_interest': current_open_interest,
        'current_open_interest_value': current_open_interest_value,
        'quote_volume': quote_volume,
        'primary_metric': primary_metric,
    }


def _build_symbol_report(symbol, exchange_metrics, binance_context, funding_rate, anchor_time, exchange_diagnostics=None):
    valid_metrics = [metric for metric in exchange_metrics if metric is not None]
    if not valid_metrics:
        return None

    latest_top_ratio = binance_context.get('top_long_short_position_ratio', {})
    latest_global_ratio = binance_context.get('global_long_short_account_ratio', {})

    top_current = latest_top_ratio.get('current') or {}
    top_previous = latest_top_ratio.get('previous') or {}
    global_current = latest_global_ratio.get('current') or {}
    global_previous = latest_global_ratio.get('previous') or {}

    top_delta = None
    if top_current.get('long_short_ratio') is not None and top_previous.get('long_short_ratio') is not None:
        top_delta = top_current['long_short_ratio'] - top_previous['long_short_ratio']

    global_delta = None
    if global_current.get('long_short_ratio') is not None and global_previous.get('long_short_ratio') is not None:
        global_delta = global_current['long_short_ratio'] - global_previous['long_short_ratio']

    sentiment_score, sentiment_state = _calc_sentiment_score(top_delta, global_delta)

    weights = _build_metric_weights(valid_metrics)
    for metric in valid_metrics:
        metric['weight'] = weights.get(metric['exchange'], 0)
        metric['weight_percent'] = metric['weight'] * 100

    weighted_funding_rate = _weighted_metric_value(valid_metrics, 'funding_rate')
    primary_metric = max(valid_metrics, key=lambda metric: metric.get('weight', 0))
    direction_hint = primary_metric.get('score_direction_hint', 'neutral')
    price_move_ratio = primary_metric.get('price_move_ratio')
    atr_ratio = primary_metric.get('atr_ratio')
    effective_funding_rate = weighted_funding_rate if weighted_funding_rate is not None else funding_rate
    risk_score, risk_level, risk_reasons = _calc_risk_score(direction_hint, effective_funding_rate, price_move_ratio, atr_ratio)

    aggregate = _aggregate_weighted_scores(
        exchange_metrics=valid_metrics,
        sentiment_score=sentiment_score,
        sentiment_state=sentiment_state,
        risk_score=risk_score,
        risk_level=risk_level,
        risk_reasons=risk_reasons,
    )
    if aggregate is None:
        return None

    exchange_scores = aggregate['exchange_scores']
    included_exchanges = [metric['exchange'] for metric in exchange_scores]
    if exchange_diagnostics:
        missing_exchanges = [item['exchange'] for item in exchange_diagnostics if not item.get('included')]
    else:
        missing_exchanges = [exchange for exchange in _normalize_exchange_list(ENABLED_EXCHANGES) if exchange not in included_exchanges]
    status = 'complete' if not missing_exchanges else 'partial'

    total_score = aggregate['total_score']
    trade_signal, operation_advice = _score_signal(total_score)

    exchange_open_interest = []
    for metric in exchange_scores:
        exchange_open_interest.append(
            {
                'exchange': metric['exchange'],
                'open_interest': metric['open_interest'],
                'open_interest_formatted': metric['open_interest'],
                'open_interest_value': metric['open_interest_value'],
                'open_interest_value_formatted': metric['open_interest_value'],
                'share_percent': metric['weight'] * 100,
                'quantity_share_percent': metric['weight'] * 100,
                'score': metric['total_score'],
                'weighted_score': metric['weighted_total_score'],
            }
        )

    return {
        'symbol': symbol,
        'current_time': anchor_time,
        'status': status,
        'source_exchanges': included_exchanges,
        'included_exchanges': included_exchanges,
        'missing_exchanges': missing_exchanges,
        'exchange_diagnostics': exchange_diagnostics or [],
        'current_price': aggregate['current_price'],
        'current_open_interest': aggregate['current_open_interest'],
        'current_open_interest_value': aggregate['current_open_interest_value'],
        'quote_volume': aggregate['quote_volume'],
        'trend_score': aggregate['trend_score'],
        'momentum_score': aggregate['momentum_score'],
        'position_score': aggregate['position_score'],
        'sentiment_score': aggregate['sentiment_score'],
        'risk_score': aggregate['risk_score'],
        'total_score': total_score,
        'trend_direction': aggregate['trend_direction'],
        'momentum_direction': aggregate['momentum_direction'],
        'position_structure': aggregate['position_structure'],
        'sentiment_state': aggregate['sentiment_state'],
        'risk_level': aggregate['risk_level'],
        'trade_signal': trade_signal,
        'operation_advice': operation_advice,
        'exchange_open_interest': exchange_open_interest,
        'exchange_scores': exchange_scores,
        'funding_rate': effective_funding_rate,
        'binance_context': binance_context,
        'top_long_short_ratio': top_current.get('long_short_ratio'),
        'global_long_short_ratio': global_current.get('long_short_ratio'),
        'top_long_short_ratio_delta': top_delta,
        'global_long_short_ratio_delta': global_delta,
        'raw_inputs': {
            'funding_rate': effective_funding_rate,
            'top_long_short_position_ratio': top_current,
            'top_long_short_account_ratio': (binance_context.get('top_long_short_account_ratio') or {}).get('current') or {},
            'global_long_short_account_ratio': global_current,
        },
    }


def get_market_structure_score_snapshot(symbols=None, session=None, now_ms=None, exchanges=None):
    target_symbols = symbols if symbols is not None else get_market_structure_score_symbols(session=session)
    target_exchanges = _normalize_exchange_list(exchanges or ENABLED_EXCHANGES)
    if not target_symbols or not target_exchanges:
        return {
            'data': [],
            'cache_update_time': None,
            'summary': {
                'total_symbols': 0,
                'complete_symbols': 0,
                'partial_symbols': 0,
                'empty_symbols': 0,
                'strong_long_count': 0,
                'long_count': 0,
                'neutral_count': 0,
                'short_count': 0,
                'strong_short_count': 0,
            },
        }

    own_session = session is None
    db = session
    if db is None:
        from coinx.database import get_session as get_db_session

        db = get_db_session()

    try:
        current_time_ms = now_ms if now_ms is not None else __import__('time').time() * 1000
        anchor_time = latest_closed_5m_open_time(int(current_time_ms))
        supported_exchanges = set(_normalize_exchange_list(get_supported_exchange_ids()))
        exchange_maps = {}

        overall_start = time.perf_counter()
        logger.info(
            '开始计算合约市场结构评分: symbols=%d exchanges=%d anchor_time=%s',
            len(target_symbols),
            len(target_exchanges),
            anchor_time,
        )
        load_start = time.perf_counter()

        eligible_exchanges = [exchange for exchange in target_exchanges if exchange in supported_exchanges]
        max_workers = min(MARKET_STRUCTURE_EXCHANGE_MAX_WORKERS, len(eligible_exchanges) or 1)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_exchange = {
                executor.submit(_load_exchange_map_for_score, exchange, target_symbols, anchor_time): exchange
                for exchange in eligible_exchanges
            }
            for future in as_completed(future_to_exchange):
                exchange = future_to_exchange[future]
                exchange_maps[exchange] = future.result()
        exchange_load_duration = time.perf_counter() - load_start

        logger.info(
            '评分数据加载完成: symbols=%d exchanges=%d 耗时=%.2fs',
            len(target_symbols),
            len(exchange_maps),
            exchange_load_duration,
        )

        binance_context_start = time.perf_counter()
        binance_context_map = _load_binance_context_map(db, target_symbols, anchor_time)
        context_duration = time.perf_counter() - binance_context_start
        logger.info(
            '评分 Binance 情绪数据加载完成: symbols=%d 耗时=%.2fs',
            len(target_symbols),
            context_duration,
        )

        funding_start = time.perf_counter()
        exchange_funding_maps = _load_exchange_funding_rate_maps(exchange_maps.keys(), target_symbols)
        funding_duration = time.perf_counter() - funding_start
        logger.info(
            '评分资金费率批量加载完成: exchanges=%d 耗时=%.2fs',
            len(exchange_funding_maps),
            funding_duration,
        )

        data = []
        summary = {
            'total_symbols': 0,
            'complete_symbols': 0,
            'partial_symbols': 0,
            'empty_symbols': 0,
            'strong_long_count': 0,
            'long_count': 0,
            'neutral_count': 0,
            'short_count': 0,
            'strong_short_count': 0,
            'low_risk_count': 0,
            'medium_risk_count': 0,
            'high_risk_count': 0,
        }
        summary['sentiment_health'] = _summarize_binance_context_health(
            binance_context_map,
            anchor_time=anchor_time,
            symbol_count=len(target_symbols),
        )
        symbol_collect_duration = 0.0
        symbol_align_duration = 0.0
        symbol_report_duration = 0.0
        symbol_total_duration = 0.0

        for index, symbol in enumerate(target_symbols, start=1):
            symbol_start = time.perf_counter()

            collect_start = time.perf_counter()
            per_exchange_metrics = []
            per_exchange_current_times = []

            for exchange, (oi_map, kline_map, taker_maps_by_period, quote_volume_24h_map) in exchange_maps.items():
                symbol_oi = oi_map.get(symbol, {})
                symbol_kline = kline_map.get(symbol, {})
                if not symbol_oi or not symbol_kline:
                    continue

                current_times = sorted(set(symbol_oi).intersection(symbol_kline))
                if not current_times:
                    continue
                exchange_anchor_time = current_times[-1]
                exchange_funding_rate = (exchange_funding_maps.get(exchange) or {}).get(symbol)
                exchange_quote_volume_24h = (quote_volume_24h_map or {}).get(symbol)
                metric = _build_exchange_metric(
                    exchange=exchange,
                    symbol=symbol,
                    oi_by_time=symbol_oi,
                    kline_by_time=symbol_kline,
                    taker_maps_by_period={
                        period: period_map.get(symbol, {})
                        for period, period_map in (taker_maps_by_period or {}).items()
                    },
                    anchor_time=exchange_anchor_time,
                    funding_rate=exchange_funding_rate,
                    quote_volume_24h=exchange_quote_volume_24h,
                )
                if metric is not None:
                    per_exchange_metrics.append(metric)
                    per_exchange_current_times.append(exchange_anchor_time)

            collect_duration = time.perf_counter() - collect_start
            symbol_collect_duration += collect_duration

            if not per_exchange_metrics:
                summary['empty_symbols'] += 1
                continue

            align_start = time.perf_counter()
            symbol_anchor_time = min(per_exchange_current_times) if per_exchange_current_times else anchor_time
            aligned_metrics = []
            for metric in per_exchange_metrics:
                exchange = metric['exchange']
                oi_map, kline_map, taker_maps_by_period, quote_volume_24h_map = exchange_maps.get(exchange, ({}, {}, {}, {}))
                symbol_oi = oi_map.get(symbol, {})
                symbol_kline = kline_map.get(symbol, {})
                if symbol_anchor_time not in symbol_oi or symbol_anchor_time not in symbol_kline:
                    continue
                aligned_metric = _build_exchange_metric(
                    exchange=exchange,
                    symbol=symbol,
                    oi_by_time=symbol_oi,
                    kline_by_time=symbol_kline,
                    taker_maps_by_period={
                        period: period_map.get(symbol, {})
                        for period, period_map in (taker_maps_by_period or {}).items()
                    },
                    anchor_time=symbol_anchor_time,
                    funding_rate=metric.get('funding_rate'),
                    quote_volume_24h=(quote_volume_24h_map or {}).get(symbol),
                )
                if aligned_metric is not None:
                    aligned_metrics.append(aligned_metric)

            align_duration = time.perf_counter() - align_start
            symbol_align_duration += align_duration

            if not aligned_metrics:
                summary['empty_symbols'] += 1
                continue

            report_start = time.perf_counter()
            exchange_diagnostics = _build_symbol_exchange_diagnostics(
                exchange_maps=exchange_maps,
                symbol=symbol,
                symbol_anchor_time=symbol_anchor_time,
                included_exchanges=[metric['exchange'] for metric in aligned_metrics],
                enabled_exchanges=target_exchanges,
            )
            binance_context = binance_context_map.get(symbol, {})
            report = _build_symbol_report(
                symbol=symbol,
                exchange_metrics=aligned_metrics,
                binance_context=binance_context,
                funding_rate=None,
                anchor_time=symbol_anchor_time,
                exchange_diagnostics=exchange_diagnostics,
            )
            if report is None:
                summary['empty_symbols'] += 1
                continue

            report_duration = time.perf_counter() - report_start
            symbol_report_duration += report_duration

            data.append(report)
            summary['total_symbols'] += 1
            if report['status'] == 'complete':
                summary['complete_symbols'] += 1
            elif report['status'] == 'partial':
                summary['partial_symbols'] += 1
            else:
                summary['empty_symbols'] += 1

            if report['risk_level'] == '低':
                summary['low_risk_count'] += 1
            elif report['risk_level'] == '中':
                summary['medium_risk_count'] += 1
            elif report['risk_level'] == '高':
                summary['high_risk_count'] += 1

            signal_key = report['trade_signal']
            if signal_key == '强多':
                summary['strong_long_count'] += 1
            elif signal_key == '偏多':
                summary['long_count'] += 1
            elif signal_key == '震荡':
                summary['neutral_count'] += 1
            elif signal_key == '偏空':
                summary['short_count'] += 1
            elif signal_key == '强空':
                summary['strong_short_count'] += 1

            symbol_duration = time.perf_counter() - symbol_start
            symbol_total_duration += symbol_duration

        data.sort(
            key=lambda item: (
                -(item.get('total_score') or 0),
                -(item.get('current_open_interest_value') or 0),
                item.get('symbol') or '',
            )
        )
        for index, item in enumerate(data, start=1):
            item['rank_index'] = index

        overall_duration = time.perf_counter() - overall_start
        logger.info(
            '合约市场结构评分阶段汇总: total=%.2fs exchange_maps=%.2fs binance_context=%.2fs funding=%.2fs symbol_collect=%.2fs symbol_align=%.2fs symbol_report=%.2fs symbol_total=%.2fs',
            overall_duration,
            exchange_load_duration,
            context_duration,
            funding_duration,
            symbol_collect_duration,
            symbol_align_duration,
            symbol_report_duration,
            symbol_total_duration,
        )
        logger.info(
            '合约市场结构评分完成: total=%d complete=%d partial=%d empty=%d 耗时=%.2fs',
            summary['total_symbols'],
            summary['complete_symbols'],
            summary['partial_symbols'],
            summary['empty_symbols'],
            overall_duration,
        )

        return {
            'data': data,
            'cache_update_time': anchor_time if data else None,
            'summary': summary,
        }
    finally:
        if own_session:
            db.close()
