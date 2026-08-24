"""Multi-timeframe trade opportunity scoring derived from 5m market data."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import math
import threading
import time

from coinx.collector.exchange_adapters import get_exchange_adapter, get_supported_exchange_ids
from coinx.collector.exchange_repair import latest_closed_5m_open_time
from coinx.config import ENABLED_EXCHANGES
from coinx.repositories.market_structure_score import (
    _atr,
    _ema,
    _load_exchange_funding_rate_maps,
    _normalize_exchange_list,
    get_market_structure_score_symbols,
)
from coinx.repositories.market_structure_series import (
    load_market_structure_aggregated_kline_maps,
    load_market_structure_exchange_maps,
)

FIVE_MINUTES_MS = 5 * 60 * 1000
TREND_KLINE_POINTS = 120
FLOW_WINDOW_POINTS = 12
EMA_FAST_PERIOD = 20
EMA_SLOW_PERIOD = 60
ONE_HOUR_MS = 12 * FIVE_MINUTES_MS
FOUR_HOURS_MS = 48 * FIVE_MINUTES_MS
HIGHER_TIMEFRAME_SLOPE_POINTS = 12
TRADE_KLINE_LOOKBACK_MS = (TREND_KLINE_POINTS - 1) * FIVE_MINUTES_MS
HIGHER_TIMEFRAME_KLINE_POINTS = 72
PIVOT_RADIUS = 2
STOP_ATR_BUFFER = 0.25
FALLBACK_STOP_ATR = 1.5

_SNAPSHOT_CACHE_LOCK = threading.Lock()
_SNAPSHOT_CACHE = {}
_SNAPSHOT_INFLIGHT = {}

PLAN_DIRECTIONS = {
    '可做多': 1,
    '等待回踩': 1,
    '可做空': -1,
    '等待反弹': -1,
}


@dataclass(frozen=True)
class _AggregatedKlinePoint:
    open_time: int
    high_price: float
    low_price: float
    close_price: float
    quote_volume: float


def _aggregate_closed_5m_klines(points, interval_ms):
    """Aggregate only UTC-aligned buckets containing every expected 5m bar."""
    by_time = {int(point.open_time): point for point in points if getattr(point, 'open_time', None) is not None}
    buckets = {}
    for open_time, point in by_time.items():
        buckets.setdefault(open_time - (open_time % interval_ms), []).append(point)

    result = []
    expected_points = interval_ms // FIVE_MINUTES_MS
    for bucket_time in sorted(buckets):
        expected_times = [bucket_time + index * FIVE_MINUTES_MS for index in range(expected_points)]
        if any(timestamp not in by_time for timestamp in expected_times):
            continue
        bucket = [by_time[timestamp] for timestamp in expected_times]
        highs = [_float(getattr(point, 'high_price', None)) for point in bucket]
        lows = [_float(getattr(point, 'low_price', None)) for point in bucket]
        closes = [_float(getattr(point, 'close_price', None)) for point in bucket]
        if any(value is None for value in highs + lows + closes):
            continue
        result.append(_AggregatedKlinePoint(
            open_time=bucket_time,
            high_price=max(highs),
            low_price=min(lows),
            close_price=closes[-1],
            quote_volume=sum(_float(getattr(point, 'quote_volume', None)) or 0.0 for point in bucket),
        ))
    return result


def _higher_timeframe_price_trend_score(points, alignment_weight, slope_weight):
    """Return a price-only trend contribution and its diagnostic state."""
    if len(points) < EMA_SLOW_PERIOD:
        return 0, '数据不足'
    closes = [_float(point.close_price) for point in points]
    if any(value is None for value in closes):
        return 0, '数据不足'

    ema20 = _ema(closes[-EMA_FAST_PERIOD:], EMA_FAST_PERIOD)
    ema60 = _ema(closes[-EMA_SLOW_PERIOD:], EMA_SLOW_PERIOD)
    score = 0
    if closes[-1] > ema20 > ema60:
        score += alignment_weight
    elif closes[-1] < ema20 < ema60:
        score -= alignment_weight

    if len(closes) >= EMA_FAST_PERIOD + HIGHER_TIMEFRAME_SLOPE_POINTS:
        previous_ema20 = _ema(
            closes[-(EMA_FAST_PERIOD + HIGHER_TIMEFRAME_SLOPE_POINTS):-HIGHER_TIMEFRAME_SLOPE_POINTS],
            EMA_FAST_PERIOD,
        )
        if ema20 > previous_ema20:
            score += slope_weight
        elif ema20 < previous_ema20:
            score -= slope_weight
    return score, _trend_state(score)


def _float(value):
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _clamp(value, lower, upper):
    return max(lower, min(upper, value))


def _sign(value):
    return 1 if value > 0 else (-1 if value < 0 else 0)


def _trend_state(score):
    if score >= 30:
        return '强多趋势'
    if score >= 10:
        return '多头趋势'
    if score <= -30:
        return '强空趋势'
    if score <= -10:
        return '空头趋势'
    return '震荡'


def _entry_state(score):
    if score >= 65:
        return '可做多'
    if score >= 20:
        return '等待回踩'
    if score <= -65:
        return '可做空'
    if score <= -20:
        return '等待反弹'
    return '观望'


def _entry_score(trend_score, timing_score, risk_score):
    directional_score = trend_score + timing_score
    return _sign(directional_score) * max(0, abs(directional_score) + risk_score)


def _weighted(metrics, field):
    return sum(metric[field] * metric['weight'] for metric in metrics)


def _point_oi(point):
    return _float(getattr(point, 'sum_open_interest', None))


def _flow_point(taker_map, timestamp):
    return taker_map.get(timestamp)


def _flow_window_metrics(points, taker_map):
    flow_value = 0.0
    quote_volume = 0.0
    for point in points:
        flow_point = _flow_point(taker_map, point.open_time)
        if flow_point is None:
            continue
        buy = _float(getattr(flow_point, 'buy_vol', None)) or 0.0
        sell = _float(getattr(flow_point, 'sell_vol', None)) or 0.0
        price = _float(point.close_price) or 0.0
        flow_value += (buy - sell) * price
        quote_volume += _float(point.quote_volume) or 0.0
    return flow_value, flow_value / quote_volume if quote_volume else None


def _confirmed_pivots(points):
    highs, lows = [], []
    for index in range(PIVOT_RADIUS, len(points) - PIVOT_RADIUS):
        high = _float(getattr(points[index], 'high_price', None))
        low = _float(getattr(points[index], 'low_price', None))
        if high is not None:
            neighbors = [
                _float(getattr(points[offset], 'high_price', None))
                for offset in range(index - PIVOT_RADIUS, index + PIVOT_RADIUS + 1)
                if offset != index
            ]
            if all(value is not None and high > value for value in neighbors):
                highs.append((index, high))
        if low is not None:
            neighbors = [
                _float(getattr(points[offset], 'low_price', None))
                for offset in range(index - PIVOT_RADIUS, index + PIVOT_RADIUS + 1)
                if offset != index
            ]
            if all(value is not None and low < value for value in neighbors):
                lows.append((index, low))
    return highs, lows


def _latest_confirmed_pivot_high(points):
    pivot_highs, _ = _confirmed_pivots(points)
    if not pivot_highs:
        return None
    index, price = pivot_highs[-1]
    return {
        'price': price,
        'time': getattr(points[index], 'open_time', None),
    }


def _distinct_levels(levels, atr, reverse=False):
    result = []
    tolerance = max(atr * 0.05, 1e-12)
    for level in sorted(levels, reverse=reverse):
        if not any(abs(level - existing) <= tolerance for existing in result):
            result.append(level)
    return result


def _direction_percent(entry_price, level, direction):
    return round(direction * (level - entry_price) / entry_price * 100, 6)


def _higher_timeframe_pivots(metric):
    highs, lows = [], []
    for higher_points in (metric.get('_higher_timeframe_points') or {}).values():
        pivot_highs, pivot_lows = _confirmed_pivots(higher_points)
        highs.extend((higher_points[index].open_time, price) for index, price in pivot_highs)
        lows.extend((higher_points[index].open_time, price) for index, price in pivot_lows)
    return highs, lows


def _build_trade_plan(entry_state, metric):
    direction = PLAN_DIRECTIONS.get(entry_state)
    if direction is None:
        return None
    if metric is None:
        return {
            'source_exchange': 'binance',
            'status': 'unavailable',
            'reason': 'Binance 数据不足',
        }

    current_price = _float(metric.get('current_price'))
    ema20 = _float(metric.get('ema20'))
    atr = _float(metric.get('atr'))
    points = metric.get('_plan_points') or []
    if current_price is None or ema20 is None or atr is None or atr <= 0 or not points:
        return {
            'source_exchange': 'binance',
            'status': 'unavailable',
            'reason': 'Binance 5m 数据不足',
        }

    entry_price = ema20 if entry_state in {'等待回踩', '等待反弹'} else current_price
    pivot_highs, pivot_lows = _confirmed_pivots(points)
    higher_highs, higher_lows = _higher_timeframe_pivots(metric)
    if direction > 0:
        stop_candidates = [(getattr(points[index], 'open_time', index), price) for index, price in pivot_lows if price < entry_price]
        higher_stop_candidates = [(timestamp, price) for timestamp, price in higher_lows if price < entry_price]
        selected_stop = max(higher_stop_candidates or stop_candidates, default=None)
        stop_loss = (selected_stop[1] - STOP_ATR_BUFFER * atr) if selected_stop else (entry_price - FALLBACK_STOP_ATR * atr)
        stop_source = 'higher_timeframe' if higher_stop_candidates else ('5m' if stop_candidates else 'atr')
        fallback_targets = _distinct_levels([price for _, price in pivot_highs if price > entry_price], atr)
    else:
        stop_candidates = [(getattr(points[index], 'open_time', index), price) for index, price in pivot_highs if price > entry_price]
        higher_stop_candidates = [(timestamp, price) for timestamp, price in higher_highs if price > entry_price]
        selected_stop = max(higher_stop_candidates or stop_candidates, default=None)
        stop_loss = (selected_stop[1] + STOP_ATR_BUFFER * atr) if selected_stop else (entry_price + FALLBACK_STOP_ATR * atr)
        stop_source = 'higher_timeframe' if higher_stop_candidates else ('5m' if stop_candidates else 'atr')
        fallback_targets = _distinct_levels([price for _, price in pivot_lows if price < entry_price], atr, reverse=True)

    risk_distance = abs(entry_price - stop_loss)
    if risk_distance <= 0:
        return {
            'source_exchange': 'binance',
            'status': 'unavailable',
            'reason': 'Binance 止损结构无效',
        }

    higher_levels = []
    if direction > 0:
        higher_levels.extend(price for _, price in higher_highs if price > entry_price)
    else:
        higher_levels.extend(price for _, price in higher_lows if price < entry_price)
    structural_targets = _distinct_levels(higher_levels, atr, reverse=direction < 0)
    target_source = 'higher_timeframe' if structural_targets else ('5m' if fallback_targets else 'r_extension')
    targets = (structural_targets or fallback_targets)[:3]
    multiple = 1
    while len(targets) < 3:
        candidate = entry_price + direction * risk_distance * multiple
        multiple += 1
        previous = targets[-1] if targets else entry_price
        if (direction > 0 and candidate > previous) or (direction < 0 and candidate < previous):
            targets.append(candidate)

    target_r_values = [round(abs(target - entry_price) / risk_distance, 4) for target in targets]
    space_status = 'adequate' if target_r_values[0] >= 1 else 'insufficient'

    return {
        'source_exchange': 'binance',
        'status': 'ready',
        'entry_price': entry_price,
        'atr': atr,
        'stop_loss': stop_loss,
        'stop_source': stop_source,
        'tp1': targets[0],
        'tp2': targets[1],
        'tp3': targets[2],
        'stop_loss_percent': _direction_percent(entry_price, stop_loss, direction),
        'tp1_percent': _direction_percent(entry_price, targets[0], direction),
        'tp2_percent': _direction_percent(entry_price, targets[1], direction),
        'tp3_percent': _direction_percent(entry_price, targets[2], direction),
        'tp1_r': target_r_values[0],
        'tp2_r': target_r_values[1],
        'tp3_r': target_r_values[2],
        'target_source': target_source,
        'space_status': space_status,
        'space_reason': '前方结构空间不足（TP1 < 1R）' if space_status == 'insufficient' else None,
    }


def _exchange_metric(
    exchange,
    symbol,
    oi_by_time,
    kline_by_time,
    taker_maps,
    anchor,
    funding_rate,
    higher_timeframe_points=None,
):
    times = sorted(timestamp for timestamp in kline_by_time if timestamp <= anchor)
    if len(times) < EMA_SLOW_PERIOD or anchor not in oi_by_time:
        return None
    all_points = [kline_by_time[timestamp] for timestamp in times]
    points = all_points[-TREND_KLINE_POINTS:]
    closes = [_float(point.close_price) for point in points]
    if any(value is None for value in closes):
        return None
    current = points[-1]
    current_price = closes[-1]
    ema20 = _ema(closes[-EMA_FAST_PERIOD:], EMA_FAST_PERIOD)
    ema60 = _ema(closes[-EMA_SLOW_PERIOD:], EMA_SLOW_PERIOD)
    previous_ema20 = _ema(closes[-(EMA_FAST_PERIOD + FLOW_WINDOW_POINTS):-FLOW_WINDOW_POINTS], EMA_FAST_PERIOD)
    atr = _atr(points[-(EMA_FAST_PERIOD + 1):])
    if None in (ema20, ema60, previous_ema20, atr) or atr <= 0:
        return None

    price_trend_score = 0
    if current_price > ema20 > ema60:
        price_trend_score += 15
    elif current_price < ema20 < ema60:
        price_trend_score -= 15
    if ema20 > previous_ema20:
        price_trend_score += 5
    elif ema20 < previous_ema20:
        price_trend_score -= 5
    if len(points) >= 48:
        recent, previous = points[-24:], points[-48:-24]
        recent_high = max(_float(point.high_price) or current_price for point in recent)
        recent_low = min(_float(point.low_price) or current_price for point in recent)
        previous_high = max(_float(point.high_price) or current_price for point in previous)
        previous_low = min(_float(point.low_price) or current_price for point in previous)
        if recent_high > previous_high and recent_low > previous_low:
            price_trend_score += 5
        elif recent_high < previous_high and recent_low < previous_low:
            price_trend_score -= 5

    if higher_timeframe_points is None:
        higher_timeframe_points = {
            '1h': _aggregate_closed_5m_klines(all_points, ONE_HOUR_MS),
            '4h': _aggregate_closed_5m_klines(all_points, FOUR_HOURS_MS),
        }
    one_hour_score, one_hour_state = _higher_timeframe_price_trend_score(
        higher_timeframe_points['1h'], alignment_weight=3, slope_weight=1,
    )
    four_hour_score, four_hour_state = _higher_timeframe_price_trend_score(
        higher_timeframe_points['4h'], alignment_weight=5, slope_weight=1,
    )
    htf_price_trend_score = one_hour_score + four_hour_score
    price_trend_score += htf_price_trend_score

    previous_time = anchor - FIVE_MINUTES_MS
    hour_time = anchor - FLOW_WINDOW_POINTS * FIVE_MINUTES_MS
    previous_oi = _point_oi(oi_by_time.get(previous_time))
    hour_oi = _point_oi(oi_by_time.get(hour_time))
    current_oi = _point_oi(oi_by_time.get(anchor))
    hour_price_point = kline_by_time.get(hour_time)
    hour_price = _float(getattr(hour_price_point, 'close_price', None))
    oi_trend_score = 0
    oi_change_1h = None
    price_change_1h = None
    if current_oi not in (None, 0) and hour_oi not in (None, 0) and hour_price not in (None, 0):
        oi_change_1h = (current_oi - hour_oi) / hour_oi
        price_change_1h = (current_price - hour_price) / hour_price
        if abs(oi_change_1h) >= .005 and abs(price_change_1h) >= .001:
            if price_change_1h > 0:
                oi_trend_score = 15 if oi_change_1h > 0 else 5
            else:
                oi_trend_score = -15 if oi_change_1h > 0 else -5

    try:
        taker_period = get_exchange_adapter(exchange).taker_period_for_interval('5m') or '5m'
    except Exception:
        taker_period = '5m'
    taker_map = (taker_maps or {}).get(taker_period, {})
    flow_value, flow_ratio_1h = _flow_window_metrics(points[-FLOW_WINDOW_POINTS:], taker_map)
    previous_flow_value, previous_flow_ratio_1h = _flow_window_metrics(
        points[-(FLOW_WINDOW_POINTS * 2):-FLOW_WINDOW_POINTS], taker_map,
    )
    flow_trend_score = _clamp((flow_ratio_1h or 0) * 100, -10, 10)
    trend_score = price_trend_score + oi_trend_score + flow_trend_score
    direction = _sign(trend_score)

    distance_atr = (current_price - ema20) / atr
    price_timing_score = 0
    recent_points = points[-FLOW_WINDOW_POINTS:]
    if direction > 0:
        touched = min(_float(point.low_price) or current_price for point in recent_points) <= ema20
        if touched and current_price > ema20 and 0 <= distance_atr <= .75:
            price_timing_score = 30
        elif distance_atr > 1.5:
            price_timing_score = -20
    elif direction < 0:
        touched = max(_float(point.high_price) or current_price for point in recent_points) >= ema20
        if touched and current_price < ema20 and -.75 <= distance_atr <= 0:
            price_timing_score = -30
        elif distance_atr < -1.5:
            price_timing_score = 20

    oi_5m_change = None
    if current_oi not in (None, 0) and previous_oi not in (None, 0):
        oi_5m_change = (current_oi - previous_oi) / previous_oi
    latest_flow_ratio = None
    latest_taker = _flow_point(taker_map, anchor)
    if latest_taker is not None:
        buy = _float(getattr(latest_taker, 'buy_vol', None)) or 0.0
        sell = _float(getattr(latest_taker, 'sell_vol', None)) or 0.0
        latest_volume = _float(current.quote_volume) or 0.0
        if latest_volume:
            latest_flow_ratio = ((buy - sell) * current_price) / latest_volume
    price_change_5m = (current_price - closes[-2]) / closes[-2] if closes[-2] else None
    contract_timing_score = 0
    if direction and oi_5m_change is not None and latest_flow_ratio is not None and price_change_5m is not None:
        oi_direction = _sign(price_change_5m) * (1 if oi_5m_change > 0 else -1)
        flow_direction = _sign(latest_flow_ratio)
        if oi_direction == direction and flow_direction == direction:
            contract_timing_score = 10 * direction
        elif oi_direction == -direction and flow_direction == -direction:
            contract_timing_score = -10 * direction
    timing_score = price_timing_score + contract_timing_score

    risk_score = 0
    reasons = []
    if direction and funding_rate is not None and _sign(funding_rate) == direction and abs(funding_rate) >= .0008:
        risk_score -= 10; reasons.append('资金费率过热')
    if oi_change_1h is not None and oi_change_1h >= .06:
        risk_score -= 10; reasons.append('OI 1h 增速过快')
    atr_ratio = atr / current_price if current_price else None
    if (
        direction and price_change_5m is not None and _sign(price_change_5m) == direction and abs(price_change_5m) >= .02
    ) or (atr_ratio is not None and atr_ratio >= .02):
        risk_score -= 10; reasons.append('短周期波动过大')
    risk_score = max(-30, risk_score)
    oi_value = _float(getattr(oi_by_time[anchor], 'sum_open_interest_value', None)) or 0.0
    previous_highs = {
        timeframe: _latest_confirmed_pivot_high(higher_timeframe_points.get(timeframe) or [])
        for timeframe in ('1h', '4h')
    }
    return {
        'exchange': exchange, 'current_price': current_price, 'open_interest_value': oi_value,
        'ema20': ema20, 'atr': atr, '_plan_points': points,
        '_higher_timeframe_points': higher_timeframe_points,
        'trend_score': trend_score, 'timing_score': timing_score, 'risk_score': risk_score,
        'price_trend_score': price_trend_score, 'oi_trend_score': oi_trend_score,
        'flow_trend_score': flow_trend_score, 'htf_price_trend_score': htf_price_trend_score,
        'timeframe_trends': {'1h': one_hour_state, '4h': four_hour_state}, 'price_timing_score': price_timing_score,
        'contract_timing_score': contract_timing_score, 'ema20_distance_atr': distance_atr,
        'oi_change_5m': oi_5m_change, 'oi_change_1h': oi_change_1h, 'price_change_1h': price_change_1h,
        'flow_value_1h': flow_value, 'flow_ratio_1h': flow_ratio_1h,
        'previous_flow_value_1h': previous_flow_value,
        'previous_flow_ratio_1h': previous_flow_ratio_1h,
        'net_outflow_ratio_1h': max(0.0, -(flow_ratio_1h or 0.0)),
        'previous_net_outflow_ratio_1h': max(0.0, -(previous_flow_ratio_1h or 0.0)),
        'previous_high_1h': previous_highs['1h']['price'] if previous_highs['1h'] else None,
        'previous_high_1h_time': previous_highs['1h']['time'] if previous_highs['1h'] else None,
        'previous_high_4h': previous_highs['4h']['price'] if previous_highs['4h'] else None,
        'previous_high_4h_time': previous_highs['4h']['time'] if previous_highs['4h'] else None,
        'funding_rate': funding_rate, 'risk_reasons': reasons,
        'current_time': anchor,
    }


def _build_trade_opportunity_snapshot(symbols, exchanges, anchor):
    supported = set(get_supported_exchange_ids())
    maps = {}
    with ThreadPoolExecutor(max_workers=min(4, len(exchanges) or 1)) as executor:
        futures = {
            executor.submit(
                load_market_structure_exchange_maps,
                None,
                exchange,
                symbols,
                anchor,
                kline_lookback_ms=TRADE_KLINE_LOOKBACK_MS,
                include_quote_volume_24h=False,
            ): exchange
            for exchange in exchanges
            if exchange in supported
        }
        for future in as_completed(futures):
            maps[futures[future]] = future.result()
        # Worker completion order is nondeterministic. Preserve the configured
        # exchange order in the response so replay comparisons and clients do
        # not observe array elements moving between requests.
        maps = {exchange: maps[exchange] for exchange in exchanges if exchange in maps}
        # Higher-timeframe aggregation is the widest ClickHouse query in this
        # snapshot. Running one query per exchange at a time keeps the sum of
        # AggregatingTransform memory bounded; the source maps above are still
        # loaded concurrently because their windows are small and bounded.
        aggregated_kline_maps = {}
        for exchange in maps:
            aggregated_kline_maps[exchange] = load_market_structure_aggregated_kline_maps(
                None,
                exchange,
                symbols,
                anchor,
                intervals={'1h': ONE_HOUR_MS, '4h': FOUR_HOURS_MS},
                lookback_points=HIGHER_TIMEFRAME_KLINE_POINTS,
            )
    funding_maps = _load_exchange_funding_rate_maps(maps.keys(), symbols, as_of_ms=anchor)
    data = []
    for symbol in symbols:
        metrics = []
        for exchange, (oi_maps, kline_maps, taker_maps, _) in maps.items():
            oi_by_time = oi_maps.get(symbol, {})
            kline_by_time = kline_maps.get(symbol, {})
            common = sorted(set(oi_by_time).intersection(kline_by_time))
            if not common:
                continue
            metric = _exchange_metric(
                exchange,
                symbol,
                oi_by_time,
                kline_by_time,
                {period: values.get(symbol, {}) for period, values in taker_maps.items()},
                common[-1],
                (funding_maps.get(exchange) or {}).get(symbol),
                {
                    timeframe: [points_by_time[timestamp] for timestamp in sorted(points_by_time)]
                    for timeframe, values in (aggregated_kline_maps.get(exchange) or {}).items()
                    for points_by_time in [values.get(symbol, {})]
                },
            )
            if metric:
                metrics.append(metric)
        if not metrics:
            data.append({'symbol': symbol, 'data_status': 'unavailable', 'entry_state': '数据不足'})
            continue
        total_oi = sum(item['open_interest_value'] for item in metrics)
        for metric in metrics:
            metric['weight'] = metric['open_interest_value'] / total_oi if total_oi else 1 / len(metrics)
        trend_score = _weighted(metrics, 'trend_score')
        timing_score = _weighted(metrics, 'timing_score')
        risk_score = _weighted(metrics, 'risk_score')
        directional_score = trend_score + timing_score
        entry_score = _entry_score(trend_score, timing_score, risk_score)
        entry_state = _entry_state(entry_score)
        binance_metric = next((metric for metric in metrics if metric['exchange'] == 'binance'), None)
        trade_plan = _build_trade_plan(entry_state, binance_metric)
        reasons = sorted({reason for metric in metrics for reason in metric['risk_reasons']})
        for metric in metrics:
            metric.pop('_plan_points', None)
            metric.pop('_higher_timeframe_points', None)
        data.append({
            'symbol': symbol, 'data_status': 'complete', 'current_time': min(item['current_time'] for item in metrics),
            'current_price': _weighted(metrics, 'current_price'), 'trend_score': round(trend_score, 2),
            'timing_score': round(timing_score, 2), 'directional_score': round(directional_score, 2),
            'risk_score': round(risk_score, 2), 'entry_score': round(entry_score, 2),
            'trend_state': _trend_state(trend_score), 'entry_state': entry_state,
            'risk_reasons': reasons, 'trade_plan': trade_plan, 'exchange_scores': metrics,
        })
    priority = {'可做多': 0, '可做空': 1, '等待回踩': 2, '等待反弹': 3, '观望': 4, '数据不足': 5}
    data.sort(key=lambda item: (priority.get(item.get('entry_state'), 9), -abs(item.get('entry_score') or 0), item['symbol']))
    return {'data': data, 'cache_update_time': anchor, 'summary': {'total_symbols': len(data)}}


def get_trade_opportunity_snapshot(symbols=None, now_ms=None, exchanges=None):
    symbols = tuple(symbols or get_market_structure_score_symbols())
    exchanges = tuple(_normalize_exchange_list(exchanges or ENABLED_EXCHANGES))
    anchor = latest_closed_5m_open_time(int(now_ms if now_ms is not None else time.time() * 1000))
    cache_key = (anchor, symbols, exchanges)

    with _SNAPSHOT_CACHE_LOCK:
        cached_snapshot = _SNAPSHOT_CACHE.get(cache_key)
        if cached_snapshot is not None:
            return cached_snapshot
        completion = _SNAPSHOT_INFLIGHT.get(cache_key)
        if completion is None:
            completion = threading.Event()
            _SNAPSHOT_INFLIGHT[cache_key] = completion
            is_loader = True
        else:
            is_loader = False

    if not is_loader:
        completion.wait()
        with _SNAPSHOT_CACHE_LOCK:
            cached_snapshot = _SNAPSHOT_CACHE.get(cache_key)
        if cached_snapshot is not None:
            return cached_snapshot
        return _build_trade_opportunity_snapshot(symbols, exchanges, anchor)

    try:
        snapshot = _build_trade_opportunity_snapshot(symbols, exchanges, anchor)
    except Exception:
        with _SNAPSHOT_CACHE_LOCK:
            _SNAPSHOT_INFLIGHT.pop(cache_key, None)
            completion.set()
        raise

    with _SNAPSHOT_CACHE_LOCK:
        _SNAPSHOT_CACHE.clear()
        _SNAPSHOT_CACHE[cache_key] = snapshot
        _SNAPSHOT_INFLIGHT.pop(cache_key, None)
        completion.set()
    return snapshot
