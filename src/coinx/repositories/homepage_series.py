import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from sqlalchemy import and_, func, or_

from coinx.coin_manager import get_active_coins
from coinx.collector.exchange_adapters import get_exchange_adapter, get_supported_exchange_ids
from coinx.config import ENABLED_EXCHANGES, PRIMARY_PRICE_EXCHANGE, TIME_INTERVALS
from coinx.database import get_session
from coinx.utils import logger
from coinx.models import (
    BinanceKline,
    BinanceOpenInterestHist,
    BinanceTakerBuySellVol,
    MarketKline,
    MarketOpenInterestHist,
    MarketTakerBuySellVol,
)
from coinx.collector.binance.repair import latest_closed_5m_open_time


FIVE_MINUTES_MS = 5 * 60 * 1000
HOMEPAGE_REQUIRED_SERIES_TYPES = ('klines', 'open_interest_hist', 'taker_buy_sell_vol')
HOMEPAGE_BULK_QUERY_THRESHOLD = 8


@dataclass(frozen=True)
class HomepageOpenInterestPoint:
    symbol: str
    event_time: int
    sum_open_interest: Optional[float]
    sum_open_interest_value: Optional[float]


@dataclass(frozen=True)
class HomepageKlinePoint:
    symbol: str
    open_time: int
    high_price: Optional[float]
    low_price: Optional[float]
    close_price: Optional[float]
    quote_volume: Optional[float]
    taker_buy_quote_volume: Optional[float]


@dataclass(frozen=True)
class HomepageTakerBuySellVolPoint:
    symbol: str
    event_time: int
    buy_sell_ratio: Optional[float]
    buy_vol: Optional[float]
    sell_vol: Optional[float]


def format_number(num):
    if num is None:
        return "N/A"

    value = float(num)
    abs_num = abs(value)
    if abs_num >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}b"
    if abs_num >= 1_000_000:
        return f"{value / 1_000_000:.2f}m"
    if abs_num >= 1_000:
        return f"{value / 1_000:.2f}k"
    if abs_num >= 1:
        return f"{value:.2f}"
    return f"{value:.5e}"


def format_price(num):
    if num is None:
        return "N/A"

    value = Decimal(str(float(num)))
    if value == 0:
        return "0"

    plain = format(value, 'f').rstrip('0').rstrip('.')
    unsigned_plain = plain.lstrip('-')

    if '.' in unsigned_plain:
        integer_part, fractional_part = unsigned_plain.split('.')
        integer_digits = len(integer_part.lstrip('0'))
        total_digits = integer_digits + len(fractional_part)
    else:
        total_digits = len(unsigned_plain.lstrip('0'))

    if total_digits <= 7:
        return plain

    if abs(value) >= 1:
        return f"{float(value):.2f}"

    fixed = f"{float(value):.7f}".rstrip('0').rstrip('.')
    if fixed not in ('', '-0', '0'):
        return fixed
    return f"{float(value):.5e}"


def format_usd_value(num):
    if num is None:
        return 'N/A'

    value = float(num)
    abs_value = abs(value)
    if abs_value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"${value / 1_000:.2f}K"
    return f"${value:.2f}"


def _interval_to_ms(interval):
    if interval.endswith('m'):
        return int(interval[:-1]) * 60 * 1000
    if interval.endswith('h'):
        return int(interval[:-1]) * 60 * 60 * 1000
    if interval.endswith('d'):
        return int(interval[:-1]) * 24 * 60 * 60 * 1000
    raise ValueError(f'不支持的时间周期: {interval}')


_MAX_INTERVAL_MS = max(_interval_to_ms(interval) for interval in TIME_INTERVALS)
_REQUIRED_POINTS = (_MAX_INTERVAL_MS // FIVE_MINUTES_MS) + 1


def _empty_change():
    return {
        'ratio': None,
        'value_ratio': None,
        'open_interest': None,
        'open_interest_formatted': 'N/A',
        'open_interest_value': None,
        'open_interest_value_formatted': 'N/A',
        'price_change': None,
        'price_change_percent': None,
        'price_change_formatted': 'N/A',
        'current_price': None,
        'current_price_formatted': 'N/A',
    }


def _calc_percent_change(current_value, past_value):
    if current_value is None or past_value in (None, 0):
        return None
    return ((current_value - past_value) / past_value) * 100


def _calc_share_percent(value, total):
    if total in (None, 0):
        return None
    return float(value or 0) / float(total) * 100


def _get_exact_window(records_by_time, current_time, points, tolerance=10):
    window = []
    missing = 0
    for offset in range(points):
        record = records_by_time.get(current_time - offset * FIVE_MINUTES_MS)
        if record is None:
            missing += 1
            if missing > tolerance:
                return None
        else:
            window.append(record)
    return window


def _get_exact_window_by_step(records_by_time, current_time, points, step_ms, tolerance=10):
    window = []
    missing = 0
    for offset in range(points):
        record = records_by_time.get(current_time - offset * step_ms)
        if record is None:
            missing += 1
            if missing > tolerance:
                return None
        else:
            window.append(record)
    return window


def _calc_net_inflow_from_taker_vol(window, price=None):
    if not window:
        return 0
    buy_vol = sum(float(item.buy_vol or 0) for item in window)
    sell_vol = sum(float(item.sell_vol or 0) for item in window)
    return buy_vol - sell_vol


def _build_net_inflow_from_taker_vol(taker_vol_by_time, current_time):
    inflow = {}
    for interval in TIME_INTERVALS:
        points = _interval_to_ms(interval) // FIVE_MINUTES_MS
        window = _get_exact_window(taker_vol_by_time, current_time, points)
        if not window:
            continue

        inflow[interval] = _calc_net_inflow_from_taker_vol(window)
    return inflow


def _format_homepage_log_details(details):
    return json.dumps(details, ensure_ascii=False, sort_keys=True)


def _summarize_homepage_rejection_reasons(reasons):
    if not reasons:
        return 'unknown'

    buckets = {
        'missing_open_interest_history': False,
        'missing_kline_history': False,
        'missing_exchange_anchor': False,
        'unsupported_symbol': False,
        'missing_open_interest_target': [],
        'missing_kline_target': [],
    }

    for item in reasons:
        reason = item.get('reason')
        details = item.get('details') or {}
        if reason in ('missing_open_interest_target', 'missing_kline_target'):
            interval = details.get('interval')
            if interval:
                buckets[reason].append(interval)
        elif reason in buckets:
            buckets[reason] = True

    summary_parts = []
    if buckets['unsupported_symbol']:
        summary_parts.append('symbol_not_supported')
    if buckets['missing_open_interest_history']:
        summary_parts.append('missing_oi_history')
    if buckets['missing_kline_history']:
        summary_parts.append('missing_kline_history')
    if buckets['missing_exchange_anchor']:
        summary_parts.append('missing_anchor')
    if buckets['missing_open_interest_target']:
        summary_parts.append(f"missing_oi={','.join(buckets['missing_open_interest_target'])}")
    if buckets['missing_kline_target']:
        summary_parts.append(f"missing_kline={','.join(buckets['missing_kline_target'])}")

    if not summary_parts:
        summary_parts.append(','.join(sorted({item.get("reason", "unknown") for item in reasons})))
    return '; '.join(summary_parts)


def _compact_homepage_rejection_reasons(reasons):
    compact = []
    for item in reasons:
        reason = item.get('reason', 'unknown')
        details = item.get('details') or {}
        compact_item = {'reason': reason}
        if 'interval' in details:
            compact_item['interval'] = details['interval']
        if 'missing' in details:
            compact_item['missing'] = details['missing']
        if reason == 'unsupported_symbol':
            compact_item['symbol'] = details.get('symbol')
            compact_item['exchange'] = details.get('exchange')
        compact.append(compact_item)
    return compact


def _log_homepage_exchange_rejection(symbol, exchange, anchor_time, stage, reasons):
    if not reasons:
        return

    summary_reason = reasons[0].get('reason', 'homepage_exchange_rejected')
    summary = _summarize_homepage_rejection_reasons(reasons)
    logger.warning(
        '首页交易所门禁否决: '
        f'symbol={symbol} exchange={exchange} stage={stage} anchor_time={anchor_time} '
        f'reason={summary_reason} summary="{summary}" '
        f'details={_format_homepage_log_details(_compact_homepage_rejection_reasons(reasons))}'
    )


def _log_homepage_symbol_summary(symbol, included_exchanges, missing_exchanges, status, current_time):
    available = bool(included_exchanges)
    message = (
        '首页交易所聚合完成'
        if available
        else '首页交易所聚合为空态'
    )
    log_line = (
        f'{message}: symbol={symbol} current_time={current_time} status={status} '
        f'included_exchanges={_format_homepage_log_details(included_exchanges)} '
        f'missing_exchanges={_format_homepage_log_details(missing_exchanges)} '
        f'available={str(available).lower()}'
    )
    if available:
        logger.info(log_line)
    else:
        logger.warning(log_line)


def _collect_exchange_homepage_rejection_reasons(exchange, oi_by_time, kline_by_time, taker_maps_by_period, anchor_time):
    reasons = []

    if not oi_by_time:
        reasons.append(
            {
                'reason': 'missing_open_interest_history',
                'details': {'missing': 'open_interest_hist'},
            }
        )

    if not kline_by_time:
        reasons.append(
            {
                'reason': 'missing_kline_history',
                'details': {'missing': 'kline'},
            }
        )

    if anchor_time is None:
        if not reasons:
            reasons.append(
                {
                    'reason': 'missing_exchange_anchor',
                    'details': {'missing': 'common_time_anchor'},
                }
            )
        return reasons

    for interval in TIME_INTERVALS:
        target_time = anchor_time - _interval_to_ms(interval)
        if target_time not in oi_by_time:
            reasons.append(
                {
                    'reason': 'missing_open_interest_target',
                    'details': {
                        'interval': interval,
                        'target_time': target_time,
                    },
                }
            )
            continue

        if target_time not in kline_by_time:
            reasons.append(
                {
                    'reason': 'missing_kline_target',
                    'details': {
                        'interval': interval,
                        'target_time': target_time,
                    },
                }
            )
            continue

    return reasons


def _period_to_ms(period):
    if period.endswith('m'):
        return int(period[:-1]) * 60 * 1000
    if period.endswith('h') or period.endswith('H'):
        return int(period[:-1]) * 60 * 60 * 1000
    if period.endswith('d') or period.endswith('D'):
        return int(period[:-1]) * 24 * 60 * 60 * 1000
    raise ValueError(f'unsupported homepage period: {period}')


def _calc_net_inflow_for_period(taker_vol_by_time, current_time, interval, period):
    if not taker_vol_by_time:
        return None

    period_ms = _period_to_ms(period)
    interval_ms = _interval_to_ms(interval)
    points = interval_ms // period_ms
    if points <= 0:
        return None

    if period == '5m':
        period_current_time = current_time
        if points == 1 and period_current_time not in taker_vol_by_time:
            return None
    else:
        available_times = [event_time for event_time in taker_vol_by_time if event_time <= current_time]
        if not available_times:
            return None
        period_current_time = max(available_times)
    window = _get_exact_window_by_step(taker_vol_by_time, period_current_time, points, period_ms)
    if not window:
        return None
    return _calc_net_inflow_from_taker_vol(window)


def _has_required_change_coverage(oi_by_time, kline_by_time, current_time):
    for interval in TIME_INTERVALS:
        target_time = current_time - _interval_to_ms(interval)
        if target_time not in oi_by_time or target_time not in kline_by_time:
            return False
    return True


def _has_required_net_inflow_coverage(kline_by_time, current_time):
    for interval in TIME_INTERVALS:
        points = _interval_to_ms(interval) // FIVE_MINUTES_MS
        if _get_exact_window(kline_by_time, current_time, points) is None:
            return False
    return True


def _has_required_net_inflow_coverage_vol(taker_vol_by_time, current_time):
    for interval in TIME_INTERVALS:
        points = _interval_to_ms(interval) // FIVE_MINUTES_MS
        if _get_exact_window(taker_vol_by_time, current_time, points) is None:
            return False
    return True


def _has_complete_homepage_coverage(oi_by_time, kline_by_time):
    common_times = sorted(set(oi_by_time).intersection(kline_by_time))
    if not common_times:
        return False

    current_time = common_times[-1]
    return _has_required_change_coverage(oi_by_time, kline_by_time, current_time)


def _has_complete_homepage_coverage_full(oi_by_time, kline_by_time, taker_vol_by_time):
    common_times = sorted(set(oi_by_time).intersection(kline_by_time).intersection(taker_vol_by_time))
    if not common_times:
        return False

    current_time = common_times[-1]
    return _has_required_change_coverage(oi_by_time, kline_by_time, current_time) and _has_required_net_inflow_coverage_vol(
        taker_vol_by_time, current_time
    )


def _build_open_interest_point(row):
    return HomepageOpenInterestPoint(
        symbol=row.symbol,
        event_time=int(row.event_time),
        sum_open_interest=float(row.sum_open_interest) if row.sum_open_interest is not None else None,
        sum_open_interest_value=float(row.sum_open_interest_value) if row.sum_open_interest_value is not None else None,
    )


def _build_kline_point(row):
    return HomepageKlinePoint(
        symbol=row.symbol,
        open_time=int(row.open_time),
        high_price=float(row.high_price) if hasattr(row, 'high_price') and row.high_price is not None else None,
        low_price=float(row.low_price) if hasattr(row, 'low_price') and row.low_price is not None else None,
        close_price=float(row.close_price) if row.close_price is not None else None,
        quote_volume=float(row.quote_volume) if row.quote_volume is not None else None,
        taker_buy_quote_volume=float(row.taker_buy_quote_volume) if row.taker_buy_quote_volume is not None else None,
    )


def _build_taker_buy_sell_vol_point(row):
    return HomepageTakerBuySellVolPoint(
        symbol=row.symbol,
        event_time=int(row.event_time),
        buy_sell_ratio=float(row.buy_sell_ratio) if row.buy_sell_ratio is not None else None,
        buy_vol=float(row.buy_vol) if row.buy_vol is not None else None,
        sell_vol=float(row.sell_vol) if row.sell_vol is not None else None,
    )


def _get_enabled_exchanges():
    return _normalize_exchange_list(ENABLED_EXCHANGES)


def _merge_time_points(*points):
    existing_points = [point for point in points if point is not None]
    if not existing_points:
        return None

    symbol = existing_points[0].symbol
    event_time = existing_points[0].event_time
    total_open_interest = sum(float(point.sum_open_interest or 0) for point in existing_points)
    total_open_interest_value = sum(float(point.sum_open_interest_value or 0) for point in existing_points)
    return HomepageOpenInterestPoint(
        symbol=symbol,
        event_time=event_time,
        sum_open_interest=total_open_interest,
        sum_open_interest_value=total_open_interest_value,
    )


def _with_estimated_open_interest_value(point, reference_kline):
    if point is None:
        return None
    if reference_kline is None or reference_kline.close_price in (None, 0):
        return point
    price = float(reference_kline.close_price)
    if point.sum_open_interest_value in (None, 0) and point.sum_open_interest is not None:
        return HomepageOpenInterestPoint(
            symbol=point.symbol,
            event_time=point.event_time,
            sum_open_interest=point.sum_open_interest,
            sum_open_interest_value=float(point.sum_open_interest) * price,
        )
    if point.sum_open_interest in (None, 0) and point.sum_open_interest_value is not None:
        return HomepageOpenInterestPoint(
            symbol=point.symbol,
            event_time=point.event_time,
            sum_open_interest=float(point.sum_open_interest_value) / price,
            sum_open_interest_value=point.sum_open_interest_value,
        )
    return HomepageOpenInterestPoint(
        symbol=point.symbol,
        event_time=point.event_time,
        sum_open_interest=point.sum_open_interest,
        sum_open_interest_value=point.sum_open_interest_value,
    )


def _merge_taker_points(*points):
    existing_points = [point for point in points if point is not None]
    if not existing_points:
        return None

    symbol = existing_points[0].symbol
    event_time = existing_points[0].event_time
    total_buy_vol = sum(float(point.buy_vol or 0) for point in existing_points)
    total_sell_vol = sum(float(point.sell_vol or 0) for point in existing_points)
    ratio = None
    if total_sell_vol:
        ratio = total_buy_vol / total_sell_vol
    return HomepageTakerBuySellVolPoint(
        symbol=symbol,
        event_time=event_time,
        buy_sell_ratio=ratio,
        buy_vol=total_buy_vol,
        sell_vol=total_sell_vol,
    )


def _build_exchange_open_interest_rows(exchange_points, total_value, total_open_interest):
    rows = []
    for exchange, point in exchange_points.items():
        if point is None:
            continue

        open_interest = float(point.sum_open_interest or 0)
        open_interest_value = float(point.sum_open_interest_value or 0)
        rows.append(
            {
                'exchange': exchange,
                'open_interest': open_interest,
                'open_interest_formatted': format_number(open_interest),
                'open_interest_value': open_interest_value,
                'open_interest_value_formatted': format_usd_value(open_interest_value),
                'share_percent': _calc_share_percent(open_interest_value, total_value),
                'quantity_share_percent': _calc_share_percent(open_interest, total_open_interest),
            }
        )

    rows.sort(key=lambda item: item['open_interest_value'], reverse=True)
    return rows


def _build_exchange_status_rows(
    exchanges,
    supported_exchanges,
    symbol_exchange_snapshots,
    exchange_rejection_info,
    included_open_interest_map,
):
    rows = []
    for exchange in exchanges:
        snapshot = symbol_exchange_snapshots.get(exchange) or {}
        support_state = snapshot.get('support_state') or {'state': 'supported'}
        if exchange not in supported_exchanges:
            rows.append(
                {
                    'exchange': exchange,
                    'status': 'unsupported',
                    'open_interest': None,
                    'open_interest_formatted': 'N/A',
                    'open_interest_value': None,
                    'open_interest_value_formatted': 'N/A',
                    'share_percent': None,
                    'quantity_share_percent': None,
                }
            )
            continue

        if support_state.get('state') == 'unsupported':
            rows.append(
                {
                    'exchange': exchange,
                    'status': 'unsupported',
                    'open_interest': None,
                    'open_interest_formatted': 'N/A',
                    'open_interest_value': None,
                    'open_interest_value_formatted': 'N/A',
                    'share_percent': None,
                    'quantity_share_percent': None,
                }
            )
            continue

        included_row = included_open_interest_map.get(exchange)
        if included_row is not None:
            row = dict(included_row)
            row['exchange'] = exchange
            row['status'] = 'included'
            rows.append(row)
            continue

        current_time = snapshot.get('current_time')
        current_point = None
        if current_time is not None:
            current_point = _with_estimated_open_interest_value(
                snapshot.get('oi_by_time', {}).get(current_time),
                snapshot.get('kline_by_time', {}).get(current_time),
            )

        open_interest = float(current_point.sum_open_interest or 0) if current_point is not None else None
        open_interest_value = float(current_point.sum_open_interest_value or 0) if current_point is not None else None
        row_status = 'excluded'
        if support_state.get('state') == 'unknown':
            row_status = 'unknown'
        row = {
            'exchange': exchange,
            'status': row_status,
            'open_interest': open_interest,
            'open_interest_formatted': format_number(open_interest) if open_interest is not None else 'N/A',
            'open_interest_value': open_interest_value,
            'open_interest_value_formatted': format_usd_value(open_interest_value),
            'share_percent': None,
            'quantity_share_percent': None,
        }
        rejection = exchange_rejection_info.get(exchange)
        if rejection:
            row['reason'] = rejection.get('reasons') or []
            row['stage'] = rejection.get('stage')
        if support_state.get('state') == 'unknown':
            row['support_state'] = 'unknown'
        rows.append(row)

    rows.sort(
        key=lambda item: (
            {'included': 0, 'excluded': 1, 'unknown': 2, 'unsupported': 3}.get(item.get('status'), 4),
            -(item.get('open_interest_value') or 0),
            item.get('exchange') or '',
        )
    )
    return rows


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


def _get_exchange_common_time(oi_by_time, kline_by_time):
    common_times = sorted(set(oi_by_time).intersection(kline_by_time))
    if not common_times:
        return None
    return common_times[-1]


def _exchange_supports_homepage_anchor(exchange, oi_by_time, kline_by_time, taker_maps_by_period, anchor_time):
    return not _collect_exchange_homepage_rejection_reasons(
        exchange,
        oi_by_time,
        kline_by_time,
        taker_maps_by_period,
        anchor_time,
    )


def _build_exchange_homepage_snapshot(exchange, oi_by_time, kline_by_time, taker_maps_by_period):
    current_time = _get_exchange_common_time(oi_by_time, kline_by_time)
    if current_time is None:
        return {
            'complete': False,
            'current_time': None,
            'reasons': _collect_exchange_homepage_rejection_reasons(
                exchange,
                oi_by_time,
                kline_by_time,
                taker_maps_by_period,
                None,
            ),
        }

    reasons = _collect_exchange_homepage_rejection_reasons(
        exchange,
        oi_by_time,
        kline_by_time,
        taker_maps_by_period,
        current_time,
    )
    complete = not reasons
    return {
        'complete': complete,
        'current_time': current_time,
        'reasons': reasons,
    }


def _get_latest_series_time_map(session, model, symbols, time_field_name, upper_bound=None):
    if not symbols:
        return {}

    time_field = getattr(model, time_field_name)
    query = session.query(model.symbol, func.max(time_field)).filter(
        model.symbol.in_(symbols),
        model.period == '5m',
    )
    if upper_bound is not None:
        query = query.filter(time_field <= upper_bound)
    rows = query.group_by(model.symbol).all()
    return {symbol: int(latest_time) for symbol, latest_time in rows if latest_time is not None}


def _build_homepage_lower_bounds(session, symbols, upper_bound=None):
    latest_oi_time_map = _get_latest_series_time_map(
        session=session,
        model=BinanceOpenInterestHist,
        symbols=symbols,
        time_field_name='event_time',
        upper_bound=upper_bound,
    )
    latest_kline_time_map = _get_latest_series_time_map(
        session=session,
        model=BinanceKline,
        symbols=symbols,
        time_field_name='open_time',
        upper_bound=upper_bound,
    )
    latest_taker_vol_time_map = _get_latest_series_time_map(
        session=session,
        model=BinanceTakerBuySellVol,
        symbols=symbols,
        time_field_name='event_time',
        upper_bound=upper_bound,
    )

    lower_bounds = {}
    for symbol in symbols:
        latest_oi_time = latest_oi_time_map.get(symbol)
        latest_kline_time = latest_kline_time_map.get(symbol)
        latest_taker_vol_time = latest_taker_vol_time_map.get(symbol)
        if latest_oi_time is None or latest_kline_time is None or latest_taker_vol_time is None:
            continue

        current_time = min(latest_oi_time, latest_kline_time, latest_taker_vol_time)
        lower_bounds[symbol] = current_time - _MAX_INTERVAL_MS

    return lower_bounds


def _load_recent_series_map(session, model, symbols, time_field_name, lower_bounds=None, upper_bound=None):
    if not symbols:
        return {}

    time_field = getattr(model, time_field_name)
    effective_lower_bounds = lower_bounds or {}
    conditions = []
    for symbol in symbols:
        lower_bound = effective_lower_bounds.get(symbol)
        if lower_bound is None:
            conditions.append(model.symbol == symbol)
        else:
            conditions.append(and_(model.symbol == symbol, time_field >= lower_bound))

    builder = _build_open_interest_point if model is BinanceOpenInterestHist else _build_kline_point
    selected_columns = (
        (
            BinanceOpenInterestHist.symbol,
            BinanceOpenInterestHist.event_time,
            BinanceOpenInterestHist.sum_open_interest,
            BinanceOpenInterestHist.sum_open_interest_value,
        )
        if model is BinanceOpenInterestHist
        else (
            BinanceKline.symbol,
            BinanceKline.open_time,
            BinanceKline.close_price,
            BinanceKline.quote_volume,
            BinanceKline.taker_buy_quote_volume,
        )
    )

    query = session.query(*selected_columns).filter(
        model.period == '5m',
        or_(*conditions),
    )
    if upper_bound is not None:
        query = query.filter(time_field <= upper_bound)
    rows = query.all()

    records_by_symbol = {symbol: {} for symbol in symbols}
    for row in rows:
        point = builder(row)
        records_by_symbol.setdefault(point.symbol, {})[int(getattr(point, time_field_name))] = point
    return records_by_symbol


def _load_recent_open_interest_map(session, symbols, lower_bounds=None, upper_bound=None):
    return _load_recent_series_map(
        session=session,
        model=BinanceOpenInterestHist,
        symbols=symbols,
        time_field_name='event_time',
        lower_bounds=lower_bounds,
        upper_bound=upper_bound,
    )


def _load_recent_klines_map(session, symbols, lower_bounds=None, upper_bound=None):
    return _load_recent_series_map(
        session=session,
        model=BinanceKline,
        symbols=symbols,
        time_field_name='open_time',
        lower_bounds=lower_bounds,
        upper_bound=upper_bound,
    )


def _load_recent_open_interest(session, symbol, upper_bound=None):
    query = session.query(
        BinanceOpenInterestHist.symbol,
        BinanceOpenInterestHist.event_time,
        BinanceOpenInterestHist.sum_open_interest,
        BinanceOpenInterestHist.sum_open_interest_value,
    ).filter(
        BinanceOpenInterestHist.symbol == symbol,
        BinanceOpenInterestHist.period == '5m',
    )
    if upper_bound is not None:
        query = query.filter(BinanceOpenInterestHist.event_time <= upper_bound)
    rows = query.order_by(BinanceOpenInterestHist.event_time.desc()).limit(_REQUIRED_POINTS).all()
    return {int(row.event_time): _build_open_interest_point(row) for row in rows}


def _load_recent_klines(session, symbol, upper_bound=None):
    query = session.query(
        BinanceKline.symbol,
        BinanceKline.open_time,
        BinanceKline.high_price,
        BinanceKline.low_price,
        BinanceKline.close_price,
        BinanceKline.quote_volume,
        BinanceKline.taker_buy_quote_volume,
    ).filter(
        BinanceKline.symbol == symbol,
        BinanceKline.period == '5m',
    )
    if upper_bound is not None:
        query = query.filter(BinanceKline.open_time <= upper_bound)
    rows = query.order_by(BinanceKline.open_time.desc()).limit(_REQUIRED_POINTS).all()
    return {int(row.open_time): _build_kline_point(row) for row in rows}


def _load_recent_taker_buy_sell_vol_map(session, symbols, lower_bounds=None, upper_bound=None):
    return _load_recent_series_map(
        session=session,
        model=BinanceTakerBuySellVol,
        symbols=symbols,
        time_field_name='event_time',
        lower_bounds=lower_bounds,
        upper_bound=upper_bound,
    )


def _load_recent_taker_buy_sell_vol(session, symbol, upper_bound=None):
    query = session.query(
        BinanceTakerBuySellVol.symbol,
        BinanceTakerBuySellVol.event_time,
        BinanceTakerBuySellVol.buy_sell_ratio,
        BinanceTakerBuySellVol.buy_vol,
        BinanceTakerBuySellVol.sell_vol,
    ).filter(
        BinanceTakerBuySellVol.symbol == symbol,
        BinanceTakerBuySellVol.period == '5m',
    )
    if upper_bound is not None:
        query = query.filter(BinanceTakerBuySellVol.event_time <= upper_bound)
    rows = query.order_by(BinanceTakerBuySellVol.event_time.desc()).limit(_REQUIRED_POINTS).all()
    return {int(row.event_time): _build_taker_buy_sell_vol_point(row) for row in rows}


def _get_recent_lower_bound(session, model, symbols, time_field_name, upper_bound=None, exchange=None, period='5m'):
    time_field = getattr(model, time_field_name)
    query = session.query(func.max(time_field)).filter(
        model.symbol.in_(symbols),
        model.period == period,
    )
    if hasattr(model, 'exchange') and exchange is not None:
        query = query.filter(model.exchange == exchange)
    if upper_bound is not None:
        query = query.filter(time_field <= upper_bound)

    latest_time = query.scalar()
    if latest_time is None:
        return None
    return max(0, int(latest_time) - _MAX_INTERVAL_MS)


def _build_change_target_times(current_times):
    target_times = set()
    for current_time in current_times:
        if current_time is None:
            continue
        current_time = int(current_time)
        target_times.add(current_time)
        for interval in TIME_INTERVALS:
            target_times.add(current_time - _interval_to_ms(interval))
    return {timestamp for timestamp in target_times if timestamp >= 0}


def _get_recent_time_candidates(session, model, symbol, time_field_name, upper_bound=None, exchange=None, limit=12):
    time_field = getattr(model, time_field_name)
    query = session.query(time_field).filter(
        model.symbol == symbol,
        model.period == '5m',
    )
    if hasattr(model, 'exchange') and exchange is not None:
        query = query.filter(model.exchange == exchange)
    if upper_bound is not None:
        query = query.filter(time_field <= upper_bound)
    return [int(row[0]) for row in query.order_by(time_field.desc()).limit(limit).all() if row[0] is not None]


def _load_open_interest_model_map(session, model, symbols, upper_bound=None, exchange=None):
    if not symbols:
        return {}

    if len(symbols) <= HOMEPAGE_BULK_QUERY_THRESHOLD:
        records_by_symbol = {symbol: {} for symbol in symbols}
        for symbol in symbols:
            target_times = _build_change_target_times(
                _get_recent_time_candidates(
                    session,
                    model,
                    symbol,
                    'event_time',
                    upper_bound=upper_bound,
                    exchange=exchange,
                )
            )
            if not target_times:
                continue
            query = session.query(
                model.symbol,
                model.event_time,
                model.sum_open_interest,
                model.sum_open_interest_value,
            ).filter(
                model.symbol == symbol,
                model.period == '5m',
            )
            if hasattr(model, 'exchange') and exchange is not None:
                query = query.filter(model.exchange == exchange)
            if upper_bound is not None:
                query = query.filter(model.event_time <= upper_bound)
            query = query.filter(model.event_time.in_(target_times))

            rows = query.all()
            for row in rows:
                point = _build_open_interest_point(row)
                records_by_symbol.setdefault(point.symbol, {})[point.event_time] = point
        return records_by_symbol

    lower_bound = _get_recent_lower_bound(
        session=session,
        model=model,
        symbols=symbols,
        time_field_name='event_time',
        upper_bound=upper_bound,
        exchange=exchange,
    )
    query = session.query(
        model.symbol,
        model.event_time,
        model.sum_open_interest,
        model.sum_open_interest_value,
    ).filter(model.symbol.in_(symbols), model.period == '5m')
    if hasattr(model, 'exchange') and exchange is not None:
        query = query.filter(model.exchange == exchange)
    if upper_bound is not None:
        query = query.filter(model.event_time <= upper_bound)
    if lower_bound is not None:
        query = query.filter(model.event_time >= lower_bound)

    records_by_symbol = {symbol: {} for symbol in symbols}
    for row in query.all():
        point = _build_open_interest_point(row)
        records_by_symbol.setdefault(point.symbol, {})[point.event_time] = point
    return records_by_symbol


def _load_kline_model_map(session, model, symbols, upper_bound=None, exchange=None):
    if not symbols:
        return {}

    if len(symbols) <= HOMEPAGE_BULK_QUERY_THRESHOLD:
        records_by_symbol = {symbol: {} for symbol in symbols}
        for symbol in symbols:
            target_times = _build_change_target_times(
                _get_recent_time_candidates(
                    session,
                    model,
                    symbol,
                    'open_time',
                    upper_bound=upper_bound,
                    exchange=exchange,
                )
            )
            if not target_times:
                continue
            query = session.query(
                model.symbol,
                model.open_time,
                model.close_price,
                model.quote_volume,
                model.taker_buy_quote_volume,
            ).filter(
                model.symbol == symbol,
                model.period == '5m',
            )
            if hasattr(model, 'exchange') and exchange is not None:
                query = query.filter(model.exchange == exchange)
            if upper_bound is not None:
                query = query.filter(model.open_time <= upper_bound)
            query = query.filter(model.open_time.in_(target_times))

            rows = query.all()
            for row in rows:
                point = _build_kline_point(row)
                records_by_symbol.setdefault(point.symbol, {})[point.open_time] = point
        return records_by_symbol

    lower_bound = _get_recent_lower_bound(
        session=session,
        model=model,
        symbols=symbols,
        time_field_name='open_time',
        upper_bound=upper_bound,
        exchange=exchange,
    )
    query = session.query(
        model.symbol,
        model.open_time,
        model.high_price,
        model.low_price,
        model.close_price,
        model.quote_volume,
        model.taker_buy_quote_volume,
    ).filter(model.symbol.in_(symbols), model.period == '5m')
    if hasattr(model, 'exchange') and exchange is not None:
        query = query.filter(model.exchange == exchange)
    if upper_bound is not None:
        query = query.filter(model.open_time <= upper_bound)
    if lower_bound is not None:
        query = query.filter(model.open_time >= lower_bound)

    records_by_symbol = {symbol: {} for symbol in symbols}
    for row in query.all():
        point = _build_kline_point(row)
        records_by_symbol.setdefault(point.symbol, {})[point.open_time] = point
    return records_by_symbol


def _load_taker_vol_model_map(session, model, symbols, upper_bound=None, exchange=None, period='5m'):
    if not symbols:
        return {}

    if len(symbols) <= HOMEPAGE_BULK_QUERY_THRESHOLD:
        records_by_symbol = {symbol: {} for symbol in symbols}
        for symbol in symbols:
            query = session.query(
                model.symbol,
                model.event_time,
                model.buy_sell_ratio,
                model.buy_vol,
                model.sell_vol,
            ).filter(
                model.symbol == symbol,
                model.period == period,
            )
            if hasattr(model, 'exchange') and exchange is not None:
                query = query.filter(model.exchange == exchange)
            if upper_bound is not None:
                query = query.filter(model.event_time <= upper_bound)

            rows = query.order_by(model.event_time.desc()).limit(_REQUIRED_POINTS).all()
            for row in rows:
                point = _build_taker_buy_sell_vol_point(row)
                records_by_symbol.setdefault(point.symbol, {})[point.event_time] = point
        return records_by_symbol

    lower_bound = _get_recent_lower_bound(
        session=session,
        model=model,
        symbols=symbols,
        time_field_name='event_time',
        upper_bound=upper_bound,
        exchange=exchange,
        period=period,
    )
    query = session.query(
        model.symbol,
        model.event_time,
        model.buy_sell_ratio,
        model.buy_vol,
        model.sell_vol,
    ).filter(model.symbol.in_(symbols), model.period == period)
    if hasattr(model, 'exchange') and exchange is not None:
        query = query.filter(model.exchange == exchange)
    if upper_bound is not None:
        query = query.filter(model.event_time <= upper_bound)
    if lower_bound is not None:
        query = query.filter(model.event_time >= lower_bound)

    records_by_symbol = {symbol: {} for symbol in symbols}
    for row in query.all():
        point = _build_taker_buy_sell_vol_point(row)
        records_by_symbol.setdefault(point.symbol, {})[point.event_time] = point
    return records_by_symbol


def _load_exchange_homepage_maps(session, exchange, symbols, upper_bound=None):
    exchange = exchange.lower()
    try:
        adapter = get_exchange_adapter(exchange)
        taker_periods = sorted(
            {
                adapter.taker_period_for_interval(interval)
                for interval in TIME_INTERVALS
                if adapter.taker_period_for_interval(interval)
            }
        )
    except Exception:
        adapter = None
        taker_periods = ['5m']

    start_time = __import__('time').perf_counter()
    logger.info('首页映射加载开始: exchange=%s symbols=%d', exchange, len(symbols))

    oi_start = __import__('time').perf_counter()
    oi_map = _load_open_interest_model_map(
        session,
        MarketOpenInterestHist,
        symbols,
        upper_bound=upper_bound,
        exchange=exchange,
    )
    logger.info(
        '首页映射 OI 完成: exchange=%s symbols=%d 耗时=%.2fs',
        exchange,
        len(symbols),
        __import__('time').perf_counter() - oi_start,
    )

    kline_start = __import__('time').perf_counter()
    kline_map = _load_kline_model_map(
        session,
        MarketKline,
        symbols,
        upper_bound=upper_bound,
        exchange=exchange,
    )
    logger.info(
        '首页映射 Kline 完成: exchange=%s symbols=%d 耗时=%.2fs',
        exchange,
        len(symbols),
        __import__('time').perf_counter() - kline_start,
    )

    taker_start = __import__('time').perf_counter()
    taker_maps_by_period = {
        period: _load_taker_vol_model_map(
            session,
            MarketTakerBuySellVol,
            symbols,
            upper_bound=upper_bound,
            exchange=exchange,
            period=period,
        )
        for period in taker_periods
        }
    logger.info(
        '首页映射 Taker 完成: exchange=%s periods=%d symbols=%d 耗时=%.2fs',
        exchange,
        len(taker_maps_by_period),
        len(symbols),
        __import__('time').perf_counter() - taker_start,
    )

    if exchange == 'binance':
        legacy_start = __import__('time').perf_counter()
        missing_oi_symbols = [symbol for symbol in symbols if not oi_map.get(symbol)]
        missing_kline_symbols = [symbol for symbol in symbols if not kline_map.get(symbol)]
        missing_taker_symbols = [
            symbol
            for symbol in symbols
            if not (taker_maps_by_period.get('5m') or {}).get(symbol)
        ]

        if missing_oi_symbols:
            legacy_oi_map = _load_open_interest_model_map(
                session,
                BinanceOpenInterestHist,
                missing_oi_symbols,
                upper_bound=upper_bound,
            )
            for symbol in missing_oi_symbols:
                oi_map[symbol] = legacy_oi_map.get(symbol, {})

        if missing_kline_symbols:
            legacy_kline_map = _load_kline_model_map(
                session,
                BinanceKline,
                missing_kline_symbols,
                upper_bound=upper_bound,
            )
            for symbol in missing_kline_symbols:
                kline_map[symbol] = legacy_kline_map.get(symbol, {})

        if missing_taker_symbols:
            legacy_taker_map = _load_taker_vol_model_map(
                session,
                BinanceTakerBuySellVol,
                missing_taker_symbols,
                upper_bound=upper_bound,
                period='5m',
            )
            taker_maps_by_period.setdefault('5m', {})
            for symbol in missing_taker_symbols:
                taker_maps_by_period['5m'][symbol] = legacy_taker_map.get(symbol, {})

        logger.info(
            '首页映射 Binance 兼容加载完成: exchange=%s missing_oi=%d missing_kline=%d missing_taker=%d 耗时=%.2fs',
            exchange,
            len(missing_oi_symbols),
            len(missing_kline_symbols),
            len(missing_taker_symbols),
            __import__('time').perf_counter() - legacy_start,
        )

    logger.info('首页映射加载完成: exchange=%s 耗时=%.2fs', exchange, __import__('time').perf_counter() - start_time)
    return oi_map, kline_map, taker_maps_by_period


def _aggregate_homepage_series_maps(session, symbols, upper_bound=None):
    exchanges = _get_enabled_exchanges()
    supported_exchanges = set(_normalize_exchange_list(get_supported_exchange_ids()))
    exchange_maps = {}
    exchange_adapters = {}
    for exchange in exchanges:
        if exchange not in supported_exchanges:
            exchange_maps[exchange] = ({symbol: {} for symbol in symbols}, {symbol: {} for symbol in symbols}, {})
            exchange_adapters[exchange] = None
            continue
        exchange_maps[exchange] = _load_exchange_homepage_maps(session, exchange, symbols, upper_bound=upper_bound)
        try:
            exchange_adapters[exchange] = get_exchange_adapter(exchange)
        except Exception:
            exchange_adapters[exchange] = None

    primary_exchange = PRIMARY_PRICE_EXCHANGE.lower()

    aggregate_oi_map = {symbol: {} for symbol in symbols}
    coverage_map = {
        symbol: {
            'source_exchanges': [],
            'included_exchanges': [],
            'missing_exchanges': [],
            'open_interest_by_exchange': {},
            'net_inflow': {},
            'status': 'empty',
        }
        for symbol in symbols
    }
    selected_kline_map = {symbol: {} for symbol in symbols}

    for symbol in symbols:
        symbol_exchange_snapshots = {}
        exchange_rejection_info = {}
        for exchange, (oi_map, _kline_map, taker_maps_by_period) in exchange_maps.items():
            if exchange not in supported_exchanges:
                symbol_exchange_snapshots[exchange] = {
                    'current_time': None,
                    'oi_by_time': {},
                    'kline_by_time': {},
                    'taker_maps_by_period': {},
                    'complete': False,
                    'unsupported': True,
                    'support_state': {'state': 'unsupported'},
                    'reasons': [
                        {
                            'reason': 'unsupported_exchange',
                            'details': {'exchange': exchange},
                        }
                    ],
                }
                coverage_map[symbol]['missing_exchanges'].append(exchange)
                exchange_rejection_info[exchange] = {
                    'stage': 'unsupported',
                    'anchor_time': None,
                    'reasons': symbol_exchange_snapshots[exchange]['reasons'],
                }
                continue

            adapter = exchange_adapters.get(exchange)
            support_state = {'state': 'supported', 'supported': True, 'known': True}
            if adapter is not None and hasattr(adapter, 'symbol_support_state'):
                support_state = adapter.symbol_support_state(symbol, session=session)

            if support_state.get('state') == 'unsupported':
                symbol_exchange_snapshots[exchange] = {
                    'current_time': None,
                    'oi_by_time': {},
                    'kline_by_time': {},
                    'taker_maps_by_period': {},
                    'complete': False,
                    'unsupported': True,
                    'support_state': support_state,
                    'reasons': [
                        {
                            'reason': 'unsupported_symbol',
                            'details': {
                                'exchange': exchange,
                                'symbol': symbol,
                            },
                        }
                    ],
                }
                coverage_map[symbol]['missing_exchanges'].append(exchange)
                exchange_rejection_info[exchange] = {
                    'stage': 'unsupported_symbol',
                    'anchor_time': None,
                    'reasons': symbol_exchange_snapshots[exchange]['reasons'],
                }
                continue

            symbol_oi = oi_map.get(symbol, {})
            symbol_kline = _kline_map.get(symbol, {})
            symbol_taker = {
                period: period_map.get(symbol, {})
                for period, period_map in (taker_maps_by_period or {}).items()
            }
            if symbol_oi or symbol_kline or any(symbol_taker.values()):
                snapshot = _build_exchange_homepage_snapshot(
                    exchange=exchange,
                    oi_by_time=symbol_oi,
                    kline_by_time=symbol_kline,
                    taker_maps_by_period=symbol_taker,
                )
                symbol_exchange_snapshots[exchange] = {
                    'current_time': snapshot['current_time'],
                    'oi_by_time': symbol_oi,
                    'kline_by_time': symbol_kline,
                    'taker_maps_by_period': symbol_taker,
                    'complete': snapshot['complete'],
                    'unsupported': False,
                    'support_state': support_state,
                    'reasons': snapshot.get('reasons') or _collect_exchange_homepage_rejection_reasons(
                        exchange,
                        symbol_oi,
                        symbol_kline,
                        symbol_taker,
                        snapshot.get('current_time'),
                    ),
                }
                if snapshot['complete']:
                    coverage_map[symbol]['source_exchanges'].append(exchange)
                else:
                    coverage_map[symbol]['missing_exchanges'].append(exchange)
                    exchange_rejection_info[exchange] = {
                        'stage': 'initial_snapshot',
                        'anchor_time': snapshot.get('current_time'),
                        'reasons': symbol_exchange_snapshots[exchange]['reasons'],
                    }
            else:
                symbol_exchange_snapshots[exchange] = {
                    'current_time': None,
                    'oi_by_time': symbol_oi,
                    'kline_by_time': symbol_kline,
                    'taker_maps_by_period': symbol_taker,
                    'complete': False,
                    'unsupported': False,
                    'support_state': support_state,
                    'reasons': _collect_exchange_homepage_rejection_reasons(
                        exchange,
                        symbol_oi,
                        symbol_kline,
                        symbol_taker,
                        None,
                    ),
                }
                coverage_map[symbol]['missing_exchanges'].append(exchange)
                exchange_rejection_info[exchange] = {
                    'stage': 'initial_snapshot',
                    'anchor_time': None,
                    'reasons': symbol_exchange_snapshots[exchange]['reasons'],
                }

        included_exchanges = [
            exchange
            for exchange, snapshot in symbol_exchange_snapshots.items()
            if snapshot.get('complete')
        ]
        if not included_exchanges:
            coverage_map[symbol]['status'] = 'empty'
            coverage_map[symbol]['exchange_statuses'] = _build_exchange_status_rows(
                exchanges,
                supported_exchanges,
                symbol_exchange_snapshots,
                exchange_rejection_info,
                {},
            )
            for exchange, rejection in exchange_rejection_info.items():
                _log_homepage_exchange_rejection(
                    symbol=symbol,
                    exchange=exchange,
                    anchor_time=rejection.get('anchor_time'),
                    stage=rejection.get('stage', 'initial_snapshot'),
                    reasons=rejection.get('reasons') or [],
                )
            _log_homepage_symbol_summary(
                symbol=symbol,
                included_exchanges=[],
                missing_exchanges=_normalize_exchange_list(coverage_map[symbol]['missing_exchanges']),
                status='empty',
                current_time=None,
            )
            continue

        anchor_candidates = [
            snapshot['current_time']
            for snapshot in symbol_exchange_snapshots.values()
            if snapshot.get('complete') and snapshot.get('current_time') is not None
        ]
        if not anchor_candidates:
            coverage_map[symbol]['status'] = 'empty'
            coverage_map[symbol]['exchange_statuses'] = _build_exchange_status_rows(
                exchanges,
                supported_exchanges,
                symbol_exchange_snapshots,
                exchange_rejection_info,
                {},
            )
            for exchange, rejection in exchange_rejection_info.items():
                _log_homepage_exchange_rejection(
                    symbol=symbol,
                    exchange=exchange,
                    anchor_time=rejection.get('anchor_time'),
                    stage=rejection.get('stage', 'initial_snapshot'),
                    reasons=rejection.get('reasons') or [],
                )
            _log_homepage_symbol_summary(
                symbol=symbol,
                included_exchanges=[],
                missing_exchanges=_normalize_exchange_list(coverage_map[symbol]['missing_exchanges']),
                status='empty',
                current_time=None,
            )
            continue

        anchor_time = min(anchor_candidates)
        stable = False
        while not stable:
            included_exchanges = []
            for exchange, snapshot in symbol_exchange_snapshots.items():
                if _exchange_supports_homepage_anchor(
                    exchange=exchange,
                    oi_by_time=snapshot['oi_by_time'],
                    kline_by_time=snapshot['kline_by_time'],
                    taker_maps_by_period=snapshot['taker_maps_by_period'],
                    anchor_time=anchor_time,
                ):
                    included_exchanges.append(exchange)
                else:
                    coverage_map[symbol]['missing_exchanges'].append(exchange)
                    exchange_rejection_info[exchange] = {
                        'stage': 'anchor_validation',
                        'anchor_time': anchor_time,
                        'reasons': _collect_exchange_homepage_rejection_reasons(
                            exchange,
                            snapshot['oi_by_time'],
                            snapshot['kline_by_time'],
                            snapshot['taker_maps_by_period'],
                            anchor_time,
                        ),
                    }

            included_exchanges = _normalize_exchange_list(included_exchanges)
            if not included_exchanges:
                anchor_time = None
                break

            new_anchor_time = min(symbol_exchange_snapshots[exchange]['current_time'] for exchange in included_exchanges)
            stable = new_anchor_time == anchor_time
            anchor_time = new_anchor_time

        if anchor_time is None or not included_exchanges:
            coverage_map[symbol]['status'] = 'empty'
            coverage_map[symbol]['included_exchanges'] = []
            coverage_map[symbol]['source_exchanges'] = []
            coverage_map[symbol]['missing_exchanges'] = _normalize_exchange_list(coverage_map[symbol]['missing_exchanges'])
            coverage_map[symbol]['exchange_statuses'] = _build_exchange_status_rows(
                exchanges,
                supported_exchanges,
                symbol_exchange_snapshots,
                exchange_rejection_info,
                {},
            )
            for exchange, rejection in exchange_rejection_info.items():
                _log_homepage_exchange_rejection(
                    symbol=symbol,
                    exchange=exchange,
                    anchor_time=rejection.get('anchor_time'),
                    stage=rejection.get('stage', 'initial_snapshot'),
                    reasons=rejection.get('reasons') or [],
                )
            _log_homepage_symbol_summary(
                symbol=symbol,
                included_exchanges=[],
                missing_exchanges=coverage_map[symbol]['missing_exchanges'],
                status='empty',
                current_time=None,
            )
            continue

        missing_exchanges = [exchange for exchange in exchanges if exchange not in included_exchanges]
        coverage_map[symbol]['included_exchanges'] = included_exchanges
        coverage_map[symbol]['source_exchanges'] = included_exchanges
        coverage_map[symbol]['missing_exchanges'] = _normalize_exchange_list(
            [*coverage_map[symbol]['missing_exchanges'], *missing_exchanges]
        )
        coverage_map[symbol]['status'] = 'complete' if not coverage_map[symbol]['missing_exchanges'] else 'partial'
        for exchange in coverage_map[symbol]['missing_exchanges']:
            rejection = exchange_rejection_info.get(exchange)
            if rejection:
                _log_homepage_exchange_rejection(
                    symbol=symbol,
                    exchange=exchange,
                    anchor_time=rejection.get('anchor_time'),
                    stage=rejection.get('stage', 'initial_snapshot'),
                    reasons=rejection.get('reasons') or [],
                )
        _log_homepage_symbol_summary(
            symbol=symbol,
            included_exchanges=included_exchanges,
            missing_exchanges=coverage_map[symbol]['missing_exchanges'],
            status=coverage_map[symbol]['status'],
            current_time=anchor_time,
        )

        price_exchange = primary_exchange if primary_exchange in included_exchanges else included_exchanges[0]
        selected_kline_map[symbol] = exchange_maps[price_exchange][1].get(symbol, {})

        oi_times = sorted(
            set().union(*(snapshot['oi_by_time'].keys() for snapshot in symbol_exchange_snapshots.values()))
        )
        for event_time in oi_times:
            exchange_points = {}
            for exchange in included_exchanges:
                snapshot = symbol_exchange_snapshots[exchange]
                point = _with_estimated_open_interest_value(
                    snapshot['oi_by_time'].get(event_time),
                    selected_kline_map[symbol].get(event_time),
                )
                if point is not None:
                    exchange_points[exchange] = point

            merged_point = _merge_time_points(*exchange_points.values())
            if merged_point is not None:
                coverage_map[symbol]['open_interest_by_exchange'][event_time] = exchange_points
                aggregate_oi_map[symbol][event_time] = merged_point

        for interval in TIME_INTERVALS:
            interval_values = []
            for exchange in included_exchanges:
                snapshot = symbol_exchange_snapshots[exchange]
                adapter = exchange_adapters.get(exchange)
                if adapter is None:
                    continue
                period = adapter.taker_period_for_interval(interval)
                if not period:
                    continue
                taker_by_time = (snapshot['taker_maps_by_period'] or {}).get(period, {})
                inflow = _calc_net_inflow_for_period(taker_by_time, anchor_time, interval, period)
                if inflow is not None:
                    interval_values.append(inflow)

            if interval_values:
                coverage_map[symbol]['net_inflow'][interval] = sum(interval_values)

        included_open_interest_rows = _build_exchange_open_interest_rows(
            coverage_map[symbol]['open_interest_by_exchange'].get(anchor_time, {}),
            aggregate_oi_map[symbol][anchor_time].sum_open_interest_value if anchor_time in aggregate_oi_map[symbol] else 0,
            aggregate_oi_map[symbol][anchor_time].sum_open_interest if anchor_time in aggregate_oi_map[symbol] else 0,
        )
        coverage_map[symbol]['exchange_statuses'] = _build_exchange_status_rows(
            exchanges,
            supported_exchanges,
            symbol_exchange_snapshots,
            exchange_rejection_info,
            {item['exchange']: item for item in included_open_interest_rows},
        )

    return aggregate_oi_map, selected_kline_map, {}, coverage_map


def _load_homepage_series_maps(session, symbols, upper_bound=None):
    return _aggregate_homepage_series_maps(session, symbols, upper_bound=upper_bound)


def _build_coin_payload(symbol, oi_by_time, kline_by_time, taker_vol_by_time, coverage=None):
    common_times = sorted(set(oi_by_time).intersection(kline_by_time))
    oi_times = sorted(oi_by_time)
    included_exchanges = list((coverage or {}).get('included_exchanges') or (coverage or {}).get('source_exchanges') or [])
    missing_exchanges = list((coverage or {}).get('missing_exchanges') or [])
    status = (coverage or {}).get('status')
    if status not in ('complete', 'partial', 'empty'):
        status = 'complete' if included_exchanges and not missing_exchanges else ('partial' if included_exchanges else 'empty')

    if not common_times and not oi_times:
        empty_changes = {interval: _empty_change() for interval in TIME_INTERVALS}
        return {
            'symbol': symbol,
            'source_exchanges': included_exchanges,
            'included_exchanges': included_exchanges,
            'missing_exchanges': missing_exchanges,
            'status': 'empty',
            'exchange_open_interest': [],
            'exchange_statuses': list((coverage or {}).get('exchange_statuses') or []),
            'current_open_interest': None,
            'current_open_interest_formatted': 'N/A',
            'current_open_interest_value': None,
            'current_open_interest_value_formatted': 'N/A',
            'current_price': None,
            'current_price_formatted': 'N/A',
            'price_change': None,
            'price_change_percent': None,
            'price_change_formatted': 'N/A',
            'net_inflow': {},
            'changes': empty_changes,
            'current_time': None,
        }

    current_time = common_times[-1] if common_times else oi_times[-1]
    net_inflow = dict((coverage or {}).get('net_inflow') or {})

    if not net_inflow and taker_vol_by_time and any(taker_vol_by_time.values()):
        net_inflow = _build_net_inflow_from_taker_vol(taker_vol_by_time, current_time)

    current_oi = oi_by_time.get(current_time)
    if current_oi is None:
        empty_changes = {interval: _empty_change() for interval in TIME_INTERVALS}
        return {
            'symbol': symbol,
            'source_exchanges': included_exchanges,
            'included_exchanges': included_exchanges,
            'missing_exchanges': missing_exchanges,
            'status': 'empty',
            'exchange_open_interest': [],
            'exchange_statuses': list((coverage or {}).get('exchange_statuses') or []),
            'current_open_interest': None,
            'current_open_interest_formatted': 'N/A',
            'current_open_interest_value': None,
            'current_open_interest_value_formatted': 'N/A',
            'current_price': None,
            'current_price_formatted': 'N/A',
            'price_change': None,
            'price_change_percent': None,
            'price_change_formatted': 'N/A',
            'net_inflow': net_inflow,
            'changes': empty_changes,
            'current_time': current_time,
        }

    current_kline = kline_by_time.get(current_time)

    current_open_interest = float(current_oi.sum_open_interest or 0)
    current_open_interest_value = float(current_oi.sum_open_interest_value or 0)
    current_price = float(current_kline.close_price) if current_kline and current_kline.close_price is not None else None
    exchange_open_interest = _build_exchange_open_interest_rows(
        ((coverage or {}).get('open_interest_by_exchange') or {}).get(current_time, {}),
        current_open_interest_value,
        current_open_interest,
    )

    changes = {}
    for interval in TIME_INTERVALS:
        target_time = current_time - _interval_to_ms(interval)
        target_oi = oi_by_time.get(target_time)
        target_kline = kline_by_time.get(target_time)

        if target_oi is None or target_kline is None:
            changes[interval] = _empty_change()
            continue

        past_open_interest = float(target_oi.sum_open_interest or 0)
        past_open_interest_value = float(target_oi.sum_open_interest_value or 0)
        past_price = float(target_kline.close_price) if target_kline.close_price is not None else None
        price_change = current_price - past_price if current_price is not None and past_price is not None else None

        changes[interval] = {
            'ratio': _calc_percent_change(current_open_interest, past_open_interest),
            'value_ratio': _calc_percent_change(current_open_interest_value, past_open_interest_value),
            'open_interest': past_open_interest,
            'open_interest_formatted': format_number(past_open_interest),
            'open_interest_value': past_open_interest_value,
            'open_interest_value_formatted': format_number(past_open_interest_value),
            'price_change': price_change,
            'price_change_percent': _calc_percent_change(current_price, past_price),
            'price_change_formatted': format_price(price_change),
            'current_price': past_price,
            'current_price_formatted': format_price(past_price),
        }

    day_change = changes.get('24h', _empty_change())
    return {
        'symbol': symbol,
        'source_exchanges': included_exchanges,
        'included_exchanges': included_exchanges,
        'missing_exchanges': missing_exchanges,
        'status': status,
        'exchange_open_interest': exchange_open_interest,
        'exchange_statuses': list((coverage or {}).get('exchange_statuses') or []),
        'current_open_interest': current_open_interest,
        'current_open_interest_formatted': format_number(current_open_interest),
        'current_open_interest_value': current_open_interest_value,
        'current_open_interest_value_formatted': format_number(current_open_interest_value),
        'current_price': current_price,
        'current_price_formatted': format_price(current_price),
        'price_change': day_change['price_change'],
        'price_change_percent': day_change['price_change_percent'],
        'price_change_formatted': day_change['price_change_formatted'],
        'net_inflow': net_inflow,
        'changes': changes,
        'current_time': current_time,
    }


def _get_symbols(symbols):
    return symbols if symbols is not None else get_active_coins()


def _build_homepage_series_snapshot(symbols=None, session=None, now_ms=None):
    target_symbols = _get_symbols(symbols)
    own_session = session is None
    db = session or get_session()

    try:
        if not target_symbols:
            return {
                'data': [],
                'cache_update_time': None,
            }

        current_time_ms = now_ms if now_ms is not None else __import__('time').time() * 1000
        anchor_time = latest_closed_5m_open_time(int(current_time_ms))
        recent_open_interest_map, recent_klines_map, recent_taker_vol_map, coverage_map = _load_homepage_series_maps(
            db,
            target_symbols,
            upper_bound=anchor_time,
        )
        data = []
        update_time = None

        for symbol in target_symbols:
            coin = _build_coin_payload(
                symbol=symbol,
                oi_by_time=recent_open_interest_map.get(symbol, {}),
                kline_by_time=recent_klines_map.get(symbol, {}),
                taker_vol_by_time=recent_taker_vol_map.get(symbol, {}),
                coverage=coverage_map.get(symbol, {}),
            )
            if coin is None:
                continue

            coin_current_time = coin.get('current_time')
            if coin_current_time is not None:
                update_time = coin_current_time if update_time is None else min(update_time, coin_current_time)
            coin.pop('current_time', None)
            data.append(coin)

        return {
            'data': data,
            'cache_update_time': update_time,
        }
    finally:
        if own_session:
            db.close()


def get_homepage_series_snapshot(symbols=None, session=None, now_ms=None):
    return _build_homepage_series_snapshot(symbols=symbols, session=session, now_ms=now_ms)


def get_homepage_series_data(symbols=None, session=None, now_ms=None):
    return get_homepage_series_snapshot(symbols=symbols, session=session, now_ms=now_ms)['data']


def get_homepage_series_update_time(symbols=None, session=None, now_ms=None):
    return get_homepage_series_snapshot(symbols=symbols, session=session, now_ms=now_ms)['cache_update_time']


def should_refresh_homepage_series(symbols=None, now_ms=None, session=None):
    target_symbols = _get_symbols(symbols)
    own_session = session is None
    db = session or get_session()

    try:
        current_time_ms = now_ms if now_ms is not None else __import__('time').time() * 1000
        target_time = latest_closed_5m_open_time(int(current_time_ms))
        recent_open_interest_map, recent_klines_map, recent_taker_vol_map, _coverage_map = _load_homepage_series_maps(
            db,
            target_symbols,
            upper_bound=target_time,
        )

        if any(((_coverage_map.get(symbol) or {}).get('status')) != 'complete' for symbol in target_symbols):
            return True

        for symbol in target_symbols:
            oi_by_time = recent_open_interest_map.get(symbol, {})
            kline_by_time = recent_klines_map.get(symbol, {})
            taker_vol_by_time = recent_taker_vol_map.get(symbol, {})
            raw_common_times = sorted(set(oi_by_time).intersection(kline_by_time))

            if not raw_common_times:
                return True

            if raw_common_times[-1] > target_time:
                return True

            filtered_open_interest_map, filtered_klines_map, filtered_taker_vol_map, _filtered_coverage_map = _load_homepage_series_maps(
                db,
                [symbol],
                upper_bound=target_time,
            )
            filtered_oi_by_time = filtered_open_interest_map.get(symbol, {})
            filtered_kline_by_time = filtered_klines_map.get(symbol, {})
            filtered_taker_vol_by_time = filtered_taker_vol_map.get(symbol, {})
            common_times = sorted(set(filtered_oi_by_time).intersection(filtered_kline_by_time))

            if not common_times:
                return True

            current_symbol_time = common_times[-1]
            if current_symbol_time < target_time:
                return True

            if not _has_complete_homepage_coverage(filtered_oi_by_time, filtered_kline_by_time):
                return True

        return False
    finally:
        if own_session:
            db.close()
