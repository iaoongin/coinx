import json
from dataclasses import dataclass
from decimal import Decimal
from functools import lru_cache
from typing import Optional

from sqlalchemy import and_, func, or_, text

from coinx.coin_manager import get_active_coins
from coinx.collector.exchange_adapters import get_exchange_adapter, get_supported_exchange_ids
from coinx.config import ENABLED_EXCHANGES, PRIMARY_PRICE_EXCHANGE, TIME_INTERVALS, HOMEPAGE_WINDOW_HEALTH_THRESHOLD
from coinx.database import get_session
from coinx.utils import logger
from coinx.models import (
    MarketKline,
    MarketOpenInterestHist,
    MarketTakerBuySellVol,
)
from coinx.collector.exchange_repair import latest_closed_5m_open_time
from .funding_rate import load_latest_funding_rates


FIVE_MINUTES_MS = 5 * 60 * 1000
HOMEPAGE_REQUIRED_SERIES_TYPES = ('klines', 'open_interest_hist', 'taker_buy_sell_vol')


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


@lru_cache(maxsize=10000)
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


@lru_cache(maxsize=10000)
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


def format_funding_rate(rate):
    """格式化资金费率为百分比字符串，如 0.001 -> '0.100%'"""
    if rate is None:
        return 'N/A'
    return f"{float(rate) * 100:.3f}%"


def format_funding_countdown(next_funding_time):
    """格式化下次结算倒计时"""
    if next_funding_time is None:
        return 'N/A'

    now_ms = int(__import__('time').time() * 1000)
    diff_ms = int(next_funding_time) - now_ms

    if diff_ms <= 0:
        return '已结算'

    diff_seconds = diff_ms // 1000
    hours = diff_seconds // 3600
    minutes = (diff_seconds % 3600) // 60

    if hours > 0:
        return f"{hours}h{minutes:02d}m"
    return f"{minutes}m"


def _interval_to_ms(interval):
    if interval.endswith('m'):
        return int(interval[:-1]) * 60 * 1000
    if interval.endswith('h'):
        return int(interval[:-1]) * 60 * 60 * 1000
    if interval.endswith('d'):
        return int(interval[:-1]) * 24 * 60 * 60 * 1000
    raise ValueError(f'不支持的时间周期: {interval}')





def _load_net_inflow_sql(session, exchange, symbols, upper_bound):
    import time as _time
    func_start = _time.time()

    intervals = [
        ('5m',   1),
        ('15m',  3),
        ('30m',  6),
        ('1h',   12),
        ('4h',   48),
        ('12h',  144),
        ('24h',  288),
        ('48h',  576),
        ('72h',  864),
        ('168h', 2016),
    ]

    result = _empty_net_inflow_map(symbols)

    if not symbols:
        return result

    latest_rows = session.query(
        MarketTakerBuySellVol.symbol,
        func.max(MarketTakerBuySellVol.event_time).label('latest_time')
    ).filter(
        MarketTakerBuySellVol.symbol.in_(symbols),
        MarketTakerBuySellVol.exchange == exchange,
        MarketTakerBuySellVol.period == '5m',
    ).group_by(MarketTakerBuySellVol.symbol).all()

    if not latest_rows:
        total_ms = (_time.time() - func_start) * 1000
        logger.info(_fmt('净流入SQL查询完成：', exchange=exchange, symbols=0, duration=f'{total_ms:.0f}ms'))
        return result

    symbol_latest = {row.symbol: int(row.latest_time) for row in latest_rows}

    symbol_placeholders = ', '.join(f':sym_{i}' for i in range(len(symbol_latest)))
    sql = text(f"""
        SELECT
            v.symbol,
            SUM(CASE WHEN v.event_time >= :base_5m   THEN IFNULL(v.buy_vol,0) - IFNULL(v.sell_vol,0) ELSE 0 END) AS net_inflow_5m,
            SUM(CASE WHEN v.event_time >= :base_15m  THEN IFNULL(v.buy_vol,0) - IFNULL(v.sell_vol,0) ELSE 0 END) AS net_inflow_15m,
            SUM(CASE WHEN v.event_time >= :base_30m  THEN IFNULL(v.buy_vol,0) - IFNULL(v.sell_vol,0) ELSE 0 END) AS net_inflow_30m,
            SUM(CASE WHEN v.event_time >= :base_1h   THEN IFNULL(v.buy_vol,0) - IFNULL(v.sell_vol,0) ELSE 0 END) AS net_inflow_1h,
            SUM(CASE WHEN v.event_time >= :base_4h   THEN IFNULL(v.buy_vol,0) - IFNULL(v.sell_vol,0) ELSE 0 END) AS net_inflow_4h,
            SUM(CASE WHEN v.event_time >= :base_12h  THEN IFNULL(v.buy_vol,0) - IFNULL(v.sell_vol,0) ELSE 0 END) AS net_inflow_12h,
            SUM(CASE WHEN v.event_time >= :base_24h  THEN IFNULL(v.buy_vol,0) - IFNULL(v.sell_vol,0) ELSE 0 END) AS net_inflow_24h,
            SUM(CASE WHEN v.event_time >= :base_48h  THEN IFNULL(v.buy_vol,0) - IFNULL(v.sell_vol,0) ELSE 0 END) AS net_inflow_48h,
            SUM(CASE WHEN v.event_time >= :base_72h  THEN IFNULL(v.buy_vol,0) - IFNULL(v.sell_vol,0) ELSE 0 END) AS net_inflow_72h,
            SUM(CASE WHEN v.event_time >= :base_168h THEN IFNULL(v.buy_vol,0) - IFNULL(v.sell_vol,0) ELSE 0 END) AS net_inflow_168h,
            SUM(CASE WHEN v.event_time >= :base_5m   THEN (IFNULL(v.buy_vol,0) - IFNULL(v.sell_vol,0)) * k.close_price ELSE 0 END) AS net_inflow_value_5m,
            SUM(CASE WHEN v.event_time >= :base_15m  THEN (IFNULL(v.buy_vol,0) - IFNULL(v.sell_vol,0)) * k.close_price ELSE 0 END) AS net_inflow_value_15m,
            SUM(CASE WHEN v.event_time >= :base_30m  THEN (IFNULL(v.buy_vol,0) - IFNULL(v.sell_vol,0)) * k.close_price ELSE 0 END) AS net_inflow_value_30m,
            SUM(CASE WHEN v.event_time >= :base_1h   THEN (IFNULL(v.buy_vol,0) - IFNULL(v.sell_vol,0)) * k.close_price ELSE 0 END) AS net_inflow_value_1h,
            SUM(CASE WHEN v.event_time >= :base_4h   THEN (IFNULL(v.buy_vol,0) - IFNULL(v.sell_vol,0)) * k.close_price ELSE 0 END) AS net_inflow_value_4h,
            SUM(CASE WHEN v.event_time >= :base_12h  THEN (IFNULL(v.buy_vol,0) - IFNULL(v.sell_vol,0)) * k.close_price ELSE 0 END) AS net_inflow_value_12h,
            SUM(CASE WHEN v.event_time >= :base_24h  THEN (IFNULL(v.buy_vol,0) - IFNULL(v.sell_vol,0)) * k.close_price ELSE 0 END) AS net_inflow_value_24h,
            SUM(CASE WHEN v.event_time >= :base_48h  THEN (IFNULL(v.buy_vol,0) - IFNULL(v.sell_vol,0)) * k.close_price ELSE 0 END) AS net_inflow_value_48h,
            SUM(CASE WHEN v.event_time >= :base_72h  THEN (IFNULL(v.buy_vol,0) - IFNULL(v.sell_vol,0)) * k.close_price ELSE 0 END) AS net_inflow_value_72h,
            SUM(CASE WHEN v.event_time >= :base_168h THEN (IFNULL(v.buy_vol,0) - IFNULL(v.sell_vol,0)) * k.close_price ELSE 0 END) AS net_inflow_value_168h,
            ROUND(SUM(CASE WHEN v.event_time >= :base_5m   THEN 1 ELSE 0 END) / 1   * 100, 1) AS health_5m,
            ROUND(SUM(CASE WHEN v.event_time >= :base_15m  THEN 1 ELSE 0 END) / 3   * 100, 1) AS health_15m,
            ROUND(SUM(CASE WHEN v.event_time >= :base_30m  THEN 1 ELSE 0 END) / 6   * 100, 1) AS health_30m,
            ROUND(SUM(CASE WHEN v.event_time >= :base_1h   THEN 1 ELSE 0 END) / 12  * 100, 1) AS health_1h,
            ROUND(SUM(CASE WHEN v.event_time >= :base_4h   THEN 1 ELSE 0 END) / 48  * 100, 1) AS health_4h,
            ROUND(SUM(CASE WHEN v.event_time >= :base_12h  THEN 1 ELSE 0 END) / 144 * 100, 1) AS health_12h,
            ROUND(SUM(CASE WHEN v.event_time >= :base_24h  THEN 1 ELSE 0 END) / 288 * 100, 1) AS health_24h,
            ROUND(SUM(CASE WHEN v.event_time >= :base_48h  THEN 1 ELSE 0 END) / 576 * 100, 1) AS health_48h,
            ROUND(SUM(CASE WHEN v.event_time >= :base_72h  THEN 1 ELSE 0 END) / 864 * 100, 1) AS health_72h,
            ROUND(SUM(CASE WHEN v.event_time >= :base_168h THEN 1 ELSE 0 END) / 2016 * 100, 1) AS health_168h
        FROM market_taker_buy_sell_vol v
        JOIN market_klines k
          ON k.exchange = v.exchange AND k.symbol = v.symbol AND k.period = v.period AND k.open_time = v.event_time
        WHERE v.exchange = :exchange
          AND v.symbol IN ({symbol_placeholders})
          AND v.period = '5m'
          AND k.period = '5m'
          AND v.event_time >= :lower_bound
        GROUP BY v.symbol
    """)

    unified_base = list(symbol_latest.values())[0]
    base_boundaries = {}
    for interval, _ in intervals:
        base_boundaries[f'base_{interval}'] = unified_base - int(_interval_to_ms(interval)) + FIVE_MINUTES_MS

    params = {
        'exchange': exchange,
        'lower_bound': unified_base - int(_interval_to_ms('168h')) + FIVE_MINUTES_MS,
        **{f'sym_{i}': s for i, s in enumerate(symbol_latest)},
    }
    params.update(base_boundaries)

    rows = session.execute(sql, params).fetchall()

    for row in rows:
        symbol = row.symbol
        if symbol in result:
            for interval, _ in intervals:
                col = f'net_inflow_{interval}'
                val_col = f'net_inflow_value_{interval}'
                health_col = f'health_{interval}'
                val = getattr(row, col, None)
                if val is not None:
                    result[symbol]['net_inflow'][interval] = float(val)
                val_v = getattr(row, val_col, None)
                if val_v is not None:
                    result[symbol]['net_inflow_value'][interval] = float(val_v)
                health_val = getattr(row, health_col, None)
                if health_val is not None:
                    result[symbol]['health'][interval] = float(health_val)

    total_ms = (_time.time() - func_start) * 1000
    populated = sum(1 for s in result if result[s]['net_inflow'])
    logger.info(_fmt('净流入SQL查询完成：', exchange=exchange, symbols=populated, duration=f'{total_ms:.0f}ms'))
    return result


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


def _format_usd_map(values):
    return {
        interval: format_usd_value(value)
        for interval, value in (values or {}).items()
        if value is not None
    }


def _empty_net_inflow_map(symbols):
    return {symbol: {'net_inflow': {}, 'net_inflow_value': {}, 'health': {}} for symbol in symbols}


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
    logger.warning(_fmt('交易所门禁否决：', exchange=exchange, symbol=symbol, stage=stage,
                        anchor_time=anchor_time, reason=summary_reason, summary=summary))


def _log_homepage_symbol_summary(symbol, included_exchanges, missing_exchanges, status, current_time):
    available = bool(included_exchanges)
    log = _fmt('交易所聚合完成：' if available else '交易所聚合为空态：', symbol=symbol,
               current_time=current_time, status=status,
               included_exchanges=_format_homepage_log_details(included_exchanges),
               missing_exchanges=_format_homepage_log_details(missing_exchanges),
               available=str(available).lower())
    if available:
        logger.info(log)
    else:
        logger.warning(log)


def _collect_exchange_homepage_rejection_reasons(exchange, oi_by_time, kline_by_time, anchor_time):
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


def _has_required_change_coverage(oi_by_time, kline_by_time, current_time):
    if not current_time:
        return False
    for interval in TIME_INTERVALS:
        target = current_time - _interval_to_ms(interval)
        if oi_by_time.get(target) is None:
            return False
    return True


def _has_complete_homepage_coverage(oi_by_time, kline_by_time):
    common_times = sorted(set(oi_by_time).intersection(kline_by_time))
    if not common_times:
        return False

    current_time = common_times[-1]
    return _has_required_change_coverage(oi_by_time, kline_by_time, current_time)


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
    return point


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


def _supports_taker(exchange):
    try:
        adapter = get_exchange_adapter(exchange)
        return adapter is not None and adapter.taker_period_by_interval is not None
    except Exception:
        return False


def _has_unreliable_taker_source(exchange):
    return (exchange or '').lower() == 'gate'


def _has_available_taker_source(exchange, status, support_state=None, taker_rejection=None):
    if status != 'included':
        return False
    if (support_state or {}).get('state') == 'unsupported':
        return False
    if _has_unreliable_taker_source(exchange):
        return False
    if not _supports_taker(exchange):
        return False
    return not bool((taker_rejection or {}).get('reasons'))


def _build_taker_status(exchange, row_status, support_state=None, taker_rejection=None):
    if row_status != 'included':
        return row_status
    if (support_state or {}).get('state') == 'unsupported':
        return 'unsupported'
    if _has_unreliable_taker_source(exchange):
        return 'unreliable'
    if not _supports_taker(exchange):
        return 'unsupported'
    if (taker_rejection or {}).get('reasons'):
        return 'missing'
    return 'available'


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
        exchange_supports_taker = _supports_taker(exchange)
        if exchange not in supported_exchanges:
            rows.append(
                {
                    'exchange': exchange,
                    'status': 'unsupported',
                    'supports_taker': False,
                    'taker_status': 'unsupported',
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
                    'supports_taker': False,
                    'taker_status': 'unsupported',
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
            taker_rejection = snapshot.get('taker_rejection') or {}
            row['supports_taker'] = _has_available_taker_source(
                exchange,
                row['status'],
                support_state=support_state,
                taker_rejection=taker_rejection,
            )
            row['taker_status'] = _build_taker_status(
                exchange,
                row['status'],
                support_state=support_state,
                taker_rejection=taker_rejection,
            )
            if taker_rejection.get('reasons') and row['taker_status'] == 'missing':
                row['taker_reason'] = taker_rejection.get('reasons')
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
            'supports_taker': False,
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
        taker_rejection = snapshot.get('taker_rejection') or {}
        row['taker_status'] = _build_taker_status(
            exchange,
            row_status,
            support_state=support_state,
            taker_rejection=taker_rejection,
        )
        if taker_rejection.get('reasons') and row['taker_status'] == 'missing':
            row['taker_reason'] = taker_rejection.get('reasons')
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


def _exchange_supports_homepage_anchor(exchange, oi_by_time, kline_by_time, anchor_time):
    return not _collect_exchange_homepage_rejection_reasons(
        exchange,
        oi_by_time,
        kline_by_time,
        anchor_time,
    )


def _build_exchange_homepage_snapshot(exchange, oi_by_time, kline_by_time):
    _s = __import__('time').perf_counter()
    current_time = _get_exchange_common_time(oi_by_time, kline_by_time)
    _t1 = __import__('time').perf_counter()
    _ct_ms = (_t1 - _s) * 1000
    if current_time is None:
        reasons = _collect_exchange_homepage_rejection_reasons(
            exchange,
            oi_by_time,
            kline_by_time,
            None,
        )
        _t2 = __import__('time').perf_counter()
        _rej_ms = (_t2 - _t1) * 1000
        logger.info(_fmt('snapshot明细：', exchange=exchange, common_time_ms=f'{_ct_ms:.1f}', rejection_ms=f'{_rej_ms:.1f}'))
        return {
            'complete': False,
            'current_time': None,
            'reasons': reasons,
        }

    reasons = _collect_exchange_homepage_rejection_reasons(
        exchange,
        oi_by_time,
        kline_by_time,
        current_time,
    )
    _t2 = __import__('time').perf_counter()
    _rej_ms = (_t2 - _t1) * 1000
    complete = not reasons
    logger.info(_fmt('snapshot明细：', exchange=exchange, common_time_ms=f'{_ct_ms:.1f}', rejection_ms=f'{_rej_ms:.1f}'))
    return {
        'complete': complete,
        'current_time': current_time,
        'reasons': reasons,
    }


_FMT_COL_WIDTHS = (18, 16, 16, 16)


def _cjk_ljust(s, width):
    dw = sum(2 if ord(c) > 0x2e80 else 1 for c in s)
    return s + ' ' * max(0, width - dw)


def _fmt(label, **kw):
    items = list(kw.items())
    parts = [_cjk_ljust(label, _FMT_COL_WIDTHS[0])]
    for i, (k, v) in enumerate(items[:3]):
        parts.append(_cjk_ljust(f'{k}={v}', _FMT_COL_WIDTHS[min(i + 1, 3)]))
    for k, v in items[3:]:
        parts.append(f'{k}={v}')
    return '  '.join(parts)


def _load_open_interest_model_map(session, model, symbols, upper_bound=None, exchange=None):
    """加载 OI 数据 - 按目标时间点加载"""
    import time as _time
    func_start = _time.time()
    if not symbols:
        return {}, {}

    time_field = model.event_time
    latest_query = session.query(
        model.symbol,
        func.max(time_field).label('latest_time')
    ).filter(
        model.symbol.in_(symbols),
        model.period == '5m'
    )
    if hasattr(model, 'exchange') and exchange is not None:
        latest_query = latest_query.filter(model.exchange == exchange)
    latest_query = latest_query.group_by(model.symbol)

    symbol_latest = {}
    for row in latest_query.all():
        symbol_latest[row.symbol] = int(row.latest_time)

    if not symbol_latest:
        return {}, {}

    latest_ms = (_time.time() - func_start) * 1000
    logger.info(_fmt('OI查询：', exchange=exchange, symbols=len(symbols), duration=f'{latest_ms:.0f}ms'))

    conditions = []
    for symbol, latest in symbol_latest.items():
        target_times = {latest}
        for interval in TIME_INTERVALS:
            target_times.add(latest - _interval_to_ms(interval))
        target_times = sorted(t for t in target_times if t > 0)
        if not target_times:
            continue
        condition = and_(
            model.symbol == symbol,
            model.event_time.in_(target_times),
        )
        if hasattr(model, 'exchange') and exchange is not None:
            condition = and_(condition, model.exchange == exchange)
        conditions.append(condition)

    if not conditions:
        return {}, symbol_latest

    query = session.query(
        model.symbol,
        model.event_time,
        model.sum_open_interest,
        model.sum_open_interest_value,
    ).filter(
        or_(*conditions),
        model.period == '5m',
    )
    if upper_bound is not None:
        query = query.filter(model.event_time <= upper_bound)

    records_by_symbol = {symbol: {} for symbol in symbols}
    for row in query.all():
        point = _build_open_interest_point(row)
        records_by_symbol.setdefault(point.symbol, {})[point.event_time] = point

    total_ms = (_time.time() - func_start) * 1000
    total_records = sum(len(v) for v in records_by_symbol.values())
    logger.info(_fmt('OI查询完成：', exchange=exchange, records=total_records, duration=f'{total_ms:.0f}ms'))
    return records_by_symbol, symbol_latest


def _load_kline_model_map(session, model, symbols, symbol_latest=None, upper_bound=None, exchange=None):
    """加载 Kline 数据 - 按目标时间点加载。接受 symbol_latest 确保与 OI 同基准"""
    import time as _time
    func_start = _time.time()
    if not symbols:
        return {}, {}

    if symbol_latest is None:
        time_field = model.open_time
        latest_query = session.query(
            model.symbol,
            func.max(time_field).label('latest_time')
        ).filter(
            model.symbol.in_(symbols),
            model.period == '5m'
        )
        if hasattr(model, 'exchange') and exchange is not None:
            latest_query = latest_query.filter(model.exchange == exchange)
        latest_query = latest_query.group_by(model.symbol)

        symbol_latest = {}
        for row in latest_query.all():
            symbol_latest[row.symbol] = int(row.latest_time)

        if not symbol_latest:
            return {}, {}

    latest_ms = (_time.time() - func_start) * 1000
    logger.info(_fmt('Kline查询：', exchange=exchange, symbols=len(symbols), duration=f'{latest_ms:.0f}ms'))

    conditions = []
    for symbol, latest in symbol_latest.items():
        target_times = {latest}
        for interval in TIME_INTERVALS:
            target_times.add(latest - _interval_to_ms(interval))
        target_times = sorted(t for t in target_times if t > 0)
        if not target_times:
            continue
        condition = and_(
            model.symbol == symbol,
            model.open_time.in_(target_times),
        )
        if hasattr(model, 'exchange') and exchange is not None:
            condition = and_(condition, model.exchange == exchange)
        conditions.append(condition)

    if not conditions:
        return {}, symbol_latest

    query = session.query(
        model.symbol,
        model.open_time,
        model.high_price,
        model.low_price,
        model.close_price,
        model.quote_volume,
        model.taker_buy_quote_volume,
    ).filter(
        or_(*conditions),
        model.period == '5m',
    )
    if upper_bound is not None:
        query = query.filter(model.open_time <= upper_bound)

    records_by_symbol = {symbol: {} for symbol in symbols}
    for row in query.all():
        point = _build_kline_point(row)
        records_by_symbol.setdefault(point.symbol, {})[point.open_time] = point

    total_ms = (_time.time() - func_start) * 1000
    logger.info(_fmt('Kline查询完成：', exchange=exchange, symbols=len(symbols), records=sum(len(v) for v in records_by_symbol.values()), duration=f'{total_ms:.0f}ms'))
    return records_by_symbol, symbol_latest


def _load_exchange_homepage_maps(session, exchange, symbols, upper_bound=None):
    exchange = exchange.lower()

    start_time = __import__('time').perf_counter()
    logger.debug(_fmt('交易所加载开始：', exchange=exchange, symbols=len(symbols)))

    oi_start = __import__('time').perf_counter()
    oi_map, symbol_latest = _load_open_interest_model_map(
        session,
        MarketOpenInterestHist,
        symbols,
        upper_bound=upper_bound,
        exchange=exchange,
    )
    oi_elapsed = __import__('time').perf_counter() - oi_start
    if oi_elapsed >= 0.1:
        logger.info(_fmt('OI 加载完成：', exchange=exchange, symbols=len(symbols), duration=f'{oi_elapsed:.2f}s'))

    kline_start = __import__('time').perf_counter()
    kline_map, kline_latest = _load_kline_model_map(
        session,
        MarketKline,
        symbols,
        symbol_latest=symbol_latest,
        upper_bound=upper_bound,
        exchange=exchange,
    )
    kline_elapsed = __import__('time').perf_counter() - kline_start
    if kline_elapsed >= 0.1:
        logger.info(_fmt('Kline 加载完成：', exchange=exchange, symbols=len(symbols), duration=f'{kline_elapsed:.2f}s'))

    net_inflow_start = __import__('time').perf_counter()
    if _has_unreliable_taker_source(exchange):
        net_inflow_map = _empty_net_inflow_map(symbols)
        logger.info('Skip homepage net inflow SQL for unreliable taker source: exchange=%s symbols=%s', exchange, len(symbols))
    else:
        net_inflow_map = _load_net_inflow_sql(
            session,
            exchange,
            symbols,
            upper_bound=upper_bound,
        )
    net_inflow_elapsed = __import__('time').perf_counter() - net_inflow_start
    if net_inflow_elapsed >= 0.1:
        logger.info(_fmt('净流入查询完成：', exchange=exchange, symbols=len(symbols), duration=f'{net_inflow_elapsed:.2f}s'))

    unified_latest = {
        sym: min(symbol_latest.get(sym, 0), kline_latest.get(sym, 0))
        for sym in symbols
        if sym in symbol_latest and sym in kline_latest
    }
    logger.info(_fmt('交易所加载完成：', exchange=exchange, duration=f'{__import__("time").perf_counter() - start_time:.2f}s'))
    return oi_map, kline_map, net_inflow_map, unified_latest


def _aggregate_homepage_series_maps(session, symbols, upper_bound=None):
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time as _time

    exchanges = _get_enabled_exchanges()
    supported_exchanges = set(_normalize_exchange_list(get_supported_exchange_ids()))
    exchange_maps = {}
    exchange_adapters = {}

    # 分离支持和不支持的交易所
    supported_list = [e for e in exchanges if e in supported_exchanges]
    unsupported_list = [e for e in exchanges if e not in supported_exchanges]

    # 不支持的交易所直接填充空数据
    for exchange in unsupported_list:
        exchange_maps[exchange] = ({symbol: {} for symbol in symbols}, {symbol: {} for symbol in symbols}, {}, {})
        exchange_adapters[exchange] = None

    # 并行查询支持的交易所
    def load_exchange(exchange):
        """Load one exchange in a worker thread."""
        from coinx.database import get_session
        thread_session = get_session()
        adapter = None
        try:
            logger.info('Homepage exchange load start: exchange=%s', exchange)
            result = _load_exchange_homepage_maps(thread_session, exchange, symbols, upper_bound=upper_bound)
            try:
                adapter = get_exchange_adapter(exchange)
            except Exception:
                adapter = None
            if adapter is not None and hasattr(adapter, 'warm_symbol_support_cache'):
                try:
                    adapter.warm_symbol_support_cache()
                except Exception as exc:
                    logger.warning('Exchange support cache prewarm failed: exchange=%s error=%s', exchange, exc)
            logger.info('Homepage exchange load done: exchange=%s', exchange)
            return exchange, result, adapter, None
        except Exception as e:
            logger.error('Homepage exchange load failed: exchange=%s error=%s', exchange, e)
            return exchange, None, adapter, e
        finally:
            thread_session.close()

    start_time = _time.perf_counter()

    logger.info('Homepage parallel exchange load start: exchanges=%s exchange_list=%s', len(supported_list), supported_list)
    with ThreadPoolExecutor(max_workers=min(4, len(supported_list))) as executor:
        futures = {executor.submit(load_exchange, exchange): exchange for exchange in supported_list}

        for future in as_completed(futures):
            exchange, result, adapter, error = future.result()
            if error:
                logger.error('Homepage exchange load failed after future completion: exchange=%s error=%s', exchange, error)
                exchange_maps[exchange] = ({symbol: {} for symbol in symbols}, {symbol: {} for symbol in symbols}, {}, {})
            else:
                exchange_maps[exchange] = result

            exchange_adapters[exchange] = adapter

    elapsed = _time.perf_counter() - start_time
    logger.info('Homepage parallel exchange load done: exchanges=%s duration=%.2fs', len(exchanges), elapsed)

    primary_exchange = PRIMARY_PRICE_EXCHANGE.lower()

    aggregate_oi_map = {symbol: {} for symbol in symbols}
    coverage_map = {
        symbol: {
            'source_exchanges': [],
            'included_exchanges': [],
            'missing_exchanges': [],
            'open_interest_by_exchange': {},
            'net_inflow': {},
            'net_inflow_value': {},
            'status': 'empty',
            'latest_time': None,
        }
        for symbol in symbols
    }
    selected_kline_map = {symbol: {} for symbol in symbols}

    for symbol in symbols:
        _sym_start = __import__('time').perf_counter()
        symbol_exchange_snapshots = {}
        exchange_rejection_info = {}
        symbol_net_inflow_by_exchange = {}
        for exchange, (oi_map, _kline_map, net_inflow_map, unified_latest) in exchange_maps.items():
            if exchange not in supported_exchanges:
                symbol_exchange_snapshots[exchange] = {
                    'current_time': None,
                    'oi_by_time': {},
                    'kline_by_time': {},
                    'complete': False,
                    'unsupported': True,
                    'support_state': {'state': 'unsupported'},
                    'taker_rejection': {
                        'stage': 'taker_validation',
                        'anchor_time': None,
                        'reasons': [],
                    },
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

            _e_start = __import__('time').perf_counter()
            adapter = exchange_adapters.get(exchange)
            support_state = {'state': 'supported', 'supported': True, 'known': True}
            if adapter is not None and hasattr(adapter, 'symbol_support_state'):
                support_state = adapter.symbol_support_state(symbol)
            _t_support = (__import__('time').perf_counter() - _e_start) * 1000

            if support_state.get('state') == 'unsupported':
                symbol_exchange_snapshots[exchange] = {
                    'current_time': None,
                    'oi_by_time': {},
                    'kline_by_time': {},
                    'complete': False,
                    'unsupported': True,
                    'support_state': support_state,
                    'taker_rejection': {
                        'stage': 'taker_validation',
                        'anchor_time': None,
                        'reasons': [],
                    },
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
                logger.info(_fmt('聚合打点：', symbol=symbol, exchange=exchange, support=f'{_t_support:.1f}', get='-', snap='-', taker='-'))
                continue

            _t3 = __import__('time').perf_counter()
            symbol_oi = oi_map.get(symbol, {})
            symbol_kline = _kline_map.get(symbol, {})
            symbol_net_inflow = net_inflow_map.get(symbol, {'net_inflow': {}, 'health': {}})
            symbol_net_inflow_by_exchange[exchange] = symbol_net_inflow
            _t_get = (__import__('time').perf_counter() - _t3) * 1000

            _t_snap = 0.0
            _t_taker = 0.0
            if symbol_oi or symbol_kline:
                _t4 = __import__('time').perf_counter()
                snapshot = _build_exchange_homepage_snapshot(
                    exchange=exchange,
                    oi_by_time=symbol_oi,
                    kline_by_time=symbol_kline,
                )
                _t_snap = (__import__('time').perf_counter() - _t4) * 1000

                _t5 = __import__('time').perf_counter()
                taker_reasons = []
                if _has_unreliable_taker_source(exchange):
                    taker_reasons.append({'reason': 'unreliable_taker_source', 'details': {'exchange': exchange}})
                else:
                    net_inflow_data = symbol_net_inflow.get('net_inflow', {})
                    health = symbol_net_inflow.get('health', {})
                    has_any_taker = bool(net_inflow_data)
                    if not has_any_taker:
                        taker_reasons.append({'reason': 'missing_taker_history', 'details': {'health_pct': 0}})
                    else:
                        for interval in TIME_INTERVALS:
                            h = health.get(interval)
                            if h is not None and h < HOMEPAGE_WINDOW_HEALTH_THRESHOLD:
                                taker_reasons.append({
                                    'reason': 'taker_health_low',
                                    'details': {'interval': interval, 'health_pct': h},
                                })
                _t_taker = (__import__('time').perf_counter() - _t5) * 1000

                symbol_exchange_snapshots[exchange] = {
                    'current_time': snapshot['current_time'],
                    'oi_by_time': symbol_oi,
                    'kline_by_time': symbol_kline,
                    'complete': snapshot['complete'],
                    'unsupported': False,
                    'support_state': support_state,
                    'taker_rejection': {
                        'stage': 'taker_validation',
                        'anchor_time': snapshot.get('current_time'),
                        'reasons': taker_reasons,
                    },
                    'reasons': snapshot.get('reasons') or _collect_exchange_homepage_rejection_reasons(
                        exchange,
                        symbol_oi,
                        symbol_kline,
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
                    'complete': False,
                    'unsupported': False,
                    'support_state': support_state,
                    'taker_rejection': {
                        'stage': 'taker_validation',
                        'anchor_time': None,
                        'reasons': [{'reason': 'missing_taker_history', 'details': {'health_pct': 0}}],
                    },
                    'reasons': _collect_exchange_homepage_rejection_reasons(
                        exchange,
                        symbol_oi,
                        symbol_kline,
                        None,
                    ),
                }
                coverage_map[symbol]['missing_exchanges'].append(exchange)
                exchange_rejection_info[exchange] = {
                    'stage': 'initial_snapshot',
                    'anchor_time': None,
                    'reasons': symbol_exchange_snapshots[exchange]['reasons'],
                }

            logger.info(_fmt('聚合打点：', symbol=symbol, exchange=exchange, support=f'{_t_support:.1f}', get=f'{_t_get:.1f}', snap=f'{_t_snap:.1f}', taker=f'{_t_taker:.1f}'))

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

        _t1 = __import__('time').perf_counter()
        _snap_elapsed = (_t1 - _sym_start) * 1000
        logger.info(_fmt('聚合打点：', symbol=symbol, stage='snapshot', ms=f'{_snap_elapsed:.0f}'))

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
                            anchor_time,
                        ),
                    }

            included_exchanges = _normalize_exchange_list(included_exchanges)
            if not included_exchanges:
                anchor_time = None
                break

            new_anchor_time = max(symbol_exchange_snapshots[exchange]['current_time'] for exchange in included_exchanges)
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
        coverage_map[symbol]['missing_exchanges'] = _normalize_exchange_list(missing_exchanges)
        coverage_map[symbol]['status'] = 'complete' if not coverage_map[symbol]['missing_exchanges'] else 'partial'

        unified_times = [
            unified_latest.get(symbol)
            for exchange, (_, _, _, unified_latest) in exchange_maps.items()
            if exchange in included_exchanges and unified_latest.get(symbol)
        ]
        coverage_map[symbol]['latest_time'] = min(unified_times) if unified_times else None
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

        _t2 = __import__('time').perf_counter()
        _anchor_elapsed = (_t2 - _t1) * 1000
        logger.info(_fmt('聚合打点：', symbol=symbol, stage='anchor_loop', ms=f'{_anchor_elapsed:.0f}'))

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

        # 从 SQL 结果累计净流入
        for interval in TIME_INTERVALS:
            interval_values = []
            interval_value_values = []
            for exchange in included_exchanges:
                taker_rejection = symbol_exchange_snapshots[exchange].get('taker_rejection', {})
                if taker_rejection.get('reasons'):
                    continue
                net_inflow_data = symbol_net_inflow_by_exchange.get(exchange, {})
                inflow = net_inflow_data.get('net_inflow', {}).get(interval)
                inflow_value = net_inflow_data.get('net_inflow_value', {}).get(interval)
                if inflow is not None:
                    interval_values.append(inflow)
                if inflow_value is not None:
                    interval_value_values.append(inflow_value)

            if interval_values:
                coverage_map[symbol]['net_inflow'][interval] = sum(interval_values)
            if interval_value_values:
                coverage_map[symbol]['net_inflow_value'][interval] = sum(interval_value_values)

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

        _t3 = __import__('time').perf_counter()
        _merge_elapsed = (_t3 - _t2) * 1000
        _total_elapsed = (_t3 - _sym_start) * 1000
        logger.info(_fmt('聚合打点：', symbol=symbol, stage='merge', ms=f'{_merge_elapsed:.0f}', total=f'{_total_elapsed:.0f}'))

    return aggregate_oi_map, selected_kline_map, {}, coverage_map


def _load_homepage_series_maps(session, symbols, upper_bound=None):
    aggregate_oi_map, selected_kline_map, _, coverage_map = _aggregate_homepage_series_maps(
        session, symbols, upper_bound=upper_bound
    )
    funding_rate_map = load_latest_funding_rates(symbols, session=session)
    return aggregate_oi_map, selected_kline_map, coverage_map, funding_rate_map


def _build_coin_payload(symbol, oi, kline_by_time, coverage=None, funding_rate=None):
    common_times = sorted(set(oi).intersection(kline_by_time))
    oi_times = sorted(oi)
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
            'net_inflow_value': {},
            'net_inflow_value_formatted': {},
            'changes': empty_changes,
            'current_time': None,
            'predicted_rate': None,
            'predicted_rate_formatted': 'N/A',
            'funding_rate': None,
            'funding_rate_formatted': 'N/A',
            'next_funding_time': None,
            'next_funding_time_formatted': 'N/A',
        }

    current_time = common_times[-1] if common_times else oi_times[-1]
    net_inflow = dict((coverage or {}).get('net_inflow') or {})
    net_inflow_value = dict((coverage or {}).get('net_inflow_value') or {})

    current_oi = oi.get(current_time)
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
            'net_inflow_value': net_inflow_value,
            'net_inflow_value_formatted': _format_usd_map(net_inflow_value),
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
        target_oi = oi.get(target_time)
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
            'open_interest_value_formatted': format_usd_value(past_open_interest_value),
            'price_change': price_change,
            'price_change_percent': _calc_percent_change(current_price, past_price),
            'price_change_formatted': format_price(price_change),
            'current_price': past_price,
            'current_price_formatted': format_price(past_price),
        }

    day_change = changes.get('24h', _empty_change())

    # Extract funding rate data
    predicted_rate = float(funding_rate['predicted_rate']) if funding_rate and funding_rate['predicted_rate'] is not None else None
    funding_rate_value = float(funding_rate['funding_rate']) if funding_rate and funding_rate['funding_rate'] is not None else None
    next_funding_time = int(funding_rate['next_funding_time']) if funding_rate and funding_rate['next_funding_time'] is not None else None

    latest_time = (coverage or {}).get('latest_time')

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
        'current_open_interest_value_formatted': format_usd_value(current_open_interest_value),
        'current_price': current_price,
        'current_price_formatted': format_price(current_price),
        'price_change': day_change['price_change'],
        'price_change_percent': day_change['price_change_percent'],
        'price_change_formatted': day_change['price_change_formatted'],
        'net_inflow': net_inflow,
        'net_inflow_value': net_inflow_value,
        'net_inflow_value_formatted': _format_usd_map(net_inflow_value),
        'changes': changes,
        'current_time': current_time,
        'latest_time': latest_time,
        'predicted_rate': predicted_rate,
        'predicted_rate_formatted': format_funding_rate(predicted_rate),
        'funding_rate': funding_rate_value,
        'funding_rate_formatted': format_funding_rate(funding_rate_value),
        'next_funding_time': next_funding_time,
        'next_funding_time_formatted': format_funding_countdown(next_funding_time),
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
        recent_open_interest_map, recent_klines_map, coverage_map, funding_rate_map = _load_homepage_series_maps(
            db,
            target_symbols,
            upper_bound=anchor_time,
        )
        data = []
        update_time = None

        for symbol in target_symbols:
            coin = _build_coin_payload(
                symbol=symbol,
                oi=recent_open_interest_map.get(symbol, {}),
                kline_by_time=recent_klines_map.get(symbol, {}),
                coverage=coverage_map.get(symbol, {}),
                funding_rate=funding_rate_map.get(symbol),
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
        recent_open_interest_map, recent_klines_map, _coverage_map, _ = _load_homepage_series_maps(
            db,
            target_symbols,
            upper_bound=target_time,
        )

        if any(((_coverage_map.get(symbol) or {}).get('status')) != 'complete' for symbol in target_symbols):
            return True

        for symbol in target_symbols:
            oi_by_time = recent_open_interest_map.get(symbol, {})
            kline_by_time = recent_klines_map.get(symbol, {})
            common_times = sorted(set(oi_by_time).intersection(kline_by_time))

            if not common_times:
                return True

            current_symbol_time = common_times[-1]
            if current_symbol_time < target_time:
                return True

            if not _has_complete_homepage_coverage(oi_by_time, kline_by_time):
                return True

        return False
    finally:
        if own_session:
            db.close()
