from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from sqlalchemy import and_, func, or_

from coinx.coin_manager import get_active_coins
from coinx.config import TIME_INTERVALS
from coinx.database import get_session
from coinx.models import BinanceKline, BinanceOpenInterestHist, BinanceTakerBuySellVol
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


def _interval_to_ms(interval):
    if interval.endswith('m'):
        return int(interval[:-1]) * 60 * 1000
    if interval.endswith('h'):
        return int(interval[:-1]) * 60 * 60 * 1000
    if interval.endswith('d'):
        return int(interval[:-1]) * 24 * 60 * 60 * 1000
    raise ValueError(f'Unsupported interval: {interval}')


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


def _load_homepage_series_maps(session, symbols, upper_bound=None):
    if len(symbols) < HOMEPAGE_BULK_QUERY_THRESHOLD:
        return (
            {symbol: _load_recent_open_interest(session, symbol, upper_bound=upper_bound) for symbol in symbols},
            {symbol: _load_recent_klines(session, symbol, upper_bound=upper_bound) for symbol in symbols},
            {symbol: _load_recent_taker_buy_sell_vol(session, symbol, upper_bound=upper_bound) for symbol in symbols},
        )

    lower_bounds = _build_homepage_lower_bounds(session, symbols, upper_bound=upper_bound)
    return (
        _load_recent_open_interest_map(session, symbols, lower_bounds=lower_bounds, upper_bound=upper_bound),
        _load_recent_klines_map(session, symbols, lower_bounds=lower_bounds, upper_bound=upper_bound),
        _load_recent_taker_buy_sell_vol_map(session, symbols, lower_bounds=lower_bounds, upper_bound=upper_bound),
    )


def _build_coin_payload(symbol, oi_by_time, kline_by_time, taker_vol_by_time):
    common_times = sorted(set(oi_by_time).intersection(kline_by_time))
    if not common_times:
        return None

    current_time = common_times[-1]
    net_inflow = {}

    if taker_vol_by_time and any(taker_vol_by_time.values()):
        net_inflow = _build_net_inflow_from_taker_vol(taker_vol_by_time, current_time)

    current_oi = oi_by_time[current_time]
    current_kline = kline_by_time[current_time]

    current_open_interest = float(current_oi.sum_open_interest or 0)
    current_open_interest_value = float(current_oi.sum_open_interest_value or 0)
    current_price = float(current_kline.close_price or 0)

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
        past_price = float(target_kline.close_price or 0)
        price_change = current_price - past_price

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
        recent_open_interest_map, recent_klines_map, recent_taker_vol_map = _load_homepage_series_maps(
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
            )
            if coin is None:
                continue

            update_time = coin['current_time'] if update_time is None else min(update_time, coin['current_time'])
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
        recent_open_interest_map, recent_klines_map, recent_taker_vol_map = _load_homepage_series_maps(db, target_symbols)

        for symbol in target_symbols:
            oi_by_time = recent_open_interest_map.get(symbol, {})
            kline_by_time = recent_klines_map.get(symbol, {})
            taker_vol_by_time = recent_taker_vol_map.get(symbol, {})
            raw_common_times = sorted(set(oi_by_time).intersection(kline_by_time))

            if not raw_common_times:
                return True

            if raw_common_times[-1] > target_time:
                return True

            filtered_open_interest_map, filtered_klines_map, filtered_taker_vol_map = _load_homepage_series_maps(
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
