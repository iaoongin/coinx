import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import and_, func, or_

from coinx.collector.exchange_adapters import get_exchange_adapter, get_supported_exchange_ids
from coinx.config import ENABLED_EXCHANGES, TIME_INTERVALS
from coinx.database import get_session
from coinx.models import (
    MarketKline,
    MarketOpenInterestHist,
    MarketTakerBuySellVol,
)
from coinx.read_backend import get_clickhouse_repository, is_clickhouse_read
from coinx.utils import logger


FIVE_MINUTES_MS = 5 * 60 * 1000
MARKET_STRUCTURE_BULK_QUERY_THRESHOLD = 8
MARKET_STRUCTURE_INTERVALS = ('5m', '15m', '30m', '1h', '4h', '6h', '12h', '24h')
MARKET_STRUCTURE_COMPONENT_MAX_WORKERS = 4
MARKET_STRUCTURE_KLINE_POINTS = 120


@dataclass(frozen=True)
class MarketStructureOpenInterestPoint:
    symbol: str
    event_time: int
    sum_open_interest: Optional[float]
    sum_open_interest_value: Optional[float]


@dataclass(frozen=True)
class MarketStructureKlinePoint:
    symbol: str
    open_time: int
    high_price: Optional[float]
    low_price: Optional[float]
    close_price: Optional[float]
    quote_volume: Optional[float]
    taker_buy_quote_volume: Optional[float]


@dataclass(frozen=True)
class MarketStructureTakerBuySellVolPoint:
    symbol: str
    event_time: int
    buy_sell_ratio: Optional[float]
    buy_vol: Optional[float]
    sell_vol: Optional[float]


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


def _build_open_interest_point(row):
    return MarketStructureOpenInterestPoint(
        symbol=row.symbol,
        event_time=int(row.event_time),
        sum_open_interest=float(row.sum_open_interest) if row.sum_open_interest is not None else None,
        sum_open_interest_value=float(row.sum_open_interest_value) if row.sum_open_interest_value is not None else None,
    )


def _build_kline_point(row):
    return MarketStructureKlinePoint(
        symbol=row.symbol,
        open_time=int(row.open_time),
        high_price=float(row.high_price) if hasattr(row, 'high_price') and row.high_price is not None else None,
        low_price=float(row.low_price) if hasattr(row, 'low_price') and row.low_price is not None else None,
        close_price=float(row.close_price) if row.close_price is not None else None,
        quote_volume=float(row.quote_volume) if row.quote_volume is not None else None,
        taker_buy_quote_volume=(
            float(row.taker_buy_quote_volume)
            if getattr(row, 'taker_buy_quote_volume', None) is not None
            else None
        ),
    )


def _build_taker_buy_sell_vol_point(row):
    return MarketStructureTakerBuySellVolPoint(
        symbol=row.symbol,
        event_time=int(row.event_time),
        buy_sell_ratio=float(row.buy_sell_ratio) if row.buy_sell_ratio is not None else None,
        buy_vol=float(row.buy_vol) if row.buy_vol is not None else None,
        sell_vol=float(row.sell_vol) if row.sell_vol is not None else None,
    )


def _get_enabled_exchanges():
    return _normalize_exchange_list(ENABLED_EXCHANGES)


def _load_market_structure_exchange_maps_clickhouse(
    exchange,
    symbols,
    upper_bound=None,
    kline_lookback_ms=None,
    include_quote_volume_24h=True,
):
    """Load the same point maps as the ORM path directly from ClickHouse."""
    from types import SimpleNamespace

    repo = get_clickhouse_repository()
    symbols = list(symbols or [])
    if not symbols:
        return ({}, {}, {}, {})

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
        taker_periods = ['5m']

    upper = int(upper_bound) if upper_bound is not None else None
    oi_lower = max(0, upper - _MARKET_STRUCTURE_CONTEXT_LOOKBACK_MS) if upper is not None else None
    kline_lower = max(0, upper - int(kline_lookback_ms or _MARKET_STRUCTURE_KLINE_LOOKBACK_MS)) if upper is not None else None
    taker_lower = oi_lower
    quote_lower = max(0, upper - _MARKET_STRUCTURE_VOLUME_24H_LOOKBACK_MS) if upper is not None else None

    oi_rows = repo.market_rows(
        'market_open_interest_hist',
        'symbol, event_time, sum_open_interest, sum_open_interest_value',
        symbols=symbols, exchange=exchange, period='5m', time_column='event_time',
        lower_bound=oi_lower, upper_bound=upper, order_by='symbol, event_time',
    )
    kline_rows = repo.market_rows(
        'market_klines',
        'symbol, open_time, high_price, low_price, close_price, quote_volume, taker_buy_quote_volume',
        symbols=symbols, exchange=exchange, period='5m', time_column='open_time',
        lower_bound=kline_lower, upper_bound=upper, order_by='symbol, open_time',
    )
    taker_rows_by_period = {}
    for period in taker_periods:
        taker_rows_by_period[period] = repo.market_rows(
            'market_taker_buy_sell_vol',
            'symbol, event_time, buy_sell_ratio, buy_vol, sell_vol',
            symbols=symbols, exchange=exchange, period=period, time_column='event_time',
            lower_bound=taker_lower, upper_bound=upper, order_by='symbol, event_time',
        )

    oi_map = {symbol: {} for symbol in symbols}
    for row in oi_rows:
        point = _build_open_interest_point(SimpleNamespace(**row))
        oi_map.setdefault(point.symbol, {})[point.event_time] = point

    kline_map = {symbol: {} for symbol in symbols}
    for row in kline_rows:
        point = _build_kline_point(SimpleNamespace(**row))
        kline_map.setdefault(point.symbol, {})[point.open_time] = point

    taker_maps = {}
    for period, rows in taker_rows_by_period.items():
        period_map = {symbol: {} for symbol in symbols}
        for row in rows:
            point = _build_taker_buy_sell_vol_point(SimpleNamespace(**row))
            period_map.setdefault(point.symbol, {})[point.event_time] = point
        taker_maps[period] = period_map

    quote_volume_24h_map = {symbol: 0.0 for symbol in symbols}
    if include_quote_volume_24h:
        quote_volume_24h_map.update(
            repo.kline_quote_volume_by_symbol(
                symbols=symbols,
                exchange=exchange,
                period='5m',
                lower_bound=quote_lower,
                upper_bound=upper,
            )
        )

    return oi_map, kline_map, taker_maps, quote_volume_24h_map


def _get_recent_lower_bound(
    session,
    model,
    symbols,
    time_field_name,
    upper_bound=None,
    exchange=None,
    period='5m',
    lookback_ms=None,
):
    if upper_bound is not None and lookback_ms is not None:
        return max(0, int(upper_bound) - lookback_ms)

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
    return max(0, int(latest_time) - _MARKET_STRUCTURE_KLINE_LOOKBACK_MS)


def _build_change_target_times(current_times):
    target_times = set()
    for current_time in current_times:
        if current_time is None:
            continue
        current_time = int(current_time)
        target_times.add(current_time)
        for interval in MARKET_STRUCTURE_INTERVALS:
            target_times.add(current_time - _interval_to_ms(interval))
    return {timestamp for timestamp in target_times if timestamp >= 0}


def _interval_to_ms(interval):
    if interval.endswith('m'):
        return int(interval[:-1]) * 60 * 1000
    if interval.endswith('h'):
        return int(interval[:-1]) * 60 * 60 * 1000
    if interval.endswith('d'):
        return int(interval[:-1]) * 24 * 60 * 60 * 1000
    raise ValueError(f'不支持的时间周期: {interval}')


_MARKET_STRUCTURE_KLINE_LOOKBACK_MS = (MARKET_STRUCTURE_KLINE_POINTS - 1) * FIVE_MINUTES_MS
_MARKET_STRUCTURE_CONTEXT_LOOKBACK_MS = _interval_to_ms('6h')
_MARKET_STRUCTURE_VOLUME_24H_LOOKBACK_MS = (288 - 1) * FIVE_MINUTES_MS
VOLUME_RATIO_LOOKBACK_POINTS = 288
_REQUIRED_POINTS = VOLUME_RATIO_LOOKBACK_POINTS + 1


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


def _load_open_interest_model_map(session, model, symbols, upper_bound=None, exchange=None, lookback_ms=None):
    if not symbols:
        return {}

    if len(symbols) <= MARKET_STRUCTURE_BULK_QUERY_THRESHOLD:
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
        period='5m',
        lookback_ms=lookback_ms,
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


def _load_kline_model_map(session, model, symbols, upper_bound=None, exchange=None, lookback_ms=None):
    if not symbols:
        return {}

    if len(symbols) <= MARKET_STRUCTURE_BULK_QUERY_THRESHOLD and lookback_ms is None:
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
                model.high_price,
                model.low_price,
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
        lookback_ms=lookback_ms,
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


def load_market_structure_aggregated_kline_maps(
    session,
    exchange,
    symbols,
    upper_bound=None,
    intervals=None,
    lookback_points=72,
):
    """Return strict UTC-aligned 5m aggregations without transferring raw history."""
    if session is None and is_clickhouse_read():
        repo = get_clickhouse_repository()
        result = {name: {symbol: {} for symbol in symbols} for name in (intervals or {})}
        if not symbols or not intervals:
            return result
        upper = int(upper_bound) if upper_bound is not None else None
        for name, interval_ms in intervals.items():
            interval_ms = int(interval_ms)
            lower = None
            if upper is not None:
                lower = max(0, upper - (int(lookback_points) + 1) * interval_ms)
            rows = repo.aggregate_kline_rows(
                symbols=symbols,
                exchange=exchange,
                period='5m',
                interval_ms=interval_ms,
                lower_bound=lower,
                upper_bound=upper,
            )
            for row in rows:
                point = _build_kline_point(type('Row', (), row)())
                if point.high_price is None or point.low_price is None or point.close_price is None:
                    continue
                result[name].setdefault(point.symbol, {})[point.open_time] = point
        return result

    intervals = intervals or {}
    records_by_interval = {name: {symbol: {} for symbol in symbols} for name in intervals}
    if not symbols or not intervals:
        return records_by_interval

    owns_session = session is None
    session = session or get_session()
    try:
        for name, interval_ms in intervals.items():
            started_at = time.perf_counter()
            interval_ms = int(interval_ms)
            expected_points = interval_ms // FIVE_MINUTES_MS
            lower_bound = None
            if upper_bound is not None:
                # One extra bucket accommodates a partially formed latest bucket.
                lower_bound = max(0, int(upper_bound) - (int(lookback_points) + 1) * interval_ms)

            bucket_time = (
                func.floor(MarketKline.open_time / interval_ms) * interval_ms
            ).label('bucket_time')
            aggregate_query = session.query(
                MarketKline.symbol.label('symbol'),
                bucket_time,
                func.max(MarketKline.high_price).label('high_price'),
                func.min(MarketKline.low_price).label('low_price'),
                func.sum(MarketKline.quote_volume).label('quote_volume'),
                func.max(MarketKline.open_time).label('last_open_time'),
            ).filter(
                MarketKline.exchange == exchange,
                MarketKline.symbol.in_(symbols),
                MarketKline.period == '5m',
            )
            if upper_bound is not None:
                aggregate_query = aggregate_query.filter(MarketKline.open_time <= upper_bound)
            if lower_bound is not None:
                aggregate_query = aggregate_query.filter(MarketKline.open_time >= lower_bound)
            aggregate_query = aggregate_query.group_by(MarketKline.symbol, bucket_time).having(
                func.count(MarketKline.open_time) == expected_points,
                func.max(MarketKline.open_time) - func.min(MarketKline.open_time)
                == (expected_points - 1) * FIVE_MINUTES_MS,
            )
            aggregate = aggregate_query.subquery()
            rows = session.query(
                aggregate.c.symbol,
                aggregate.c.bucket_time,
                aggregate.c.high_price,
                aggregate.c.low_price,
                MarketKline.close_price,
                aggregate.c.quote_volume,
            ).join(
                aggregate,
                and_(
                    MarketKline.symbol == aggregate.c.symbol,
                    MarketKline.open_time == aggregate.c.last_open_time,
                ),
            ).filter(
                MarketKline.exchange == exchange,
                MarketKline.period == '5m',
            ).order_by(aggregate.c.symbol, aggregate.c.bucket_time).all()
            for row in rows:
                point = MarketStructureKlinePoint(
                    symbol=row.symbol,
                    open_time=int(row.bucket_time),
                    high_price=float(row.high_price) if row.high_price is not None else None,
                    low_price=float(row.low_price) if row.low_price is not None else None,
                    close_price=float(row.close_price) if row.close_price is not None else None,
                    quote_volume=float(row.quote_volume) if row.quote_volume is not None else None,
                    taker_buy_quote_volume=None,
                )
                records_by_interval[name].setdefault(point.symbol, {})[point.open_time] = point
            logger.info(
                '评分聚合 Kline 加载完成: exchange=%s interval=%s symbols=%d buckets=%d 耗时=%.2fs',
                exchange,
                name,
                len(symbols),
                len(rows),
                time.perf_counter() - started_at,
            )
    finally:
        if owns_session:
            session.close()
    return records_by_interval


def _load_taker_vol_model_map(session, model, symbols, upper_bound=None, exchange=None, period='5m', lookback_ms=None):
    if not symbols:
        return {}

    if len(symbols) <= MARKET_STRUCTURE_BULK_QUERY_THRESHOLD:
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
        lookback_ms=lookback_ms,
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


def _load_quote_volume_24h_map(session, model, symbols, upper_bound=None, exchange=None):
    if not symbols:
        return {}

    lower_bound = None
    if upper_bound is not None:
        lower_bound = max(0, int(upper_bound) - _MARKET_STRUCTURE_VOLUME_24H_LOOKBACK_MS)

    query = session.query(
        model.symbol,
        func.sum(model.quote_volume).label('quote_volume_24h'),
    ).filter(model.symbol.in_(symbols), model.period == '5m')
    if hasattr(model, 'exchange') and exchange is not None:
        query = query.filter(model.exchange == exchange)
    if upper_bound is not None:
        query = query.filter(model.open_time <= upper_bound)
    if lower_bound is not None:
        query = query.filter(model.open_time >= lower_bound)

    rows = query.group_by(model.symbol).all()
    return {
        symbol: float(quote_volume_24h)
        for symbol, quote_volume_24h in rows
        if symbol and quote_volume_24h is not None
    }


def load_market_structure_exchange_maps(
    session,
    exchange,
    symbols,
    upper_bound=None,
    kline_lookback_ms=None,
    include_quote_volume_24h=True,
):
    if session is None and is_clickhouse_read():
        return _load_market_structure_exchange_maps_clickhouse(
            exchange=exchange,
            symbols=symbols,
            upper_bound=upper_bound,
            kline_lookback_ms=kline_lookback_ms,
            include_quote_volume_24h=include_quote_volume_24h,
        )

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

    start_time = time.perf_counter()
    component_durations = {}

    def _run_component(loader):
        worker_session = get_session()
        try:
            return loader(worker_session)
        finally:
            worker_session.close()

    def _load_oi(worker_session):
        started_at = time.perf_counter()
        result = _load_open_interest_model_map(
            worker_session,
            MarketOpenInterestHist,
            symbols,
            upper_bound=upper_bound,
            exchange=exchange,
            lookback_ms=_MARKET_STRUCTURE_CONTEXT_LOOKBACK_MS,
        )
        duration = time.perf_counter() - started_at
        component_durations['oi'] = duration
        return result

    def _load_kline(worker_session):
        started_at = time.perf_counter()
        result = _load_kline_model_map(
            worker_session,
            MarketKline,
            symbols,
            upper_bound=upper_bound,
            exchange=exchange,
            lookback_ms=kline_lookback_ms or _MARKET_STRUCTURE_KLINE_LOOKBACK_MS,
        )
        duration = time.perf_counter() - started_at
        component_durations['kline'] = duration
        return result

    def _load_taker_period_map(worker_session):
        started_at = time.perf_counter()
        result = {
            period: _load_taker_vol_model_map(
                worker_session,
                MarketTakerBuySellVol,
                symbols,
                upper_bound=upper_bound,
                exchange=exchange,
                period=period,
                lookback_ms=_MARKET_STRUCTURE_CONTEXT_LOOKBACK_MS,
            )
            for period in taker_periods
        }
        duration = time.perf_counter() - started_at
        component_durations['taker'] = duration
        return result

    def _load_quote_volume_24h(worker_session):
        started_at = time.perf_counter()
        result = _load_quote_volume_24h_map(
            worker_session,
            MarketKline,
            symbols,
            upper_bound=upper_bound,
            exchange=exchange,
        )
        duration = time.perf_counter() - started_at
        component_durations['quote_volume_24h'] = duration
        return result

    component_loaders = {
        'oi': _load_oi,
        'kline': _load_kline,
        'taker': _load_taker_period_map,
    }
    if include_quote_volume_24h:
        component_loaders['quote_volume_24h'] = _load_quote_volume_24h
    component_results = {}
    max_workers = min(MARKET_STRUCTURE_COMPONENT_MAX_WORKERS, len(component_loaders))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_name = {
            executor.submit(_run_component, loader): name
            for name, loader in component_loaders.items()
        }
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            component_results[name] = future.result()

    oi_map = component_results['oi']
    kline_map = component_results['kline']
    taker_maps_by_period = component_results['taker']
    quote_volume_24h_map = component_results.get('quote_volume_24h', {})

    total_duration = time.perf_counter() - start_time
    slowest_component = 'N/A'
    slowest_duration = 0.0
    if component_durations:
        slowest_component, slowest_duration = max(component_durations.items(), key=lambda item: item[1])
    logger.info(
        '评分映射加载汇总: exchange=%s total=%.2fs slowest=%s(%.2fs) oi=%.2fs kline=%.2fs taker=%.2fs quote24h=%.2fs',
        exchange,
        total_duration,
        slowest_component,
        slowest_duration,
        component_durations.get('oi', 0.0),
        component_durations.get('kline', 0.0),
        component_durations.get('taker', 0.0),
        component_durations.get('quote_volume_24h', 0.0),
    )
    return oi_map, kline_map, taker_maps_by_period, quote_volume_24h_map
