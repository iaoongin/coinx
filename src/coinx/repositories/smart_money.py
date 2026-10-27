"""Persistence for Binance Futures Smart Money list snapshots."""

from __future__ import annotations

import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Optional

from sqlalchemy import func, text
from sqlalchemy.dialects.mysql import insert as mysql_insert

from coinx import config
from coinx import database
from coinx.collector.binance.smart_money import fetch_smart_money_signal_list
from coinx.database import get_session
from coinx.models import Coin, MarketFuturesSmartMoneySignal
from coinx.repositories.funding_rate import load_latest_funding_rates
from coinx.read_backend import get_clickhouse_repository, is_clickhouse_read
from coinx.utils import logger


SIGNAL_COLUMNS = (
    'exchange', 'time_range', 'symbol', 'period', 'snapshot_time', 'side',
    'net_notional', 'long_notional', 'short_notional', 'long_traders',
    'long_whales', 'short_traders', 'short_whales', 'long_qty', 'short_qty',
    'long_avg_entry_price', 'short_avg_entry_price', 'raw_json',
    'collected_at', 'updated_at',
)
DECIMAL_FIELDS = (
    'net_notional', 'long_notional', 'short_notional', 'long_qty', 'short_qty',
    'long_avg_entry_price', 'short_avg_entry_price',
)
INTEGER_FIELDS = ('long_traders', 'long_whales', 'short_traders', 'short_whales')
DISPLAY_DECIMAL_FIELDS = (
    'net_notional', 'long_notional', 'short_notional', 'long_qty', 'short_qty',
    'long_avg_entry_price', 'short_avg_entry_price', 'mark_price',
)
DISPLAY_INTEGER_FIELDS = INTEGER_FIELDS
DISPLAY_COLUMNS = (
    'exchange', 'time_range', 'symbol', 'period', 'snapshot_time', 'side',
    'net_notional', 'long_notional', 'short_notional', 'long_traders',
    'long_whales', 'short_traders', 'short_whales', 'long_qty', 'short_qty',
    'long_avg_entry_price', 'short_avg_entry_price',
)
DISPLAY_SORT_COLUMNS = {
    'symbol': 'symbol',
    'net_notional': 'net_notional',
    'long_notional': 'long_notional',
    'short_notional': 'short_notional',
    'long_short_ratio': 'CASE WHEN short_notional = 0 THEN NULL ELSE long_notional / short_notional END',
    'long_whales': 'long_whales',
    'short_whales': 'short_whales',
}
TREND_POINT_COUNT = 168
TREND_BUCKET_MS = 60 * 60 * 1000
TRADITIONAL_UNDERLYING_TYPES = (
    'EQUITY',
    'HK_EQUITY',
    'CN_EQUITY',
    'KR_EQUITY',
    'COMMODITY',
    'INDEX',
    'FX',
    'PREMARKET',
)


def get_smart_money_scope_symbols(
    min_net_notional=None,
    time_range=None,
    period=None,
    session=None,
) -> list[str]:
    """Return symbols from the latest large-net-notional Smart Money snapshot.

    Choose the latest snapshot for the requested range/period first, then
    apply the absolute net-notional filter. Missing or unavailable Smart Money
    data is treated as an empty scope so market collection can continue.
    """
    if not config.SMART_MONEY_ENABLED:
        return []

    threshold = (
        config.SMART_MONEY_SCOPE_MIN_NET_NOTIONAL
        if min_net_notional is None
        else min_net_notional
    )
    try:
        threshold = Decimal(str(threshold))
    except (InvalidOperation, TypeError, ValueError):
        logger.warning('聪明钱采集范围阈值无效: %s', threshold)
        return []
    if not threshold.is_finite() or threshold < 0:
        logger.warning('聪明钱采集范围阈值必须为非负有限数: %s', threshold)
        return []

    exchange = 'binance'
    # The market-series expansion always follows the ALL Smart Money window.
    time_range = str(time_range or 'ALL')
    # The scope is built from the same period that is persisted by the
    # collector; keeping one source of truth prevents mismatched snapshots.
    period = str(period or config.SMART_MONEY_PERIOD)

    try:
        if session is None and is_clickhouse_read():
            repository = get_clickhouse_repository()
            table = repository._table('market_futures_smart_money_signal')
            quoted_exchange = exchange.replace("'", "''")
            quoted_range = time_range.replace("'", "''")
            quoted_period = period.replace("'", "''")
            base_where = (
                f"exchange = '{quoted_exchange}' "
                f"AND time_range = '{quoted_range}' "
                f"AND period = '{quoted_period}'"
            )
            latest_rows = repository.client.query_rows(
                f'SELECT max(snapshot_time) AS snapshot_time FROM {table} '
                f'WHERE {base_where}'
            )
            latest_snapshot = latest_rows[0].get('snapshot_time') if latest_rows else None
            if latest_snapshot is None:
                return []
            rows = repository.client.query_rows(
                f'SELECT DISTINCT symbol FROM {table} WHERE {base_where} '
                f'AND snapshot_time = {int(latest_snapshot)} '
                f'AND abs(net_notional) > {threshold}'
            )
            return [str(row.get('symbol')) for row in rows if row.get('symbol')]

        own_session = session is None
        db = session or get_session()
        try:
            latest_snapshot = db.query(func.max(MarketFuturesSmartMoneySignal.snapshot_time)).filter(
                MarketFuturesSmartMoneySignal.exchange == exchange,
                MarketFuturesSmartMoneySignal.time_range == time_range,
                MarketFuturesSmartMoneySignal.period == period,
            ).scalar()
            if latest_snapshot is None:
                return []
            rows = db.query(MarketFuturesSmartMoneySignal.symbol).filter(
                MarketFuturesSmartMoneySignal.exchange == exchange,
                MarketFuturesSmartMoneySignal.time_range == time_range,
                MarketFuturesSmartMoneySignal.period == period,
                MarketFuturesSmartMoneySignal.snapshot_time == latest_snapshot,
                func.abs(MarketFuturesSmartMoneySignal.net_notional) > threshold,
            ).distinct().all()
            return [row[0] for row in rows if row and row[0]]
        finally:
            if own_session:
                db.close()
    except Exception as exc:
        logger.debug('读取聪明钱采集范围失败，将跳过扩展范围: %s', exc)
        return []


def load_latest_smart_money_for_symbol(
    symbol: str,
    as_of_ms: Optional[int] = None,
    session=None,
) -> list[dict[str, Any]]:
    """Load the latest persisted Smart Money row for each configured window."""
    normalized_symbol = str(symbol or '').strip().upper()
    if not normalized_symbol:
        return []

    own_session = session is None
    db = session or database.get_session()
    try:
        time_ranges = list(dict.fromkeys([
            *(config.SMART_MONEY_TIME_RANGES or []),
            '30m',
            'ALL',
        ]))
        rows = []
        for time_range in time_ranges:
            query = db.query(MarketFuturesSmartMoneySignal).filter(
                MarketFuturesSmartMoneySignal.exchange == 'binance',
                MarketFuturesSmartMoneySignal.time_range == time_range,
                MarketFuturesSmartMoneySignal.period == config.SMART_MONEY_PERIOD,
                MarketFuturesSmartMoneySignal.symbol == normalized_symbol,
            )
            if as_of_ms is not None:
                query = query.filter(MarketFuturesSmartMoneySignal.snapshot_time <= int(as_of_ms))
            row = query.order_by(MarketFuturesSmartMoneySignal.snapshot_time.desc()).first()
            if row is None:
                continue
            rows.append(_display_row({
                'exchange': row.exchange,
                'time_range': row.time_range,
                'symbol': row.symbol,
                'period': row.period,
                'snapshot_time': row.snapshot_time,
                'side': row.side,
                'net_notional': row.net_notional,
                'long_notional': row.long_notional,
                'short_notional': row.short_notional,
                'long_traders': row.long_traders,
                'long_whales': row.long_whales,
                'short_traders': row.short_traders,
                'short_whales': row.short_whales,
                'long_qty': row.long_qty,
                'short_qty': row.short_qty,
                'long_avg_entry_price': row.long_avg_entry_price,
                'short_avg_entry_price': row.short_avg_entry_price,
            }))
        rows = _attach_mark_prices(rows, session=db)
        rows = _attach_price_trends(rows, session=db)
        return rows
    except Exception as exc:
        logger.debug(
            '读取合约聪明钱详情失败: symbol=%s error_type=%s error=%s',
            normalized_symbol,
            type(exc).__name__,
            str(exc)[:200],
        )
        return []
    finally:
        if own_session:
            db.close()


def _display_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Convert database scalar values into JSON-safe values for the page."""
    result = dict(row)
    for field in DISPLAY_DECIMAL_FIELDS:
        value = result.get(field)
        if value is not None:
            try:
                result[field] = float(value)
            except (TypeError, ValueError):
                result[field] = None
    for field in DISPLAY_INTEGER_FIELDS:
        value = result.get(field)
        if value is not None:
            try:
                result[field] = int(value)
            except (TypeError, ValueError):
                result[field] = None
    try:
        long_notional = Decimal(str(result.get('long_notional')))
        short_notional = Decimal(str(result.get('short_notional')))
        result['long_short_ratio'] = (
            float(long_notional / short_notional)
            if short_notional != 0
            else None
        )
    except (InvalidOperation, TypeError, ValueError, ZeroDivisionError):
        result['long_short_ratio'] = None
    if result.get('snapshot_time') is not None:
        result['snapshot_time'] = int(result['snapshot_time'])
    return result


def _smart_money_page_result(rows, total_count, snapshot_time, snapshots, stats):
    return {
        'data': [_display_row(row) for row in rows],
        'total_count': int(total_count or 0),
        'snapshot_time': int(snapshot_time) if snapshot_time is not None else None,
        'snapshots': [int(value) for value in snapshots if value is not None],
        'stats': {
            'net_notional': float(stats.get('net_notional') or 0),
            'long_notional': float(stats.get('long_notional') or 0),
            'short_notional': float(stats.get('short_notional') or 0),
        },
    }


def _attach_mark_prices(rows, session=None):
    """Add the latest locally collected mark price to each page row."""
    normalized_rows = [dict(row) for row in rows]
    symbols = [row.get('symbol') for row in normalized_rows if row.get('symbol')]
    if not symbols:
        return normalized_rows
    prices = load_latest_funding_rates(symbols=symbols, session=session)
    for row in normalized_rows:
        funding = prices.get(row.get('symbol')) or {}
        row['mark_price'] = funding.get('mark_price')
    return normalized_rows


def _build_price_trend_map(raw_rows):
    """Keep the last 5m close in each hourly bucket for compact sparklines."""
    buckets = {}
    for raw_row in raw_rows:
        symbol = raw_row.get('symbol')
        try:
            open_time = int(raw_row.get('open_time'))
            close_price = float(raw_row.get('close_price'))
        except (TypeError, ValueError):
            continue
        if not symbol:
            continue
        bucket_time = open_time - open_time % TREND_BUCKET_MS
        current = buckets.setdefault(str(symbol), {}).get(bucket_time)
        if current is None or open_time > current[0]:
            buckets[str(symbol)][bucket_time] = (open_time, close_price)
    return {
        symbol: [
            {'time': int(bucket_time), 'value': value[1]}
            for bucket_time, value in sorted(symbol_buckets.items())
        ]
        for symbol, symbol_buckets in buckets.items()
    }


def load_binance_price_trends(symbols, point_count=TREND_POINT_COUNT, session=None):
    """Load compact hourly Binance close series for the requested symbols."""
    normalized_symbols = list(dict.fromkeys(symbol for symbol in symbols if symbol))
    trend_map = {symbol: [] for symbol in normalized_symbols}
    if not normalized_symbols:
        return trend_map

    point_count = max(2, int(point_count or TREND_POINT_COUNT))
    try:
        if session is None and is_clickhouse_read():
            repository = get_clickhouse_repository()
            quoted_symbols = ', '.join(
                f"'{str(symbol).replace(chr(39), chr(39) * 2)}'" for symbol in normalized_symbols
            )
            table = f'{repository.database}.market_klines'
            latest = repository.client.query_scalar(
                f"SELECT max(open_time) FROM {table} "
                f"WHERE exchange = 'binance' AND period = '5m' AND symbol IN ({quoted_symbols})"
            )
            if latest is None:
                return trend_map
            latest_bucket = int(latest) - int(latest) % TREND_BUCKET_MS
            lower_bound = latest_bucket - (point_count - 1) * TREND_BUCKET_MS
            raw_rows = repository.aggregate_kline_rows(
                symbols=normalized_symbols,
                exchange='binance',
                interval_ms=TREND_BUCKET_MS,
                lower_bound=lower_bound,
                upper_bound=int(latest),
                period='5m',
            )
        else:
            own_session = session is None
            db = session or get_session()
            try:
                symbol_params = {
                    f'trend_symbol_{index}': symbol
                    for index, symbol in enumerate(normalized_symbols)
                }
                symbol_placeholders = ', '.join(f':{key}' for key in symbol_params)
                base_params = {
                    'trend_exchange': 'binance',
                    'trend_period': '5m',
                    **symbol_params,
                }
                latest = db.execute(
                    text(
                        'SELECT MAX(open_time) FROM market_klines '
                        f'WHERE exchange = :trend_exchange AND period = :trend_period '
                        f'AND symbol IN ({symbol_placeholders})'
                    ),
                    base_params,
                ).scalar()
                if latest is None:
                    return trend_map
                latest_bucket = int(latest) - int(latest) % TREND_BUCKET_MS
                lower_bound = latest_bucket - (point_count - 1) * TREND_BUCKET_MS
                raw_rows = db.execute(
                    text(
                        'SELECT symbol, open_time, close_price FROM market_klines '
                        f'WHERE exchange = :trend_exchange AND period = :trend_period '
                        f'AND symbol IN ({symbol_placeholders}) '
                        'AND open_time >= :trend_lower_bound AND open_time <= :trend_upper_bound '
                        'ORDER BY symbol, open_time'
                    ),
                    {
                        **base_params,
                        'trend_lower_bound': lower_bound,
                        'trend_upper_bound': int(latest),
                    },
                ).mappings().all()
            finally:
                if own_session:
                    db.close()

        built_map = _build_price_trend_map(raw_rows)
        for symbol in normalized_symbols:
            trend_map[symbol] = built_map.get(symbol, [])
    except Exception as exc:
        logger.debug(
            '加载 Binance 价格走势失败: error_type=%s error=%s',
            type(exc).__name__,
            str(exc)[:200],
        )
    return trend_map


def _attach_price_trends(rows, session=None):
    """Attach a compact seven-day hourly close series to each page row."""
    normalized_rows = [dict(row) for row in rows]
    symbols = [row.get('symbol') for row in normalized_rows if row.get('symbol')]
    trend_map = load_binance_price_trends(symbols, session=session)
    for row in normalized_rows:
        row['price_trend'] = trend_map.get(row.get('symbol'), [])
    return normalized_rows


def _smart_money_filters(
    keyword,
    side,
    exchange,
    time_range,
    period,
    snapshot_time,
    min_net_notional=None,
    underlying_type=None,
    asset_symbols=None,
):
    filters = [
        'exchange = :exchange',
        'time_range = :time_range',
        'period = :period',
    ]
    params = {
        'exchange': exchange,
        'time_range': time_range,
        'period': period,
    }
    if keyword:
        filters.append('symbol LIKE :keyword')
        params['keyword'] = f'%{keyword}%'
    if side:
        filters.append('side = :side')
        params['side'] = side
    if snapshot_time is not None:
        filters.append('snapshot_time = :snapshot_time')
        params['snapshot_time'] = int(snapshot_time)
    if min_net_notional is not None:
        filters.append('ABS(net_notional) > :min_net_notional')
        params['min_net_notional'] = float(min_net_notional)
    if underlying_type:
        if not asset_symbols:
            filters.append('1 = 0')
        else:
            symbol_placeholders = []
            for index, symbol in enumerate(asset_symbols):
                placeholder = f'asset_symbol_{index}'
                symbol_placeholders.append(f':{placeholder}')
                params[placeholder] = symbol
            filters.append(f"symbol IN ({', '.join(symbol_placeholders)})")
    return filters, params


def _normalize_underlying_type(value):
    if value is None or str(value).strip() == '':
        return None
    return str(value).strip().upper()


def _get_symbols_by_underlying_type(underlying_type, session=None):
    if not underlying_type:
        return None
    own_session = session is None
    db = session or get_session()
    try:
        type_filter = (
            Coin.underlying_type.in_(TRADITIONAL_UNDERLYING_TYPES)
            if underlying_type == 'TRADITIONAL'
            else Coin.underlying_type == underlying_type
        )
        return [
            row[0]
            for row in db.query(Coin.symbol)
            .filter(type_filter)
            .all()
            if row[0]
        ]
    finally:
        if own_session:
            db.close()


def _normalize_min_net_notional(value):
    if value is None or str(value).strip() == '':
        return None
    try:
        minimum = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError('min_net_notional must be a non-negative number') from exc
    if not minimum.is_finite() or minimum < 0:
        raise ValueError('min_net_notional must be a non-negative number')
    return minimum


def _normalize_max_mark_price_spread_percent(value):
    if value is None or str(value).strip() == '':
        return None
    try:
        maximum = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError('max_mark_price_spread_percent must be a non-negative number') from exc
    if not maximum.is_finite() or maximum < 0:
        raise ValueError('max_mark_price_spread_percent must be a non-negative number')
    return maximum


def _normalize_max_price_trend_percent(value):
    if value is None or str(value).strip() == '':
        return None
    try:
        minimum = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError('max_price_trend_percent must be a finite number') from exc
    if not minimum.is_finite():
        raise ValueError('max_price_trend_percent must be a finite number')
    return minimum


def _normalize_max_price_drawdown_percent(value):
    if value is None or str(value).strip() == '':
        return None
    try:
        maximum = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError('max_price_drawdown_percent must be a finite number') from exc
    if not maximum.is_finite():
        raise ValueError('max_price_drawdown_percent must be a finite number')
    return maximum


def _price_distance_percent(average_price, mark_price):
    average = _decimal(average_price)
    mark = _decimal(mark_price)
    if average is None or mark is None or not average.is_finite() or not mark.is_finite():
        return None
    if average == 0:
        return None
    return abs(mark - average) / abs(average) * Decimal('100')


def _filter_rows_by_mark_price_spread(rows, maximum_percent):
    """Keep rows where either the long or short average is near mark price."""
    if maximum_percent is None:
        return list(rows)
    filtered = []
    for row in rows:
        distances = (
            _price_distance_percent(row.get('long_avg_entry_price'), row.get('mark_price')),
            _price_distance_percent(row.get('short_avg_entry_price'), row.get('mark_price')),
        )
        if any(distance is not None and distance < maximum_percent for distance in distances):
            filtered.append(row)
    return filtered


def _price_trend_change_percent(row):
    trend = row.get('price_trend') or []
    values = [
        _decimal(point.get('value'))
        for point in trend
        if isinstance(point, dict)
    ]
    values = [value for value in values if value is not None and value.is_finite()]
    if len(values) < 2 or values[0] == 0:
        return None
    return (values[-1] - values[0]) / abs(values[0]) * Decimal('100')


def _filter_rows_by_max_price_trend(rows, maximum_percent):
    if maximum_percent is None:
        return list(rows)
    filtered = []
    for row in rows:
        change = _price_trend_change_percent(row)
        if change is not None and change <= maximum_percent:
            filtered.append(row)
    return filtered


def _price_trend_drawdown_percent(row):
    trend = row.get('price_trend') or []
    values = [
        _decimal(point.get('value'))
        for point in trend
        if isinstance(point, dict)
    ]
    values = [value for value in values if value is not None and value.is_finite()]
    if len(values) < 2:
        return None
    peak = max(values)
    if peak == 0:
        return None
    return (values[-1] - peak) / abs(peak) * Decimal('100')


def _filter_rows_by_max_price_drawdown(rows, maximum_percent):
    if maximum_percent is None:
        return list(rows)
    filtered = []
    for row in rows:
        drawdown = _price_trend_drawdown_percent(row)
        if drawdown is not None and drawdown <= maximum_percent:
            filtered.append(row)
    return filtered


def _sum_smart_money_stats(rows):
    stats = {'net_notional': Decimal('0'), 'long_notional': Decimal('0'), 'short_notional': Decimal('0')}
    for row in rows:
        for field in stats:
            value = _decimal(row.get(field))
            if value is not None and value.is_finite():
                stats[field] += value
    return stats


def load_smart_money_page(
    keyword: str = '',
    side: str = '',
    sort_by: str = 'net_notional',
    sort_order: str = 'desc',
    page: int = 1,
    page_size: int = 100,
    snapshot_time: Optional[int] = None,
    min_net_notional=None,
    max_mark_price_spread_percent=None,
    max_price_trend_percent=None,
    max_price_drawdown_percent=None,
    underlying_type=None,
    exchange: str = 'binance',
    time_range: str = 'ALL',
    period: str = '15m',
    session=None,
) -> dict[str, Any]:
    """Load one Smart Money snapshot page for the display API."""
    if time_range not in {'30m', 'ALL'}:
        raise ValueError('time_range must be 30m or ALL')
    if side not in {'', 'BUY', 'SELL'}:
        raise ValueError('side must be BUY, SELL or empty')
    page = max(int(page or 1), 1)
    page_size = min(max(int(page_size or 100), 1), 500)
    min_net_notional = _normalize_min_net_notional(min_net_notional)
    max_mark_price_spread_percent = _normalize_max_mark_price_spread_percent(
        max_mark_price_spread_percent,
    )
    max_price_trend_percent = _normalize_max_price_trend_percent(max_price_trend_percent)
    max_price_drawdown_percent = _normalize_max_price_drawdown_percent(max_price_drawdown_percent)
    underlying_type = _normalize_underlying_type(underlying_type)
    sort_column = DISPLAY_SORT_COLUMNS.get(sort_by, 'net_notional')
    order = 'ASC' if str(sort_order).lower() == 'asc' else 'DESC'
    keyword = (keyword or '').strip().upper()

    if session is None and is_clickhouse_read():
        repository = get_clickhouse_repository()
        table = f'{repository.database}.market_futures_smart_money_signal FINAL'
        filters = [
            f"exchange = '{str(exchange).replace(chr(39), chr(39) * 2)}'",
            f"time_range = '{str(time_range).replace(chr(39), chr(39) * 2)}'",
            f"period = '{str(period).replace(chr(39), chr(39) * 2)}'",
        ]
        if keyword:
            escaped_keyword = keyword.replace('\\', '\\\\').replace("'", "''")
            filters.append(f"symbol LIKE '%{escaped_keyword}%'")
        if side:
            filters.append(f"side = '{side}'")
        if snapshot_time is not None:
            filters.append(f'snapshot_time = {int(snapshot_time)}')
        if min_net_notional is not None:
            filters.append(f'abs(net_notional) > {min_net_notional}')
        if underlying_type:
            asset_symbols = _get_symbols_by_underlying_type(underlying_type)
            if not asset_symbols:
                return _smart_money_page_result([], 0, None, [], {})
            quoted_symbols = ', '.join(
                f"'{str(symbol).replace(chr(39), chr(39) * 2)}'"
                for symbol in asset_symbols
            )
            filters.append(f'symbol IN ({quoted_symbols})')
        where_sql = ' AND '.join(filters)
        if snapshot_time is None:
            latest_rows = repository.client.query_rows(
                f'SELECT max(snapshot_time) AS snapshot_time FROM {table} WHERE {where_sql}'
            )
            snapshot_time = latest_rows[0].get('snapshot_time') if latest_rows else None
            if snapshot_time is None:
                return _smart_money_page_result([], 0, None, [], {})
            where_sql = f'{where_sql} AND snapshot_time = {int(snapshot_time)}'
        offset = (page - 1) * page_size
        if (
            max_mark_price_spread_percent is not None
            or max_price_trend_percent is not None
            or max_price_drawdown_percent is not None
        ):
            rows = repository.client.query_rows(
                f"SELECT {', '.join(DISPLAY_COLUMNS)} FROM {table} WHERE {where_sql} "
                f'ORDER BY {sort_column} {order}, symbol ASC'
            )
            rows = _attach_mark_prices(rows)
            if max_price_trend_percent is not None or max_price_drawdown_percent is not None:
                rows = _attach_price_trends(rows)
            rows = _filter_rows_by_mark_price_spread(rows, max_mark_price_spread_percent)
            rows = _filter_rows_by_max_price_trend(rows, max_price_trend_percent)
            rows = _filter_rows_by_max_price_drawdown(rows, max_price_drawdown_percent)
            total_count = len(rows)
            stats = _sum_smart_money_stats(rows)
            rows = rows[offset:offset + page_size]
        else:
            rows = repository.client.query_rows(
                f"SELECT {', '.join(DISPLAY_COLUMNS)} FROM {table} WHERE {where_sql} "
                f'ORDER BY {sort_column} {order}, symbol ASC LIMIT {page_size} OFFSET {offset}'
            )
            count_rows = repository.client.query_rows(
                f'SELECT count() AS total_count FROM {table} WHERE {where_sql}'
            )
            stats_rows = repository.client.query_rows(
                f'SELECT sum(net_notional) AS net_notional, sum(long_notional) AS long_notional, '
                f'sum(short_notional) AS short_notional FROM {table} WHERE {where_sql}'
            )
            total_count = count_rows[0].get('total_count', 0) if count_rows else 0
            stats = stats_rows[0] if stats_rows else {}
            rows = _attach_mark_prices(rows)
        if max_price_trend_percent is None and max_price_drawdown_percent is None:
            rows = _attach_price_trends(rows)
        snapshot_rows = repository.client.query_rows(
            f'SELECT DISTINCT snapshot_time FROM {table} WHERE '
            f"exchange = '{str(exchange).replace(chr(39), chr(39) * 2)}' "
            f"AND time_range = '{str(time_range)}' AND period = '{str(period)}' "
            'ORDER BY snapshot_time DESC LIMIT 48'
        )
        return _smart_money_page_result(
            rows,
            total_count,
            snapshot_time,
            [row.get('snapshot_time') for row in snapshot_rows],
            stats,
        )

    own_session = session is None
    db = session or get_session()
    try:
        asset_symbols = _get_symbols_by_underlying_type(underlying_type, session=db)
        base_filters, params = _smart_money_filters(
            keyword,
            side,
            exchange,
            time_range,
            period,
            snapshot_time,
            min_net_notional,
            underlying_type,
            asset_symbols,
        )
        base_where = ' AND '.join(base_filters)
        if snapshot_time is None:
            latest_sql = text(
                f'SELECT MAX(snapshot_time) FROM market_futures_smart_money_signal WHERE {base_where}'
            )
            snapshot_time = db.execute(latest_sql, params).scalar()
            if snapshot_time is None:
                return _smart_money_page_result([], 0, None, [], {})
            base_filters.append('snapshot_time = :snapshot_time')
            params['snapshot_time'] = int(snapshot_time)
            base_where = ' AND '.join(base_filters)

        offset = (page - 1) * page_size
        if (
            max_mark_price_spread_percent is not None
            or max_price_trend_percent is not None
            or max_price_drawdown_percent is not None
        ):
            rows = db.execute(
                text(
                    f'SELECT {", ".join(DISPLAY_COLUMNS)} '
                    f'FROM market_futures_smart_money_signal WHERE {base_where} '
                    f'ORDER BY {sort_column} {order}, symbol ASC'
                ),
                params,
            ).mappings().all()
            rows = _attach_mark_prices(rows, session=db)
            if max_price_trend_percent is not None or max_price_drawdown_percent is not None:
                rows = _attach_price_trends(rows, session=db)
            rows = _filter_rows_by_mark_price_spread(rows, max_mark_price_spread_percent)
            rows = _filter_rows_by_max_price_trend(rows, max_price_trend_percent)
            rows = _filter_rows_by_max_price_drawdown(rows, max_price_drawdown_percent)
            count = len(rows)
            stats = _sum_smart_money_stats(rows)
            rows = rows[offset:offset + page_size]
        else:
            count = db.execute(
                text(f'SELECT COUNT(*) FROM market_futures_smart_money_signal WHERE {base_where}'),
                params,
            ).scalar()
            rows = db.execute(
                text(
                    f'SELECT {", ".join(DISPLAY_COLUMNS)} '
                    f'FROM market_futures_smart_money_signal WHERE {base_where} '
                    f'ORDER BY {sort_column} {order}, symbol ASC LIMIT :limit OFFSET :offset'
                ),
                {**params, 'limit': page_size, 'offset': offset},
            ).mappings().all()
            rows = _attach_mark_prices(rows, session=db)
            stats = db.execute(
                text(
                    f'SELECT SUM(net_notional) AS net_notional, '
                    f'SUM(long_notional) AS long_notional, SUM(short_notional) AS short_notional '
                    f'FROM market_futures_smart_money_signal WHERE {base_where}'
                ),
                params,
            ).mappings().one()
        if max_price_trend_percent is None and max_price_drawdown_percent is None:
            rows = _attach_price_trends(rows, session=db)
        snapshot_filters, snapshot_params = _smart_money_filters(
            keyword,
            side,
            exchange,
            time_range,
            period,
            None,
            min_net_notional,
            underlying_type,
            asset_symbols,
        )
        snapshot_rows = db.execute(
            text(
                'SELECT DISTINCT snapshot_time FROM market_futures_smart_money_signal '
                f"WHERE {' AND '.join(snapshot_filters)} ORDER BY snapshot_time DESC LIMIT 48"
            ),
            snapshot_params,
        ).scalars().all()
        return _smart_money_page_result(rows, count, snapshot_time, snapshot_rows, stats)
    finally:
        if own_session:
            db.close()


def align_snapshot_time(now_ms: Optional[int] = None, period_seconds: Optional[int] = None) -> int:
    """Return the local collection bucket in milliseconds."""
    current = int(time.time() * 1000) if now_ms is None else int(now_ms)
    seconds = int(period_seconds or config.SMART_MONEY_INTERVAL)
    bucket_ms = max(seconds, 1) * 1000
    return current - current % bucket_ms


def _decimal(value: Any) -> Optional[Decimal]:
    if value is None or value == '':
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _integer(value: Any) -> Optional[int]:
    if value is None or value == '':
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_smart_money_row(
    row: Mapping[str, Any],
    snapshot_time: int,
    period: str = '15m',
    exchange: str = 'binance',
    time_range: str = '30m',
    collected_at: Optional[datetime] = None,
) -> dict[str, Any]:
    """Map the web payload to typed columns while retaining the full row."""
    now = collected_at or datetime.now()
    values = {
        'exchange': exchange,
        'time_range': time_range,
        'symbol': str(row.get('symbol') or '').strip(),
        'period': period,
        'snapshot_time': int(snapshot_time),
        'side': row.get('side'),
        'raw_json': dict(row),
        'collected_at': now,
        'updated_at': now,
    }
    for field in DECIMAL_FIELDS:
        source = {
            'net_notional': 'netNotional',
            'long_notional': 'longNotional',
            'short_notional': 'shortNotional',
            'long_qty': 'longQty',
            'short_qty': 'shortQty',
            'long_avg_entry_price': 'longAvgEntryPrice',
            'short_avg_entry_price': 'shortAvgEntryPrice',
        }[field]
        values[field] = _decimal(row.get(source))
    for field in INTEGER_FIELDS:
        source = {
            'long_traders': 'longTraders',
            'long_whales': 'longWhales',
            'short_traders': 'shortTraders',
            'short_whales': 'shortWhales',
        }[field]
        values[field] = _integer(row.get(source))
    return values


def _record_values(records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {key: value for key, value in dict(record).items() if key in SIGNAL_COLUMNS}
        for record in records
    ]


def save_smart_money_signals(records: Iterable[Mapping[str, Any]], session=None) -> int:
    """Persist normalized rows using the configured market write backend."""
    rows = _record_values(records)
    if not rows:
        return 0

    from coinx.write_backend import (
        get_clickhouse_write_repository,
        is_clickhouse_write,
        market_write_lock,
    )

    if is_clickhouse_write():
        with market_write_lock('market_futures_smart_money_signal', 'global'):
            return get_clickhouse_write_repository().insert_rows(
                'market_futures_smart_money_signal',
                SIGNAL_COLUMNS,
                rows,
                batch_id=f'smart_money_{time.time_ns()}',
            )

    own_session = session is None
    db = session or get_session()
    try:
        # The scheduler normally initializes all ORM tables at startup. Keep
        # this check at the write boundary as well for manually launched jobs
        # and deployments that start workers without the runtime bootstrap.
        MarketFuturesSmartMoneySignal.__table__.create(bind=db.get_bind(), checkfirst=True)
        dialect = db.bind.dialect.name
        if config.DB_TYPE == 'starrocks' and dialect == 'mysql':
            db.execute(MarketFuturesSmartMoneySignal.__table__.insert().values(rows))
        elif dialect == 'mysql':
            statement = mysql_insert(MarketFuturesSmartMoneySignal).values(rows)
            update_values = {
                column.name: getattr(statement.inserted, column.name)
                for column in MarketFuturesSmartMoneySignal.__table__.columns
                if column.name not in {'id', 'exchange', 'time_range', 'symbol', 'period', 'snapshot_time'}
            }
            db.execute(statement.on_duplicate_key_update(**update_values))
        else:
            for row in rows:
                identity = {
                    key: row[key]
                    for key in ('exchange', 'time_range', 'symbol', 'period', 'snapshot_time')
                }
                existing = db.query(MarketFuturesSmartMoneySignal).filter_by(**identity).one_or_none()
                if existing is None:
                    db.add(MarketFuturesSmartMoneySignal(**row))
                else:
                    for key, value in row.items():
                        if key != 'id':
                            setattr(existing, key, value)
        db.commit()
        return len(rows)
    except Exception:
        db.rollback()
        raise
    finally:
        if own_session:
            db.close()


def collect_and_save_smart_money_signals(
    session=None,
    http_session=None,
    now_ms: Optional[int] = None,
    time_range: Optional[str] = None,
    snapshot_time: Optional[int] = None,
) -> dict[str, Any]:
    """Fetch and persist one complete Smart Money list snapshot."""
    time_range = time_range or config.SMART_MONEY_TIME_RANGES[0]
    snapshot_time = snapshot_time or align_snapshot_time(now_ms=now_ms)
    started = time.perf_counter()
    rows = fetch_smart_money_signal_list(time_range=time_range, session=http_session)
    normalized = [
        normalize_smart_money_row(
            row,
            snapshot_time=snapshot_time,
            period=config.SMART_MONEY_PERIOD,
            time_range=time_range,
        )
        for row in rows
    ]
    duplicate_symbols = len(rows) - len({row.get('symbol') for row in rows if row.get('symbol')})
    written = save_smart_money_signals(normalized, session=session)
    return {
        'status': 'success',
        'time_range': time_range,
        'period': config.SMART_MONEY_PERIOD,
        'list_rows': len(rows),
        'list_written': written,
        'duplicate_symbols': duplicate_symbols,
        'snapshot_time': snapshot_time,
        'duration_ms': round((time.perf_counter() - started) * 1000, 2),
    }
