"""Queries for the cross-exchange open-interest overview."""

from typing import Any, Dict

from sqlalchemy import and_, func

from coinx.database import get_session
from coinx.models import MarketOpenInterestHist


DEFAULT_OPEN_INTEREST_PERIOD = '5m'
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200


def _as_float(value):
    return float(value) if value is not None else None


def _latest_rows(session, period, symbol=None, as_of_ms=None):
    model = MarketOpenInterestHist
    filters = [model.period == period]
    if symbol:
        filters.append(model.symbol.like(f'%{symbol}%'))
    if as_of_ms is not None:
        filters.append(model.event_time <= int(as_of_ms))

    latest = (
        session.query(
            model.exchange.label('exchange'),
            model.symbol.label('symbol'),
            func.max(model.event_time).label('event_time'),
        )
        .filter(*filters)
        .group_by(model.exchange, model.symbol)
        .subquery()
    )
    return (
        session.query(model)
        .join(
            latest,
            and_(
                model.exchange == latest.c.exchange,
                model.symbol == latest.c.symbol,
                model.event_time == latest.c.event_time,
            ),
        )
        .filter(model.period == period)
        .all()
    )


def get_open_interest_page(
    period=DEFAULT_OPEN_INTEREST_PERIOD,
    keyword='',
    min_ratio_percent=None,
    max_ratio_percent=None,
    min_open_interest=None,
    max_open_interest=None,
    only_with_supply=False,
    only_time_aligned=False,
    sort_by='ratio',
    sort_order='desc',
    page=1,
    page_size=DEFAULT_PAGE_SIZE,
    as_of_ms=None,
    session=None,
) -> Dict[str, Any]:
    """Return the latest cross-exchange OI rows grouped by symbol."""
    own_session = session is None
    db = session or get_session()
    try:
        normalized_symbol = str(keyword or '').strip().upper() or None
        rows = _latest_rows(db, period, normalized_symbol, as_of_ms=as_of_ms)

        grouped = {}
        for row in rows:
            item = grouped.setdefault(
                row.symbol,
                {
                    'symbol': row.symbol,
                    'total_open_interest': 0.0,
                    'has_open_interest': False,
                    'total_open_interest_value': 0.0,
                    'has_open_interest_value': False,
                    'exchanges': set(),
                    'exchange_details': [],
                    'event_times': set(),
                    'latest_event_time': None,
                    'binance_event_time': None,
                    'circulating_supply': None,
                    'supply_event_time': None,
                },
            )
            item['exchanges'].add(row.exchange)
            item['exchange_details'].append({
                'exchange': row.exchange,
                'open_interest': _as_float(row.sum_open_interest),
                'open_interest_value': _as_float(row.sum_open_interest_value),
                'event_time': int(row.event_time) if row.event_time is not None else None,
            })
            if row.event_time is not None:
                item['event_times'].add(int(row.event_time))
                item['latest_event_time'] = max(
                    item['latest_event_time'] or int(row.event_time),
                    int(row.event_time),
                )
            if row.exchange == 'binance':
                item['binance_event_time'] = int(row.event_time) if row.event_time is not None else None
                if row.circulating_supply is not None:
                    item['circulating_supply'] = _as_float(row.circulating_supply)
                    item['supply_event_time'] = int(row.event_time)
            if row.sum_open_interest is not None:
                item['total_open_interest'] += float(row.sum_open_interest)
                item['has_open_interest'] = True
            if row.sum_open_interest_value is not None:
                item['total_open_interest_value'] += float(row.sum_open_interest_value)
                item['has_open_interest_value'] = True

        result_rows = []
        keyword_upper = str(keyword or '').strip().upper()
        for symbol, item in grouped.items():
            if keyword_upper and keyword_upper not in symbol.upper():
                continue
            latest_event_time = max(item['event_times']) if item['event_times'] else None
            has_time_mismatch = len(item['event_times']) > 1
            if only_time_aligned and has_time_mismatch:
                continue
            exchange_details = []
            for detail in sorted(item['exchange_details'], key=lambda value: value['exchange']):
                detail = {**detail, 'is_stale': (
                    latest_event_time is not None
                    and detail['event_time'] is not None
                    and detail['event_time'] != latest_event_time
                )}
                exchange_details.append(detail)
            stale_exchanges = [
                detail['exchange'] for detail in exchange_details if detail['is_stale']
            ]
            supply = item['circulating_supply']
            total_open_interest = item['total_open_interest'] if item['has_open_interest'] else None
            total_open_interest_value = (
                item['total_open_interest_value'] if item['has_open_interest_value'] else None
            )
            ratio = (
                total_open_interest / supply
                if total_open_interest is not None and supply not in (None, 0)
                else None
            )
            ratio_percent = ratio * 100 if ratio is not None else None
            if only_with_supply and ratio_percent is None:
                continue
            if min_ratio_percent is not None and (
                ratio_percent is None or ratio_percent < float(min_ratio_percent)
            ):
                continue
            if max_ratio_percent is not None and (
                ratio_percent is None or ratio_percent > float(max_ratio_percent)
            ):
                continue
            if min_open_interest is not None and (
                total_open_interest is None or total_open_interest < float(min_open_interest)
            ):
                continue
            if max_open_interest is not None and (
                total_open_interest is None or total_open_interest > float(max_open_interest)
            ):
                continue

            result_rows.append({
                'symbol': symbol,
                'total_open_interest': total_open_interest,
                'total_open_interest_value': total_open_interest_value,
                'circulating_supply': supply,
                'oi_circulating_ratio': ratio,
                'oi_circulating_percent': ratio_percent,
                'exchange_count': len(item['exchanges']),
                'exchanges': sorted(item['exchanges']),
                'exchange_details': exchange_details,
                'anchor_exchange': 'binance' if 'binance' in item['exchanges'] else None,
                'anchor_event_time': item['binance_event_time'],
                'latest_event_time': item['latest_event_time'],
                'supply_event_time': item['supply_event_time'],
                'supply_source': 'binance_cmc' if supply is not None else None,
                'has_time_mismatch': has_time_mismatch,
                'data_quality': 'contains_stale_data' if has_time_mismatch else 'aligned',
                'stale_exchanges': stale_exchanges,
            })

        sort_keys = {
            'ratio': 'oi_circulating_percent',
            'open_interest': 'total_open_interest',
            'open_interest_value': 'total_open_interest_value',
            'circulating_supply': 'circulating_supply',
            'event_time': 'latest_event_time',
            'symbol': 'symbol',
        }
        sort_key = sort_keys.get(sort_by, sort_keys['ratio'])
        reverse = str(sort_order).lower() != 'asc'

        rows_with_value = [row for row in result_rows if row.get(sort_key) is not None]
        rows_without_value = [row for row in result_rows if row.get(sort_key) is None]
        rows_with_value.sort(key=lambda row: row[sort_key], reverse=reverse)
        result_rows = rows_with_value + rows_without_value

        total_count = len(result_rows)
        effective_page = max(1, int(page or 1))
        effective_page_size = min(MAX_PAGE_SIZE, max(1, int(page_size or DEFAULT_PAGE_SIZE)))
        start = (effective_page - 1) * effective_page_size
        page_rows = result_rows[start:start + effective_page_size]
        snapshot_times = [row['latest_event_time'] for row in page_rows if row['latest_event_time'] is not None]

        return {
            'data': page_rows,
            'total_count': total_count,
            'page': effective_page,
            'page_size': effective_page_size,
            'period': period,
            'snapshot_time': max(snapshot_times) if snapshot_times else None,
            'stats': {
                'total_symbols': total_count,
                'with_supply': sum(row['circulating_supply'] is not None for row in result_rows),
                'without_supply': sum(row['circulating_supply'] is None for row in result_rows),
                'aligned_symbols': sum(not row['has_time_mismatch'] for row in result_rows),
                'stale_symbols': sum(row['has_time_mismatch'] for row in result_rows),
                'exchanges': sorted({exchange for row in result_rows for exchange in row['exchanges']}),
            },
        }
    finally:
        if own_session:
            db.close()
