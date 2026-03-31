import time

from coinx.coin_manager import get_active_coins
from coinx.config import (
    BINANCE_SERIES_REPAIR_BOOTSTRAP_DAYS,
    BINANCE_SERIES_REPAIR_ENABLED,
    BINANCE_SERIES_REPAIR_FUTURES_PAGE_LIMIT,
    BINANCE_SERIES_REPAIR_KLINES_PAGE_LIMIT,
    BINANCE_SERIES_REPAIR_PERIOD,
    BINANCE_SERIES_REPAIR_SLEEP_MS,
)
from coinx.repositories.binance_series import (
    get_latest_series_timestamp as get_latest_series_timestamp_from_repo,
    upsert_series_records,
)
from coinx.utils import logger
from coinx.collector.binance.series import fetch_series_payload, parse_series_payload


FIVE_MINUTES_MS = 5 * 60 * 1000
DEFAULT_REPAIR_SERIES_TYPES = [
    'klines',
    'open_interest_hist',
    'global_long_short_account_ratio',
    'top_long_short_position_ratio',
    'top_long_short_account_ratio',
]


def floor_to_completed_5m(now_ms):
    return now_ms - (now_ms % FIVE_MINUTES_MS)


def get_latest_series_timestamp(symbol, series_type, session=None):
    return get_latest_series_timestamp_from_repo(
        series_type=series_type,
        symbol=symbol,
        period=BINANCE_SERIES_REPAIR_PERIOD,
        session=session,
    )


def build_repair_window(symbol, series_type, now_ms, session=None):
    target_end_time = floor_to_completed_5m(now_ms)
    latest_local_timestamp = get_latest_series_timestamp(symbol, series_type, session=session)

    if latest_local_timestamp is None:
        start_time = target_end_time - BINANCE_SERIES_REPAIR_BOOTSTRAP_DAYS * 24 * 60 * 60 * 1000
    else:
        start_time = latest_local_timestamp + FIVE_MINUTES_MS

    return {
        'symbol': symbol,
        'series_type': series_type,
        'start_time': start_time,
        'end_time': target_end_time,
        'has_gap': start_time <= target_end_time,
        'latest_local_timestamp': latest_local_timestamp,
    }


def _get_time_field(series_type):
    return 'open_time' if series_type == 'klines' else 'event_time'


def _get_page_limit(series_type):
    if series_type == 'klines':
        return BINANCE_SERIES_REPAIR_KLINES_PAGE_LIMIT
    return BINANCE_SERIES_REPAIR_FUTURES_PAGE_LIMIT


def repair_single_series(symbol, series_type, now_ms=None, http_session=None, db_session=None):
    current_time_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    window = build_repair_window(symbol, series_type, current_time_ms, session=db_session)

    if not window['has_gap']:
        return {
            'symbol': symbol,
            'series_type': series_type,
            'period': BINANCE_SERIES_REPAIR_PERIOD,
            'status': 'skipped',
            'start_time': window['start_time'],
            'end_time': window['end_time'],
            'affected': 0,
            'pages': 0,
        }

    page_limit = _get_page_limit(series_type)
    time_field = _get_time_field(series_type)
    cursor_time = window['start_time']
    affected = 0
    pages = 0
    repaired_records = 0
    last_repaired_time = None

    while cursor_time <= window['end_time']:
        payload = fetch_series_payload(
            series_type=series_type,
            symbol=symbol,
            period=BINANCE_SERIES_REPAIR_PERIOD,
            limit=page_limit,
            session=http_session,
            start_time=cursor_time,
            end_time=window['end_time'],
        )
        records = parse_series_payload(
            series_type=series_type,
            payload=payload,
            symbol=symbol,
            period=BINANCE_SERIES_REPAIR_PERIOD,
        )
        filtered_records = [
            record
            for record in records
            if cursor_time <= record[time_field] <= window['end_time']
        ]

        if not filtered_records:
            break

        affected += upsert_series_records(series_type, filtered_records, session=db_session)
        repaired_records += len(filtered_records)
        pages += 1
        last_repaired_time = max(record[time_field] for record in filtered_records)
        next_cursor_time = last_repaired_time + FIVE_MINUTES_MS

        if next_cursor_time <= cursor_time:
            break

        if len(filtered_records) < page_limit:
            cursor_time = next_cursor_time
            break

        cursor_time = next_cursor_time
        if BINANCE_SERIES_REPAIR_SLEEP_MS > 0 and cursor_time <= window['end_time']:
            time.sleep(BINANCE_SERIES_REPAIR_SLEEP_MS / 1000)

    return {
        'symbol': symbol,
        'series_type': series_type,
        'period': BINANCE_SERIES_REPAIR_PERIOD,
        'status': 'success',
        'start_time': window['start_time'],
        'end_time': window['end_time'],
        'affected': affected,
        'records': repaired_records,
        'pages': pages,
        'last_repaired_time': last_repaired_time,
    }


def repair_tracked_symbols(symbols=None, series_types=None, now_ms=None, http_session=None, db_session=None):
    tracked_symbols = get_active_coins(filter_symbols=symbols) if symbols else get_active_coins()
    active_series_types = series_types or DEFAULT_REPAIR_SERIES_TYPES
    results = []

    for symbol in tracked_symbols:
        for series_type in active_series_types:
            try:
                result = repair_single_series(
                    symbol=symbol,
                    series_type=series_type,
                    now_ms=now_ms,
                    http_session=http_session,
                    db_session=db_session,
                )
                results.append(result)
            except Exception as exc:
                logger.error(f'Binance 历史序列修补失败: symbol={symbol}, type={series_type}, error={exc}')
                results.append(
                    {
                        'symbol': symbol,
                        'series_type': series_type,
                        'period': BINANCE_SERIES_REPAIR_PERIOD,
                        'status': 'error',
                        'error': str(exc),
                    }
                )

    success_count = sum(1 for item in results if item.get('status') == 'success')
    failure_count = sum(1 for item in results if item.get('status') == 'error')
    skipped_count = sum(1 for item in results if item.get('status') == 'skipped')

    return {
        'status': 'success',
        'symbols': tracked_symbols,
        'series_types': active_series_types,
        'period': BINANCE_SERIES_REPAIR_PERIOD,
        'success_count': success_count,
        'failure_count': failure_count,
        'skipped_count': skipped_count,
        'results': results,
    }


def run_series_repair_job():
    if not BINANCE_SERIES_REPAIR_ENABLED:
        return {
            'status': 'skipped',
            'message': 'Binance 历史序列修补任务未启用',
        }

    summary = repair_tracked_symbols()
    summary.setdefault('status', 'success')
    return summary
