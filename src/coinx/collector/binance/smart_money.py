"""Binance Futures Smart Money web BAPI collector."""

from __future__ import annotations

from typing import Any, Mapping, Optional

from coinx.collector.binance.client import (
    get_session,
    request_with_binance_retry,
)
from coinx.config import (
    BINANCE_WEB_BASE_URL,
    SMART_MONEY_REQUEST_TIMEOUT_SECONDS,
    SMART_MONEY_TIME_RANGES,
)


SMART_MONEY_LIST_URL = (
    f'{BINANCE_WEB_BASE_URL.rstrip("/")}/bapi/futures/v1/public/future/'
    'smart-money/signal/list'
)
SUPPORTED_SMART_MONEY_TIME_RANGES = ('30m', 'ALL')
SMART_MONEY_HEADERS = {
    'Accept': 'application/json',
    'clienttype': 'web',
    'lang': 'zh-CN',
    'Referer': 'https://www.binance.com/zh-CN/smart-money',
}


class SmartMoneyApiError(RuntimeError):
    """Raised when Binance returns an invalid or unsuccessful BAPI response."""


def parse_smart_money_signal_list(payload: Mapping[str, Any], time_range: str = '30m') -> list[dict[str, Any]]:
    """Validate and return the raw list rows from a successful BAPI payload."""
    if not isinstance(payload, Mapping):
        raise SmartMoneyApiError('Binance Smart Money response is not an object')
    if payload.get('code') != '000000':
        message = payload.get('message') or payload.get('messageDetail') or 'unknown Binance business error'
        raise SmartMoneyApiError(f'Binance Smart Money business error: code={payload.get("code")} message={message}')
    rows = payload.get('data')
    if not isinstance(rows, list):
        raise SmartMoneyApiError('Binance Smart Money response data is not a list')

    parsed = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        copied = dict(row)
        copied.setdefault('timeRange', time_range)
        parsed.append(copied)
    return parsed


def fetch_smart_money_signal_list(
    time_range: Optional[str] = None,
    session=None,
    timeout: Optional[int] = None,
) -> list[dict[str, Any]]:
    """Fetch the complete Smart Money list for one time window."""
    time_range = time_range or SMART_MONEY_TIME_RANGES[0]
    if time_range not in SUPPORTED_SMART_MONEY_TIME_RANGES:
        supported = ', '.join(SUPPORTED_SMART_MONEY_TIME_RANGES)
        raise ValueError(f'only {supported} are supported, got {time_range!r}')
    http_session = session or get_session()
    response = request_with_binance_retry(
        http_session,
        SMART_MONEY_LIST_URL,
        params={'timeRange': time_range},
        timeout=timeout or SMART_MONEY_REQUEST_TIMEOUT_SECONDS,
        headers=SMART_MONEY_HEADERS,
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise SmartMoneyApiError('Binance Smart Money response is not JSON') from exc
    return parse_smart_money_signal_list(payload, time_range=time_range)
