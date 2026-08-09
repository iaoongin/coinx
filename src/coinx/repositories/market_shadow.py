"""Asynchronous ClickHouse shadow reads for MySQL-backed business queries.

Shadow reads are best-effort diagnostics. The caller has already received the
MySQL result, and any ClickHouse connection/query failure is logged without
changing the request result.
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Sequence, Tuple

from coinx import config
from coinx.read_clients import ClickHouseReadClient
from coinx.repositories.market_read import ClickHouseMarketReadRepository


logger = logging.getLogger(__name__)


_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="coinx-ck-shadow")
_INFLIGHT_LOCK = threading.Lock()
_INFLIGHT_KEYS = set()
_CLIENT_LOCK = threading.Lock()
_CLIENT: Optional[ClickHouseReadClient] = None
_REPOSITORY: Optional[ClickHouseMarketReadRepository] = None


def _enabled() -> bool:
    return bool(config.CLICKHOUSE_READ_SHADOW)


def _get_repository() -> ClickHouseMarketReadRepository:
    global _CLIENT, _REPOSITORY
    if _REPOSITORY is not None:
        return _REPOSITORY
    with _CLIENT_LOCK:
        if _REPOSITORY is None:
            if not config.CLICKHOUSE_URL:
                raise RuntimeError("CLICKHOUSE_URL is required when CLICKHOUSE_READ_SHADOW=true")
            timeout_seconds = max(5, int(config.CLICKHOUSE_READ_TIMEOUT_SECONDS))
            _CLIENT = ClickHouseReadClient(
                config.CLICKHOUSE_URL,
                config.CLICKHOUSE_DATABASE,
                config.CLICKHOUSE_USER,
                config.CLICKHOUSE_PASSWORD,
                timeout=(5, timeout_seconds),
            )
            _REPOSITORY = ClickHouseMarketReadRepository(_CLIENT, config.CLICKHOUSE_DATABASE)
    return _REPOSITORY


def _submit(key: Tuple[Any, ...], callback) -> None:
    if not _enabled():
        return
    with _INFLIGHT_LOCK:
        if key in _INFLIGHT_KEYS:
            return
        _INFLIGHT_KEYS.add(key)

    def run() -> None:
        try:
            callback()
        except Exception:
            logger.exception("ClickHouse shadow comparison failed: key=%s", key)
        finally:
            with _INFLIGHT_LOCK:
                _INFLIGHT_KEYS.discard(key)

    try:
        _EXECUTOR.submit(run)
    except Exception:
        with _INFLIGHT_LOCK:
            _INFLIGHT_KEYS.discard(key)
        logger.exception("ClickHouse shadow task submission failed: key=%s", key)


def _number_equal(left: Any, right: Any, tolerance: float = 1e-8) -> bool:
    if left is None or right is None:
        return left is right
    try:
        return abs(float(left) - float(right)) <= tolerance
    except (TypeError, ValueError):
        return left == right


def _compare_dicts(
    mysql_rows: Dict[str, Dict[str, Any]],
    clickhouse_rows: Dict[str, Dict[str, Any]],
    fields: Sequence[str],
) -> List[str]:
    differences = []
    mysql_keys = set(mysql_rows)
    clickhouse_keys = set(clickhouse_rows)
    for symbol in sorted(mysql_keys - clickhouse_keys):
        differences.append(f"{symbol}: missing_in_clickhouse")
    for symbol in sorted(clickhouse_keys - mysql_keys):
        differences.append(f"{symbol}: missing_in_mysql")
    for symbol in sorted(mysql_keys & clickhouse_keys):
        for field in fields:
            if not _number_equal(mysql_rows[symbol].get(field), clickhouse_rows[symbol].get(field)):
                differences.append(
                    f"{symbol}.{field}: mysql={mysql_rows[symbol].get(field)!r} "
                    f"clickhouse={clickhouse_rows[symbol].get(field)!r}"
                )
    return differences


def shadow_latest_funding_rates(
    mysql_result: Dict[str, Dict[str, Any]],
    symbols: Optional[Sequence[str]],
    period: str = "5m",
    exchange: str = "binance",
    as_of_ms: Optional[int] = None,
) -> None:
    """Schedule a non-blocking latest-funding-rate comparison."""
    if not _enabled():
        return
    copied_result = {str(symbol): dict(value or {}) for symbol, value in (mysql_result or {}).items()}
    key = ("funding", tuple(sorted(copied_result)), period, exchange, as_of_ms)

    def compare() -> None:
        clickhouse_result = _get_repository().latest_funding_rates(
            symbols=symbols,
            period=period,
            exchange=exchange,
            as_of_ms=as_of_ms,
        )
        differences = _compare_dicts(
            copied_result,
            clickhouse_result,
            ("event_time", "funding_rate", "predicted_rate", "next_funding_time", "mark_price"),
        )
        if differences:
            logger.warning(
                "ClickHouse shadow funding mismatch: symbols=%d differences=%s",
                len(set(copied_result) | set(clickhouse_result)),
                differences[:5],
            )
        else:
            logger.debug("ClickHouse shadow funding match: symbols=%d", len(copied_result))

    _submit(key, compare)


def _ticker_dict(row: Any) -> Dict[str, Any]:
    fields = (
        "symbol", "price_change", "price_change_percent", "weighted_avg_price", "last_price",
        "last_qty", "open_price", "high_price", "low_price", "volume", "quote_volume",
        "open_time", "close_time", "first_id", "last_id", "count",
    )
    if isinstance(row, dict):
        return {field: row.get(field) for field in fields}
    return {field: getattr(row, field, None) for field in fields}


def shadow_latest_tickers(
    mysql_rows: Sequence[Any],
    rank_type: str = "price_change",
    direction: str = "down",
    limit: int = 100,
    close_time: Optional[int] = None,
) -> None:
    """Schedule a non-blocking latest-ticker comparison."""
    if not _enabled():
        return
    copied_rows = [_ticker_dict(row) for row in (mysql_rows or [])]
    if close_time is None and copied_rows:
        close_time = copied_rows[0].get("close_time")
    key = ("tickers", close_time, rank_type, direction, int(limit))

    def compare() -> None:
        clickhouse_rows = _get_repository().latest_tickers(
            rank_type=rank_type,
            direction=direction,
            limit=limit,
            close_time=close_time,
        )
        mysql_map = {row.get("symbol"): row for row in copied_rows}
        clickhouse_map = {row.get("symbol"): row for row in clickhouse_rows}
        differences = _compare_dicts(
            mysql_map,
            clickhouse_map,
            (
                "price_change", "price_change_percent", "weighted_avg_price", "last_price",
                "last_qty", "open_price", "high_price", "low_price", "volume", "quote_volume",
                "open_time", "close_time", "first_id", "last_id", "count",
            ),
        )
        if differences:
            logger.warning(
                "ClickHouse shadow ticker mismatch: close_time=%s differences=%s",
                close_time,
                differences[:5],
            )
        else:
            logger.debug("ClickHouse shadow ticker match: close_time=%s rows=%d", close_time, len(copied_rows))

    _submit(key, compare)
