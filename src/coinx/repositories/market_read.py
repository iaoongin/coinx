"""Read-only market repositories shared by the MySQL and ClickHouse paths.

The MySQL implementation preserves the existing query contract while the
ClickHouse implementation provides the production read path selected by the
process-level ``READ_BACKEND`` switch. Neither implementation exposes writes.
"""

from __future__ import annotations

import re
import time
from decimal import Decimal
from threading import Lock, RLock
from typing import Any, Dict, List, Optional, Sequence, Tuple

from coinx.read_clients import ClickHouseReadClient, MySQLReadClient


TABLES = {
    "market_klines": ("open_time", "symbol"),
    "market_open_interest_hist": ("event_time", "symbol"),
    "market_taker_buy_sell_vol": ("event_time", "symbol"),
    "market_funding_rate": ("event_time", "symbol"),
    "market_snapshots": ("snapshot_time", "symbol"),
    "market_tickers": ("close_time", "symbol"),
}
LATEST_FUNDING_LOOKBACK_MS = 6 * 60 * 60 * 1000
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CACHE_LOCK = RLock()
_CACHE_KEY_LOCKS: Dict[Tuple[str, Any], Lock] = {}
_LATEST_FUNDING_CACHE: Dict[Any, Any] = {}
_FUNDING_HISTORY_CACHE: Dict[Any, Any] = {}
_LATEST_TICKER_CACHE: Dict[Any, Any] = {}
_CACHE_MAX_ENTRIES = 128
# Keep large ClickHouse aggregations spillable. This is a per-query threshold,
# not a request to raise the process/container memory limit.
_EXTERNAL_GROUP_BY_BYTES = 256 * 1024 * 1024


def _cache_key_lock(cache_name: str, key: Any) -> Lock:
    """Return a stable per-query lock so concurrent requests share one fill."""
    composite = (cache_name, key)
    with _CACHE_LOCK:
        lock = _CACHE_KEY_LOCKS.get(composite)
        if lock is None:
            lock = Lock()
            _CACHE_KEY_LOCKS[composite] = lock
        return lock


def clear_market_read_cache() -> None:
    """Clear process-level ClickHouse read caches (used by tests and diagnostics)."""
    with _CACHE_LOCK:
        _LATEST_FUNDING_CACHE.clear()
        _FUNDING_HISTORY_CACHE.clear()
        _LATEST_TICKER_CACHE.clear()
        _CACHE_KEY_LOCKS.clear()


def _identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value or ""):
        raise ValueError(f"invalid SQL identifier: {value!r}")
    return value


def _quote(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _in_list(values: Optional[Sequence[str]]) -> Optional[str]:
    if values is None:
        return None
    values = [str(value) for value in values if value]
    return ", ".join(_quote(value) for value in values) if values else "''"


def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    normalized = {}
    for key, value in row.items():
        if isinstance(value, Decimal):
            value = float(value)
        elif key.endswith("_time") or key in {"first_id", "last_id", "count", "trade_count"}:
            if value is not None:
                value = int(value)
        normalized[key] = value
    return normalized


class ClickHouseMarketReadRepository:
    def __init__(self, client: ClickHouseReadClient, database: str) -> None:
        self.client = client
        self.database = _identifier(database)
        endpoint = getattr(client, "url", None)
        if endpoint:
            auth = getattr(client, "auth", ()) or ()
            user = auth[0] if auth else None
            self._cache_namespace = ("clickhouse", str(endpoint).rstrip("/"), self.database, user)
        else:
            # Test doubles without an endpoint must not leak cache entries between instances.
            self._cache_namespace = ("client", id(client), self.database)

    @staticmethod
    def _cached_rows(cache: Dict[Any, Any], key: Any, ttl_seconds: float) -> Optional[List[Dict[str, Any]]]:
        with _CACHE_LOCK:
            cached = cache.get(key)
            if cached is None:
                return None
            stored_at, rows = cached
            if time.monotonic() - stored_at >= ttl_seconds:
                cache.pop(key, None)
                return None
            return [dict(row) for row in rows]

    @staticmethod
    def _store_rows(cache: Dict[Any, Any], key: Any, rows: List[Dict[str, Any]]) -> None:
        with _CACHE_LOCK:
            cache[key] = (time.monotonic(), [dict(row) for row in rows])
            if len(cache) > _CACHE_MAX_ENTRIES:
                oldest_key = min(cache, key=lambda item: cache[item][0])
                cache.pop(oldest_key, None)

    def _table(self, table: str, final: bool = True) -> str:
        # ReplacingMergeTree rows are eventually merged; FINAL keeps API and
        # collector reads logically deduplicated immediately after a retry.
        suffix = " FINAL" if final else ""
        return f"{self.database}.{_identifier(table)}{suffix}"

    def market_rows(
        self,
        table: str,
        columns: str,
        symbols: Optional[Sequence[str]] = None,
        exchange: Optional[str] = None,
        period: Optional[str] = None,
        time_column: Optional[str] = None,
        lower_bound: Optional[int] = None,
        upper_bound: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Read a fixed market-table projection for business repositories."""
        table = _identifier(table)
        filters: List[str] = []
        if symbols:
            symbol_filter = _in_list(symbols)
            filters.append(f"symbol IN ({symbol_filter})")
        if exchange is not None:
            filters.append(f"exchange = {_quote(exchange)}")
        if period is not None:
            filters.append(f"period = {_quote(period)}")
        if time_column is not None:
            time_column = _identifier(time_column)
            if lower_bound is not None:
                filters.append(f"{time_column} >= {int(lower_bound)}")
            if upper_bound is not None:
                filters.append(f"{time_column} <= {int(upper_bound)}")
        sql = f"SELECT {columns} FROM {self._table(table)}"
        if filters:
            sql += " WHERE " + " AND ".join(filters)
        sql += f" ORDER BY {order_by or time_column or 'symbol'}"
        return [_normalize_row(row) for row in self.client.query_rows(sql)]

    def aggregate_kline_rows(
        self,
        symbols: Sequence[str],
        exchange: str,
        interval_ms: int,
        lower_bound: Optional[int] = None,
        upper_bound: Optional[int] = None,
        period: str = "5m",
    ) -> List[Dict[str, Any]]:
        """Aggregate complete K-line buckets inside ClickHouse."""
        values = [str(value) for value in symbols if value]
        if not values:
            return []
        interval_ms = int(interval_ms)
        base_period_ms = 5 * 60 * 1000
        if interval_ms <= 0 or interval_ms % base_period_ms:
            raise ValueError("interval_ms must be a positive multiple of 5 minutes")
        expected_points = interval_ms // base_period_ms
        filters = [
            f"symbol IN ({_in_list(values)})",
            f"exchange = {_quote(exchange)}",
            f"period = {_quote(period)}",
        ]
        if lower_bound is not None:
            filters.append(f"open_time >= {int(lower_bound)}")
        if upper_bound is not None:
            filters.append(f"open_time <= {int(upper_bound)}")
        # FINAL is expensive for a wide time range because ClickHouse has to
        # merge all matching parts before the outer GROUP BY. The versioned
        # business key is stable, so collapse it with argMax first and then
        # aggregate the deduplicated rows. The outer max/min are intentional:
        # they describe the high/low over the whole bucket, not the newest row.
        deduplicated = (
            "SELECT symbol, open_time, "
            "argMax(high_price, updated_at) AS high_price, "
            "argMax(low_price, updated_at) AS low_price, "
            "argMax(close_price, updated_at) AS close_price, "
            "argMax(quote_volume, updated_at) AS quote_volume "
            f"FROM {self._table('market_klines', final=False)} "
            f"PREWHERE {' AND '.join(filters)} "
            "GROUP BY symbol, open_time"
        )
        bucket_expression = f"toUInt64(intDiv(open_time, {interval_ms}) * {interval_ms})"
        rows = self.client.query_rows(
            "SELECT symbol, "
            f"{bucket_expression} AS bucket_time, "
            "max(high_price) AS high_price, "
            "min(low_price) AS low_price, "
            "argMax(close_price, open_time) AS close_price, "
            "sum(ifNull(quote_volume, 0)) AS quote_volume "
            f"FROM ({deduplicated}) AS k "
            "GROUP BY symbol, bucket_time "
            f"HAVING count() = {expected_points} "
            f"AND max(open_time) - min(open_time) = {(expected_points - 1) * base_period_ms} "
            "ORDER BY symbol, bucket_time "
            f"SETTINGS max_bytes_before_external_group_by = {_EXTERNAL_GROUP_BY_BYTES}"
        )
        normalized = []
        for row in rows:
            item = dict(row)
            item['open_time'] = int(item.pop('bucket_time'))
            normalized.append(item)
        return normalized

    def kline_quote_volume_by_symbol(
        self,
        symbols: Sequence[str],
        exchange: str,
        lower_bound: Optional[int] = None,
        upper_bound: Optional[int] = None,
        period: str = "5m",
    ) -> Dict[str, float]:
        """Return quote-volume totals without transferring raw K-lines."""
        values = [str(value) for value in symbols if value]
        if not values:
            return {}
        filters = [
            f"symbol IN ({_in_list(values)})",
            f"exchange = {_quote(exchange)}",
            f"period = {_quote(period)}",
        ]
        if lower_bound is not None:
            filters.append(f"open_time >= {int(lower_bound)}")
        if upper_bound is not None:
            filters.append(f"open_time <= {int(upper_bound)}")
        deduplicated = (
            "SELECT symbol, open_time, "
            "argMax(quote_volume, updated_at) AS quote_volume "
            f"FROM {self._table('market_klines', final=False)} "
            f"PREWHERE {' AND '.join(filters)} "
            "GROUP BY symbol, open_time"
        )
        rows = self.client.query_rows(
            "SELECT symbol, sum(ifNull(quote_volume, 0)) AS quote_volume_24h "
            f"FROM ({deduplicated}) AS k "
            "GROUP BY symbol "
            f"SETTINGS max_bytes_before_external_group_by = {_EXTERNAL_GROUP_BY_BYTES}"
        )
        return {
            str(row['symbol']): float(row['quote_volume_24h'] or 0)
            for row in rows
            if row.get('symbol')
        }

    def latest_series_times(
        self,
        table: str,
        symbols: Sequence[str],
        exchange: Optional[str],
        period: str,
        time_column: str,
        upper_bound: Optional[int] = None,
    ) -> Dict[str, int]:
        table = _identifier(table)
        time_column = _identifier(time_column)
        symbol_filter = _in_list(symbols)
        filters = [f"symbol IN ({symbol_filter})", f"period = {_quote(period)}"]
        if exchange is not None:
            filters.append(f"exchange = {_quote(exchange)}")
        if upper_bound is not None:
            filters.append(f"{time_column} <= {int(upper_bound)}")
        rows = self.client.query_rows(
            f"SELECT symbol, max({time_column}) AS latest_time FROM {self._table(table)} "
            f"WHERE {' AND '.join(filters)} GROUP BY symbol"
        )
        return {
            str(row['symbol']): int(row['latest_time'])
            for row in rows
            if row.get('symbol') and row.get('latest_time') is not None
        }

    def table_count(self, table: str) -> int:
        value = self.client.query_scalar(f"SELECT count() FROM {self._table(table)}")
        return int(value or 0)

    def time_bounds(self, table: str) -> Dict[str, Optional[int]]:
        time_column = TABLES[table][0]
        row = self.client.query_rows(
            f"SELECT minOrNull({time_column}) AS min_time, maxOrNull({time_column}) AS max_time "
            f"FROM {self._table(table)}"
        )
        if not row:
            return {"min_time": None, "max_time": None}
        return {
            "min_time": int(row[0]["min_time"]) if row[0]["min_time"] is not None else None,
            "max_time": int(row[0]["max_time"]) if row[0]["max_time"] is not None else None,
        }

    def latest_funding_rates(
        self,
        symbols: Optional[Sequence[str]] = None,
        period: str = "5m",
        exchange: str = "binance",
        as_of_ms: Optional[int] = None,
    ) -> Dict[str, Dict[str, Any]]:
        requested = None if symbols is None else [str(value) for value in symbols if value]
        if requested == []:
            return {}
        cache_key = (
            self._cache_namespace,
            period,
            exchange,
            None if requested is None else tuple(sorted(set(requested))),
            None if as_of_ms is None else int(as_of_ms),
        )
        with _cache_key_lock("funding", cache_key):
            cached_rows = self._cached_rows(
                _LATEST_FUNDING_CACHE,
                cache_key,
                10.0 if as_of_ms is None else 300.0,
            )
            if cached_rows is not None:
                return {row["symbol"]: row for row in cached_rows}
            result = self._load_latest_funding_rates_uncached(
                requested=requested,
                period=period,
                exchange=exchange,
                as_of_ms=as_of_ms,
            )
            self._store_rows(_LATEST_FUNDING_CACHE, cache_key, list(result.values()))
            return result

    def _load_latest_funding_rates_uncached(
        self,
        requested: Optional[Sequence[str]],
        period: str,
        exchange: str,
        as_of_ms: Optional[int],
    ) -> Dict[str, Dict[str, Any]]:
        # The table is ordered by symbol first. A full-history LIMIT 1 BY query
        # therefore sorts millions of rows when the caller asks for all symbols.
        # Read a recent bounded window first, then perform an exact historical
        # fallback only for symbols that have no row in that window.
        upper = int(as_of_ms) if as_of_ms is not None else self.client.query_scalar(
            f"SELECT max(event_time) FROM {self._table('market_funding_rate')} "
            f"WHERE period = {_quote(period)} AND exchange = {_quote(exchange)}"
        )
        if upper is None:
            return {}
        upper = int(upper)
        lower = upper - LATEST_FUNDING_LOOKBACK_MS
        rows = self._latest_funding_rows(
            period=period,
            exchange=exchange,
            symbols=requested,
            lower_bound=lower,
            upper_bound=upper,
        )
        result = {row["symbol"]: _normalize_row(row) for row in rows}

        if requested is None:
            expected_rows = self.client.query_rows(
                f"SELECT DISTINCT symbol FROM {self._table('market_funding_rate')} "
                f"WHERE period = {_quote(period)} AND exchange = {_quote(exchange)} "
                f"AND event_time <= {upper}"
            )
            expected = {str(row["symbol"]) for row in expected_rows if row.get("symbol")}
        else:
            expected = set(requested)

        missing = sorted(expected - set(result))
        if missing:
            for row in self._latest_funding_rows(
                period=period,
                exchange=exchange,
                symbols=missing,
                upper_bound=upper,
            ):
                result[row["symbol"]] = _normalize_row(row)
        return result

    def _latest_funding_rows(
        self,
        period: str,
        exchange: str,
        symbols: Optional[Sequence[str]] = None,
        lower_bound: Optional[int] = None,
        upper_bound: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        filters = [f"period = {_quote(period)}", f"exchange = {_quote(exchange)}"]
        symbol_filter = _in_list(symbols)
        if symbol_filter is not None:
            filters.append(f"symbol IN ({symbol_filter})")
        if lower_bound is not None:
            filters.append(f"event_time >= {int(lower_bound)}")
        if upper_bound is not None:
            filters.append(f"event_time <= {int(upper_bound)}")
        return self.client.query_rows(
            "SELECT symbol, event_time, funding_rate, predicted_rate, next_funding_time, mark_price "
            f"FROM {self._table('market_funding_rate')} "
            f"WHERE {' AND '.join(filters)} "
            "ORDER BY symbol, event_time DESC, updated_at DESC "
            "LIMIT 1 BY symbol"
        )

    def funding_rate_history(
        self,
        symbol: str,
        hours: int = 1,
        period: str = "5m",
        exchange: str = "binance",
        now_ms: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        cutoff = int(now_ms if now_ms is not None else time.time() * 1000) - int(hours) * 3600000
        cache_key = (
            self._cache_namespace,
            str(symbol),
            int(hours),
            period,
            exchange,
            None if now_ms is None else int(now_ms),
        )
        with _cache_key_lock("funding-history", cache_key):
            cached_rows = self._cached_rows(
                _FUNDING_HISTORY_CACHE,
                cache_key,
                5.0 if now_ms is None else 300.0,
            )
            if cached_rows is not None:
                return cached_rows
            normalized = self._load_funding_rate_history_uncached(
                symbol=symbol,
                period=period,
                exchange=exchange,
                cutoff=cutoff,
                now_ms=now_ms,
            )
            self._store_rows(_FUNDING_HISTORY_CACHE, cache_key, normalized)
            return normalized

    def _load_funding_rate_history_uncached(
        self,
        symbol: str,
        period: str,
        exchange: str,
        cutoff: int,
        now_ms: Optional[int],
    ) -> List[Dict[str, Any]]:
        rows = self.client.query_rows(
            "SELECT symbol, event_time, funding_rate, predicted_rate, next_funding_time, mark_price "
            f"FROM {self._table('market_funding_rate')} "
            f"WHERE symbol = {_quote(symbol)} AND period = {_quote(period)} "
            f"AND exchange = {_quote(exchange)} AND event_time >= {cutoff} "
            + (f"AND event_time <= {int(now_ms)} " if now_ms is not None else "")
            + "ORDER BY event_time ASC"
        )
        return [_normalize_row(row) for row in rows]

    def latest_funding_rate_page(
        self, keyword: str = "", show_abnormal_only: bool = False,
        sort_by: str = "funding_rate", sort_order: str = "desc", page: int = 1,
        page_size: int = 50, threshold: float = 0.001, period: str = "5m",
        as_of_ms: Optional[int] = None, exchange: str = "binance",
    ) -> Dict[str, Any]:
        rows = list(self.latest_funding_rates(
            period=period,
            exchange=exchange,
            as_of_ms=as_of_ms,
        ).values())
        keyword_upper = (keyword or "").upper()
        if keyword_upper:
            rows = [row for row in rows if keyword_upper in str(row.get("symbol", "")).upper()]
        if show_abnormal_only:
            rows = [
                row for row in rows
                if abs(float(row.get("predicted_rate") if row.get("predicted_rate") is not None else row.get("funding_rate") or 0)) >= threshold
            ]
        abnormal_count = sum(
            abs(float(row.get("predicted_rate") if row.get("predicted_rate") is not None else row.get("funding_rate") or 0)) >= threshold
            for row in rows
        )
        positive_count = sum((row.get("funding_rate") or 0) > 0 for row in rows)
        negative_count = sum((row.get("funding_rate") or 0) < 0 for row in rows)
        sort_field = {
            "predicted_rate": "predicted_rate", "abs_predicted_rate": "predicted_rate",
            "funding_rate": "funding_rate", "abs_funding_rate": "funding_rate",
        }.get(sort_by, "predicted_rate")
        reverse = str(sort_order).lower() != "asc"
        rows.sort(key=lambda row: abs(float(row.get(sort_field) or 0)) if sort_by.startswith("abs_") else (float(row.get(sort_field)) if row.get(sort_field) is not None else 0), reverse=reverse)
        total_count = len(rows)
        start = max(int(page) - 1, 0) * int(page_size)
        return {
            "data": rows[start:start + int(page_size)], "total_count": total_count,
            "stats": {"total": total_count, "abnormal": int(abnormal_count), "positive": int(positive_count), "negative": int(negative_count)},
        }

    def abnormal_funding_rates(
        self, threshold: float = 0.001, exchange: str = "binance", as_of_ms: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        latest = self.latest_funding_rates(period="5m", exchange=exchange, as_of_ms=as_of_ms)
        rows = []
        for row in latest.values():
            value = row.get("predicted_rate") if row.get("predicted_rate") is not None else row.get("funding_rate")
            if value is not None and abs(float(value)) >= threshold:
                rows.append(dict(row))
        rows.sort(key=lambda row: abs(float(row.get("predicted_rate") or row.get("funding_rate") or 0)), reverse=True)
        return rows

    def funding_rate_sparklines(
        self, symbols: Sequence[str], hours: int = 1, exchange: str = "binance", as_of_ms: Optional[int] = None,
    ) -> Dict[str, List[Any]]:
        if not symbols:
            return {}
        cutoff = int(as_of_ms if as_of_ms is not None else time.time() * 1000) - int(hours) * 3600000
        symbol_filter = _in_list(symbols)
        rows = self.client.query_rows(
            "SELECT symbol, event_time, funding_rate "
            f"FROM {self._table('market_funding_rate')} WHERE symbol IN ({symbol_filter}) "
            f"AND period = {_quote('5m')} AND exchange = {_quote(exchange)} AND event_time >= {cutoff} "
            + (f"AND event_time <= {int(as_of_ms)} " if as_of_ms is not None else "")
            + "ORDER BY symbol ASC, event_time ASC"
        )
        result: Dict[str, List[Any]] = {}
        for row in rows:
            result.setdefault(row["symbol"], []).append(_normalize_row(row).get("funding_rate"))
        return result

    def latest_tickers(
        self,
        rank_type: str = "price_change",
        direction: str = "down",
        limit: int = 100,
        close_time: Optional[int] = None,
        as_of_ms: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        cache_key = (
            self._cache_namespace,
            rank_type,
            direction,
            max(1, int(limit)),
            None if close_time is None else int(close_time),
            None if as_of_ms is None else int(as_of_ms),
        )
        with _cache_key_lock("ticker", cache_key):
            cached_rows = self._cached_rows(_LATEST_TICKER_CACHE, cache_key, 10.0)
            if cached_rows is not None:
                return cached_rows
            normalized = self._load_latest_tickers_uncached(
                rank_type=rank_type,
                direction=direction,
                limit=limit,
                close_time=close_time,
                as_of_ms=as_of_ms,
            )
            self._store_rows(_LATEST_TICKER_CACHE, cache_key, normalized)
            return normalized

    def _load_latest_tickers_uncached(
        self,
        rank_type: str,
        direction: str,
        limit: int,
        close_time: Optional[int],
        as_of_ms: Optional[int],
    ) -> List[Dict[str, Any]]:
        if close_time is None:
            upper = f" WHERE close_time <= {int(as_of_ms)}" if as_of_ms is not None else ""
            close_time = self.client.query_scalar(
                f"SELECT max(close_time) FROM {self._table('market_tickers', final=False)}{upper}"
            )
        if close_time is None:
            return []
        order_map = {
            "price_change": "price_change_percent",
            "volume": "volume",
            "quote_volume": "quote_volume",
        }
        order_column = order_map.get(rank_type, "price_change_percent")
        order_direction = "ASC" if rank_type == "price_change" and direction == "down" else "DESC"
        rows = self.client.query_rows(
            "SELECT symbol, "
            "argMax(price_change, updated_at) AS price_change, "
            "argMax(price_change_percent, updated_at) AS price_change_percent, "
            "argMax(weighted_avg_price, updated_at) AS weighted_avg_price, "
            "argMax(last_price, updated_at) AS last_price, "
            "argMax(last_qty, updated_at) AS last_qty, "
            "argMax(open_price, updated_at) AS open_price, "
            "argMax(high_price, updated_at) AS high_price, "
            "argMax(low_price, updated_at) AS low_price, "
            "argMax(volume, updated_at) AS volume, "
            "argMax(quote_volume, updated_at) AS quote_volume, "
            "argMax(open_time, updated_at) AS open_time, "
            "argMax(close_time, updated_at) AS close_time, "
            "argMax(first_id, updated_at) AS first_id, "
            "argMax(last_id, updated_at) AS last_id, "
            "argMax(count, updated_at) AS count "
            f"FROM {self._table('market_tickers', final=False)} AS mt "
            f"WHERE mt.close_time = {int(close_time)} "
            "GROUP BY mt.symbol "
            f"ORDER BY {order_column} {order_direction}, symbol ASC LIMIT {max(1, int(limit))}"
            f" SETTINGS max_bytes_before_external_group_by = {_EXTERNAL_GROUP_BY_BYTES}"
        )
        return [_normalize_row(row) for row in rows]

    def price_volume_metrics(
        self,
        scope_limit: int = 100,
        lookback_ms: int = 26 * 60 * 60 * 1000,
        as_of_ms: Optional[int] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """Calculate alert metrics from the ClickHouse market tables."""
        scope_limit = max(1, int(scope_limit))
        upper_filter = f" AND close_time <= {int(as_of_ms)}" if as_of_ms is not None else ""
        rows = self.client.query_rows(
            f"""
            WITH latest_snapshot AS (
                SELECT max(close_time) AS snapshot_time
                FROM {self._table('market_tickers', final=False)}
                WHERE 1 = 1 {upper_filter}
            ), top_symbols AS (
                SELECT mt.symbol, latest_snapshot.snapshot_time,
                    argMax(mt.quote_volume, mt.updated_at) AS quote_volume
                FROM {self._table('market_tickers', final=False)} AS mt
                CROSS JOIN latest_snapshot
                WHERE mt.close_time = latest_snapshot.snapshot_time
                GROUP BY mt.symbol, latest_snapshot.snapshot_time
                ORDER BY quote_volume DESC
                LIMIT {scope_limit}
            ), deduplicated_klines AS (
                SELECT
                    k.symbol,
                    k.open_time,
                    argMax(k.open_price, k.updated_at) AS open_price,
                    argMax(k.close_price, k.updated_at) AS close_price,
                    argMax(k.quote_volume, k.updated_at) AS quote_volume
                FROM {self._table('market_klines', final=False)} AS k
                PREWHERE k.exchange = 'binance' AND k.period = '5m'
                WHERE k.symbol IN (SELECT symbol FROM top_symbols)
                  AND k.open_time >= (SELECT min(snapshot_time) FROM top_symbols) - {int(lookback_ms)}
                  AND k.open_time <= (SELECT max(snapshot_time) FROM top_symbols)
                GROUP BY k.symbol, k.open_time
            ), ranked_klines AS (
                SELECT
                    k.symbol, k.open_time, k.open_price, k.close_price, k.quote_volume,
                    row_number() OVER (PARTITION BY k.symbol ORDER BY k.open_time DESC) AS rn
                FROM deduplicated_klines AS k
                INNER JOIN top_symbols s ON s.symbol = k.symbol
                WHERE k.open_time >= s.snapshot_time - {int(lookback_ms)}
                  AND k.open_time <= s.snapshot_time
            ), metrics AS (
                SELECT
                    symbol,
                    maxIf(open_time, rn = 1) AS open_time,
                    maxIf(close_price, rn = 1) AS close_price,
                    (maxIf(close_price, rn = 1) - maxIf(open_price, rn = 1))
                        / nullIf(maxIf(open_price, rn = 1), 0) AS price_change,
                    maxIf(quote_volume, rn = 1)
                        / nullIf(avgIf(quote_volume, rn BETWEEN 2 AND 289), 0) AS volume_ratio,
                    countIf(rn <= 289) AS kline_count,
                    countIf(rn BETWEEN 2 AND 289 AND quote_volume IS NOT NULL)
                        AS historical_volume_count
                FROM ranked_klines
                WHERE rn <= 289
                GROUP BY symbol
            )
            SELECT
                s.symbol,
                m.open_time,
                m.close_price,
                m.price_change,
                m.volume_ratio,
                ifNull(m.kline_count, 0) AS kline_count,
                ifNull(m.historical_volume_count, 0) AS historical_volume_count
            FROM top_symbols AS s
            LEFT JOIN metrics AS m ON m.symbol = s.symbol
            ORDER BY s.symbol
            SETTINGS max_bytes_before_external_group_by = {_EXTERNAL_GROUP_BY_BYTES},
                     max_bytes_before_external_sort = {_EXTERNAL_GROUP_BY_BYTES}
            """
        )
        return {
            row['symbol']: {
                'open_time': row.get('open_time'),
                'close_price': float(row['close_price']) if row.get('close_price') is not None else None,
                'price_change': float(row['price_change']) if row.get('price_change') is not None else None,
                'volume_ratio': float(row['volume_ratio']) if row.get('volume_ratio') is not None else None,
                'kline_count': int(row.get('kline_count') or 0),
                'historical_volume_count': int(row.get('historical_volume_count') or 0),
            }
            for row in rows
            if row.get('symbol')
        }

    def series_window(
        self,
        table: str,
        symbol: str,
        hours: int = 24,
        period: str = "5m",
        exchange: Optional[str] = None,
        now_ms: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        table = _identifier(table)
        if table not in TABLES or table == "market_tickers":
            raise ValueError(f"unsupported time-series table: {table}")
        time_column = TABLES[table][0]
        filters = [f"symbol = {_quote(symbol)}", f"period = {_quote(period)}"]
        if exchange is not None:
            filters.append(f"exchange = {_quote(exchange)}")
        latest_filters = " AND ".join(filters)
        latest = self.client.query_scalar(
            f"SELECT max({time_column}) FROM {self._table(table)} WHERE {latest_filters}"
        )
        if latest is None:
            return []
        upper = min(int(latest), int(now_ms)) if now_ms is not None else int(latest)
        cutoff = upper - int(hours) * 3600000
        columns = {
            "market_klines": "exchange, symbol, period, open_time, close_time, open_price, high_price, low_price, close_price, volume, quote_volume, trade_count, taker_buy_base_volume, taker_buy_quote_volume",
            "market_open_interest_hist": "exchange, symbol, period, event_time, sum_open_interest, sum_open_interest_value",
            "market_taker_buy_sell_vol": "exchange, symbol, period, event_time, buy_sell_ratio, buy_vol, sell_vol",
            "market_funding_rate": "exchange, symbol, period, event_time, funding_rate, predicted_rate, next_funding_time, mark_price",
        }[table]
        rows = self.client.query_rows(
            f"SELECT {columns} FROM {self._table(table)} WHERE {latest_filters} "
            f"AND {time_column} BETWEEN {cutoff} AND {upper} ORDER BY {time_column} ASC"
        )
        return [_normalize_row(row) for row in rows]

    def contract_chart_series(
        self,
        symbol: str,
        hours: int = 24,
        period: str = "5m",
        as_of_ms: Optional[int] = None,
        max_points: int = 300,
    ) -> Dict[str, Any]:
        """Build the contract chart payload from ClickHouse market tables."""
        symbol_sql = _quote(symbol)
        period_sql = _quote(period)
        upper_sql = f" AND {{time_column}} <= {int(as_of_ms)}" if as_of_ms is not None else ""
        latest_times = []
        for table, time_column in (
            ("market_klines", "open_time"),
            ("market_open_interest_hist", "event_time"),
            ("market_taker_buy_sell_vol", "event_time"),
        ):
            latest = self.client.query_scalar(
                f"SELECT max({time_column}) FROM {self._table(table)} "
                f"WHERE symbol = {symbol_sql} AND period = {period_sql}"
                + upper_sql.format(time_column=time_column)
            )
            if latest is not None:
                latest_times.append(int(latest))
        if not latest_times:
            return {"range": f"{int(hours)}h", "anchor_time": None, "market": [], "flow": [], "funding_rate": []}

        anchor = max(latest_times)
        cutoff = anchor - int(hours) * 3600000
        kline_rows = self.client.query_rows(
            f"SELECT exchange, symbol, period, open_time, close_time, close_price, volume "
            f"FROM {self._table('market_klines')} WHERE symbol = {symbol_sql} AND period = {period_sql} "
            f"AND open_time BETWEEN {cutoff} AND {anchor} ORDER BY open_time ASC"
        )
        oi_rows = self.client.query_rows(
            f"SELECT exchange, symbol, period, event_time, sum_open_interest, sum_open_interest_value "
            f"FROM {self._table('market_open_interest_hist')} WHERE symbol = {symbol_sql} AND period = {period_sql} "
            f"AND event_time BETWEEN {cutoff} AND {anchor} ORDER BY event_time ASC"
        )
        flow_rows = self.client.query_rows(
            f"SELECT exchange, symbol, period, event_time, buy_vol, sell_vol "
            f"FROM {self._table('market_taker_buy_sell_vol')} WHERE symbol = {symbol_sql} AND period = {period_sql} "
            f"AND event_time BETWEEN {cutoff} AND {anchor} ORDER BY event_time ASC"
        )
        funding_rows = self.client.query_rows(
            f"SELECT symbol, event_time, funding_rate, predicted_rate "
            f"FROM {self._table('market_funding_rate')} WHERE symbol = {symbol_sql} AND period = {period_sql} "
            f"AND event_time BETWEEN {cutoff} AND {anchor} ORDER BY event_time ASC"
        )

        prices: Dict[int, Dict[str, Any]] = {}
        volumes: Dict[int, float] = {}
        for row in kline_rows:
            timestamp = int(row["open_time"])
            if timestamp not in prices or row.get("exchange") == "binance":
                prices[timestamp] = {
                    "value": float(row["close_price"]) if row.get("close_price") is not None else None,
                    "exchange": row.get("exchange"),
                }
            if row.get("volume") is not None:
                volumes[timestamp] = volumes.get(timestamp, 0.0) + float(row["volume"])

        oi: Dict[int, list] = {}
        for row in oi_rows:
            timestamp = int(row["event_time"])
            item = oi.setdefault(timestamp, [0.0, False, 0.0, False])
            if row.get("sum_open_interest_value") is not None:
                item[0] += float(row["sum_open_interest_value"])
                item[1] = True
            if row.get("sum_open_interest") is not None:
                item[2] += float(row["sum_open_interest"])
                item[3] = True

        flow: Dict[int, list] = {}
        for row in flow_rows:
            timestamp = int(row["event_time"])
            item = flow.setdefault(timestamp, [0.0, 0.0])
            item[0] += float(row.get("buy_vol") or 0)
            item[1] += float(row.get("sell_vol") or 0)

        market = []
        for timestamp in sorted(set(prices) | set(volumes) | set(oi)):
            oi_item = oi.get(timestamp, [None, False, None, False])
            market.append({
                "time": timestamp,
                "price": (prices.get(timestamp) or {}).get("value"),
                "volume": volumes.get(timestamp),
                "open_interest_value": oi_item[0] if oi_item[1] else None,
                "open_interest": oi_item[2] if oi_item[3] else None,
            })
        flow_data = [
            {"time": timestamp, "buy_volume": values[0], "sell_volume": values[1], "net_inflow": values[0] - values[1]}
            for timestamp, values in sorted(flow.items())
        ]
        funding = [
            {
                "time": int(row["event_time"]),
                "funding_rate": float(row["funding_rate"]) if row.get("funding_rate") is not None else None,
                "predicted_rate": float(row["predicted_rate"]) if row.get("predicted_rate") is not None else None,
            }
            for row in funding_rows
        ]
        step = max(1, (max(len(market), len(flow_data), len(funding)) + int(max_points) - 1) // int(max_points))
        return {
            "range": f"{int(hours)}h",
            "anchor_time": anchor,
            "market": market[::step],
            "flow": flow_data[::step],
            "funding_rate": funding[::step],
        }


class MySQLMarketReadRepository:
    """Same read contract using the independent MySQL validation client."""

    def __init__(self, client: MySQLReadClient) -> None:
        self.client = client

    def table_count(self, table: str) -> int:
        table = _identifier(table)
        value = self.client.query_scalar(f"SELECT COUNT(*) AS total FROM `{table}`")
        return int(value or 0)

    def time_bounds(self, table: str) -> Dict[str, Optional[int]]:
        table = _identifier(table)
        time_column = TABLES[table][0]
        row = self.client.query_rows(
            f"SELECT MIN(`{time_column}`) AS min_time, MAX(`{time_column}`) AS max_time "
            f"FROM `{table}`"
        )
        if not row:
            return {"min_time": None, "max_time": None}
        return {
            "min_time": int(row[0]["min_time"]) if row[0]["min_time"] is not None else None,
            "max_time": int(row[0]["max_time"]) if row[0]["max_time"] is not None else None,
        }

    def latest_funding_rates(
        self,
        symbols: Optional[Sequence[str]] = None,
        period: str = "5m",
        exchange: str = "binance",
        as_of_ms: Optional[int] = None,
    ) -> Dict[str, Dict[str, Any]]:
        filters = ["m.period = %s", "m.exchange = %s"]
        symbols_args: List[Any] = []
        if symbols is not None:
            symbols = [str(value) for value in symbols if value]
            if not symbols:
                return {}
            symbol_placeholders = ",".join(["%s"] * len(symbols))
            filters.append("m.symbol IN (" + symbol_placeholders + ")")
            symbols_args.extend(symbols)
            latest_symbol_filter = " AND symbol IN (" + symbol_placeholders + ")"
        else:
            latest_symbol_filter = ""
        as_of_filter = " AND event_time <= %s" if as_of_ms is not None else ""
        rows = self.client.query_rows(
            "SELECT m.symbol, m.event_time, m.funding_rate, m.predicted_rate, "
            "m.next_funding_time, m.mark_price "
            "FROM market_funding_rate m "
            "JOIN (SELECT symbol, MAX(event_time) AS event_time "
            "      FROM market_funding_rate WHERE period = %s AND exchange = %s" + latest_symbol_filter + as_of_filter + " "
            "      GROUP BY symbol) latest "
            "ON latest.symbol = m.symbol AND latest.event_time = m.event_time "
            "WHERE " + " AND ".join(filters) + " ORDER BY m.symbol",
            [period, exchange] + symbols_args + ([int(as_of_ms)] if as_of_ms is not None else [])
            + [period, exchange] + symbols_args + ([int(as_of_ms)] if as_of_ms is not None else []),
        )
        return {row["symbol"]: _normalize_row(row) for row in rows}

    def funding_rate_history(
        self,
        symbol: str,
        hours: int = 1,
        period: str = "5m",
        exchange: str = "binance",
        now_ms: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        cutoff = int(now_ms if now_ms is not None else time.time() * 1000) - int(hours) * 3600000
        rows = self.client.query_rows(
            "SELECT symbol, event_time, funding_rate, predicted_rate, next_funding_time, mark_price "
            "FROM market_funding_rate WHERE symbol = %s AND period = %s AND exchange = %s "
            "AND event_time >= %s" + (" AND event_time <= %s" if now_ms is not None else "") + " ORDER BY event_time ASC",
            [symbol, period, exchange, cutoff] + ([int(now_ms)] if now_ms is not None else []),
        )
        return [_normalize_row(row) for row in rows]

    def latest_funding_rate_page(
        self, keyword: str = "", show_abnormal_only: bool = False,
        sort_by: str = "funding_rate", sort_order: str = "desc", page: int = 1,
        page_size: int = 50, threshold: float = 0.001, period: str = "5m",
        as_of_ms: Optional[int] = None, exchange: str = "binance",
    ) -> Dict[str, Any]:
        rows_by_symbol = self.latest_funding_rates(period=period, exchange=exchange, as_of_ms=as_of_ms)
        rows = [dict(row, symbol=symbol) for symbol, row in rows_by_symbol.items()]
        keyword_upper = (keyword or "").upper()
        if keyword_upper:
            rows = [row for row in rows if keyword_upper in str(row.get("symbol", "")).upper()]
        if show_abnormal_only:
            rows = [
                row for row in rows
                if abs(float(row.get("predicted_rate") if row.get("predicted_rate") is not None else row.get("funding_rate") or 0)) >= threshold
            ]
        abnormal_count = sum(
            abs(float(row.get("predicted_rate") if row.get("predicted_rate") is not None else row.get("funding_rate") or 0)) >= threshold
            for row in rows
        )
        positive_count = sum((row.get("funding_rate") or 0) > 0 for row in rows)
        negative_count = sum((row.get("funding_rate") or 0) < 0 for row in rows)
        sort_field = {"predicted_rate": "predicted_rate", "abs_predicted_rate": "predicted_rate", "funding_rate": "funding_rate", "abs_funding_rate": "funding_rate"}.get(sort_by, "predicted_rate")
        reverse = str(sort_order).lower() != "asc"
        rows.sort(key=lambda row: abs(float(row.get(sort_field) or 0)) if sort_by.startswith("abs_") else (float(row.get(sort_field)) if row.get(sort_field) is not None else 0), reverse=reverse)
        total_count = len(rows)
        start = max(int(page) - 1, 0) * int(page_size)
        return {
            "data": rows[start:start + int(page_size)], "total_count": total_count,
            "stats": {"total": total_count, "abnormal": int(abnormal_count), "positive": int(positive_count), "negative": int(negative_count)},
        }

    def abnormal_funding_rates(
        self, threshold: float = 0.001, exchange: str = "binance", as_of_ms: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        latest = self.latest_funding_rates(period="5m", exchange=exchange, as_of_ms=as_of_ms)
        rows = []
        for symbol, row in latest.items():
            value = row.get("predicted_rate") if row.get("predicted_rate") is not None else row.get("funding_rate")
            if value is not None and abs(float(value)) >= threshold:
                rows.append(dict(row, symbol=symbol))
        rows.sort(key=lambda row: abs(float(row.get("predicted_rate") or row.get("funding_rate") or 0)), reverse=True)
        return rows

    def funding_rate_sparklines(
        self, symbols: Sequence[str], hours: int = 1, exchange: str = "binance", as_of_ms: Optional[int] = None,
    ) -> Dict[str, List[Any]]:
        if not symbols:
            return {}
        cutoff = int(as_of_ms if as_of_ms is not None else time.time() * 1000) - int(hours) * 3600000
        placeholders = ",".join(["%s"] * len(symbols))
        rows = self.client.query_rows(
            "SELECT symbol, event_time, funding_rate FROM market_funding_rate "
            f"WHERE symbol IN ({placeholders}) AND period = %s AND exchange = %s AND event_time >= %s"
            + (" AND event_time <= %s" if as_of_ms is not None else "")
            + " ORDER BY symbol ASC, event_time ASC",
            list(symbols) + ["5m", exchange, cutoff] + ([int(as_of_ms)] if as_of_ms is not None else []),
        )
        result: Dict[str, List[Any]] = {}
        for row in rows:
            result.setdefault(row["symbol"], []).append(_normalize_row(row).get("funding_rate"))
        return result

    def latest_tickers(
        self,
        rank_type: str = "price_change",
        direction: str = "down",
        limit: int = 100,
        close_time: Optional[int] = None,
        as_of_ms: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if close_time is None:
            sql = "SELECT MAX(close_time) AS close_time FROM market_tickers"
            args: List[Any] = []
            if as_of_ms is not None:
                sql += " WHERE close_time <= %s"
                args.append(int(as_of_ms))
            close_time = self.client.query_scalar(sql, args)
        if close_time is None:
            return []
        order_map = {
            "price_change": "price_change_percent",
            "volume": "volume",
            "quote_volume": "quote_volume",
        }
        order_column = order_map.get(rank_type, "price_change_percent")
        order_direction = "ASC" if rank_type == "price_change" and direction == "down" else "DESC"
        rows = self.client.query_rows(
            "SELECT symbol, price_change, price_change_percent, weighted_avg_price, last_price, "
            "last_qty, open_price, high_price, low_price, volume, quote_volume, open_time, "
            "close_time, first_id, last_id, count FROM market_tickers "
            f"WHERE close_time = %s ORDER BY {order_column} {order_direction}, symbol ASC LIMIT %s",
            [int(close_time), max(1, int(limit))],
        )
        return [_normalize_row(row) for row in rows]

    def series_window(
        self,
        table: str,
        symbol: str,
        hours: int = 24,
        period: str = "5m",
        exchange: Optional[str] = None,
        now_ms: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        table = _identifier(table)
        if table not in TABLES or table == "market_tickers":
            raise ValueError(f"unsupported time-series table: {table}")
        time_column = TABLES[table][0]
        filters = ["symbol = %s", "period = %s"]
        args: List[Any] = [symbol, period]
        if exchange is not None:
            filters.append("exchange = %s")
            args.append(exchange)
        latest = self.client.query_scalar(
            f"SELECT MAX({time_column}) AS latest_time FROM `{table}` WHERE " + " AND ".join(filters),
            args,
        )
        if latest is None:
            return []
        upper = min(int(latest), int(now_ms)) if now_ms is not None else int(latest)
        cutoff = upper - int(hours) * 3600000
        columns = {
            "market_klines": "exchange, symbol, period, open_time, close_time, open_price, high_price, low_price, close_price, volume, quote_volume, trade_count, taker_buy_base_volume, taker_buy_quote_volume",
            "market_open_interest_hist": "exchange, symbol, period, event_time, sum_open_interest, sum_open_interest_value",
            "market_taker_buy_sell_vol": "exchange, symbol, period, event_time, buy_sell_ratio, buy_vol, sell_vol",
            "market_funding_rate": "exchange, symbol, period, event_time, funding_rate, predicted_rate, next_funding_time, mark_price",
        }[table]
        rows = self.client.query_rows(
            f"SELECT {columns} FROM `{table}` WHERE " + " AND ".join(filters) +
            f" AND {time_column} BETWEEN %s AND %s ORDER BY {time_column} ASC",
            args + [cutoff, upper],
        )
        return [_normalize_row(row) for row in rows]
