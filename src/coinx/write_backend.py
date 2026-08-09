"""ClickHouse market-data write backend.

The application keeps SQLAlchemy/MySQL for transactional control-plane data,
but market-data collectors can use this small HTTP writer when
``MARKET_WRITE_BACKEND=clickhouse``.  The writer is deliberately independent
from the read-only client so a collection path cannot accidentally fall back
to a MySQL session.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

import requests

from coinx import config


logger = logging.getLogger(__name__)
BUSINESS_TIMEZONE = ZoneInfo('Asia/Shanghai')


MARKET_TABLE_COLUMNS: Dict[str, tuple[str, ...]] = {
    "market_klines": (
        "exchange", "symbol", "period", "open_time", "close_time",
        "open_price", "high_price", "low_price", "close_price", "volume",
        "quote_volume", "trade_count", "taker_buy_base_volume",
        "taker_buy_quote_volume", "created_at", "updated_at",
    ),
    "market_open_interest_hist": (
        "exchange", "symbol", "period", "event_time", "sum_open_interest",
        "sum_open_interest_value", "created_at", "updated_at",
    ),
    "market_taker_buy_sell_vol": (
        "exchange", "symbol", "period", "event_time", "buy_sell_ratio",
        "buy_vol", "sell_vol", "created_at", "updated_at",
    ),
    "market_funding_rate": (
        "exchange", "symbol", "period", "event_time", "funding_rate",
        "predicted_rate", "next_funding_time", "mark_price", "created_at",
        "updated_at",
    ),
    "market_snapshots": (
        "snapshot_time", "symbol", "batch_id", "price", "open_interest",
        "open_interest_value", "data_json", "created_at",
    ),
    "market_tickers": (
        "close_time", "symbol", "price_change", "price_change_percent",
        "weighted_avg_price", "last_price", "last_qty", "open_price",
        "high_price", "low_price", "volume", "quote_volume", "open_time",
        "first_id", "last_id", "count", "created_at", "updated_at",
    ),
}

# These are the parts of the ClickHouse DDL that affect correctness.  The
# remaining column details are checked through the explicit column whitelist.
MARKET_TABLE_SCHEMA: Dict[str, Dict[str, tuple[str, ...] | str]] = {
    "market_klines": {
        "engine": "ReplacingMergeTree",
        "sorting_key": ("exchange", "symbol", "period", "open_time"),
    },
    "market_open_interest_hist": {
        "engine": "ReplacingMergeTree",
        "sorting_key": ("exchange", "symbol", "period", "event_time"),
    },
    "market_taker_buy_sell_vol": {
        "engine": "ReplacingMergeTree",
        "sorting_key": ("exchange", "symbol", "period", "event_time"),
    },
    "market_funding_rate": {
        "engine": "ReplacingMergeTree",
        "sorting_key": ("exchange", "symbol", "period", "event_time"),
    },
    "market_snapshots": {
        "engine": "ReplacingMergeTree",
        "sorting_key": ("symbol", "snapshot_time", "batch_id"),
        "time_type": "DateTime64(3",
    },
    "market_tickers": {
        "engine": "ReplacingMergeTree",
        "sorting_key": ("symbol", "close_time"),
        "time_type": "DateTime64(3",
    },
}


class ClickHouseWriteError(RuntimeError):
    """Raised when a synchronous ClickHouse INSERT cannot be confirmed."""


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        # ClickHouse accepts quoted decimal values for Decimal columns and
        # this avoids converting high precision values through binary float.
        return format(value, "f")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=BUSINESS_TIMEZONE)
        else:
            value = value.astimezone(BUSINESS_TIMEZONE)
        # DateTime64(3, 'Asia/Shanghai') is deliberately sent as a local
        # wall-clock value so the column timezone, rather than the server's
        # default timezone, determines its meaning.
        return value.replace(tzinfo=None).isoformat(sep=" ", timespec="milliseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, (list, tuple, dict)):
        return value
    return value


def _json_default(value: Any) -> Any:
    return _json_value(value)


def _identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value or ""):
        raise ValueError(f"invalid ClickHouse identifier: {value!r}")
    return value


class ClickHouseWriteClient:
    """Synchronous HTTP INSERT client with bounded retries."""

    def __init__(
        self,
        url: str,
        database: str,
        user: str,
        password: str,
        timeout: tuple[float, float] = (10.0, 120.0),
        retries: int = 3,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.url = (url or "").rstrip("/")
        if not self.url:
            raise ValueError("ClickHouse write URL cannot be empty")
        self.database = _identifier(database)
        self.auth = (user, password)
        self.timeout = timeout
        self.retries = max(1, int(retries))
        self.session = session or requests.Session()
        self._owns_session = session is None

    def insert_json_each_row(
        self,
        table: str,
        columns: Sequence[str],
        rows: Sequence[Mapping[str, Any]],
        query_id: Optional[str] = None,
    ) -> int:
        table = _identifier(table)
        if table not in MARKET_TABLE_COLUMNS:
            raise ValueError(f"unsupported ClickHouse market table: {table}")
        allowed = set(MARKET_TABLE_COLUMNS[table])
        columns = tuple(columns)
        if (
            not columns
            or len(set(columns)) != len(columns)
            or any(column not in allowed for column in columns)
        ):
            raise ValueError(f"invalid columns for ClickHouse table {table}: {columns!r}")
        if not rows:
            return 0

        payload_lines = []
        for row in rows:
            unknown = set(row) - set(columns)
            if unknown:
                raise ValueError(f"row contains unknown columns for {table}: {sorted(unknown)!r}")
            payload_lines.append(
                json.dumps(
                    {
                        column: (
                            json.dumps(
                                row.get(column), ensure_ascii=False,
                                separators=(",", ":"), default=_json_default,
                            )
                            if column == "data_json" and isinstance(row.get(column), (dict, list))
                            else _json_value(row.get(column))
                        )
                        for column in columns
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=_json_default,
                )
            )
        payload = ("\n".join(payload_lines) + "\n").encode("utf-8")
        query_id = query_id or f"coinx_market_{table}_{uuid.uuid4().hex}"
        query = (
            f"INSERT INTO {self.database}.{table} ({', '.join(columns)}) "
            "FORMAT JSONEachRow"
        )
        params = {
            "query": query,
            "database": self.database,
            "query_id": query_id,
            "async_insert": 0,
            "wait_end_of_query": 1,
        }
        last_error: Optional[BaseException] = None
        started_at = time.perf_counter()
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.post(
                    self.url,
                    params=params,
                    data=payload,
                    auth=self.auth,
                    headers={"Content-Type": "application/octet-stream"},
                    timeout=self.timeout,
                )
                if response.ok:
                    logger.info(
                        "ClickHouse market INSERT succeeded table=%s rows=%d attempt=%d/%d query_id=%s elapsed=%.3fs",
                        table, len(rows), attempt, self.retries, query_id,
                        time.perf_counter() - started_at,
                    )
                    return len(rows)
                detail = response.text.strip() or "ClickHouse returned no error details"
                raise ClickHouseWriteError(
                    f"HTTP {response.status_code} inserting {table}: {detail}"
                )
            except Exception as exc:  # requests and server errors are retriable
                last_error = exc
                if attempt >= self.retries:
                    break
                logger.warning(
                    "ClickHouse market INSERT retry table=%s rows=%d attempt=%d/%d query_id=%s error=%s",
                    table, len(rows), attempt, self.retries, query_id, exc,
                )
                time.sleep(min(2.0, 0.25 * (2 ** (attempt - 1))))
        logger.error(
            "ClickHouse market INSERT failed table=%s rows=%d attempts=%d query_id=%s elapsed=%.3fs error=%s",
            table, len(rows), self.retries, query_id,
            time.perf_counter() - started_at, last_error,
        )
        raise ClickHouseWriteError(
            f"ClickHouse INSERT failed after {self.retries} attempts: table={table}, "
            f"query_id={query_id}"
        ) from last_error

    def query_rows(self, sql: str) -> list[dict[str, Any]]:
        response = self.session.post(
            self.url,
            params={"query": f"{sql.rstrip(';')} FORMAT JSONEachRow", "database": self.database},
            auth=self.auth,
            timeout=self.timeout,
        )
        if not response.ok:
            raise ClickHouseWriteError(response.text.strip() or f"HTTP {response.status_code}")
        return [json.loads(line) for line in response.text.splitlines() if line.strip()]

    def execute(self, sql: str, query_id: Optional[str] = None) -> None:
        response = self.session.post(
            self.url,
            params={
                "query": sql.rstrip(";"),
                "database": self.database,
                "query_id": query_id or f"coinx_market_mutation_{uuid.uuid4().hex}",
                "wait_end_of_query": 1,
            },
            auth=self.auth,
            timeout=self.timeout,
        )
        if not response.ok:
            raise ClickHouseWriteError(response.text.strip() or f"HTTP {response.status_code}")

    def close(self) -> None:
        if self._owns_session:
            self.session.close()


class ClickHouseMarketWriteRepository:
    """Table-aware market writer shared by collectors."""

    def __init__(self, client: ClickHouseWriteClient) -> None:
        self.client = client

    def insert_rows(
        self,
        table: str,
        columns: Sequence[str],
        rows: Iterable[Mapping[str, Any]],
        batch_id: Optional[str] = None,
    ) -> int:
        materialized = [dict(row) for row in rows]
        if not materialized:
            return 0
        batch_id = batch_id or uuid.uuid4().hex
        batch_size = max(1, int(getattr(config, "CLICKHOUSE_WRITE_BATCH_SIZE", 500) or 500))
        affected = 0
        for index in range(0, len(materialized), batch_size):
            chunk = materialized[index:index + batch_size]
            query_id = f"coinx_market_{table}_{batch_id}_{index // batch_size}"
            affected += self.client.insert_json_each_row(
                table, columns, chunk, query_id=query_id,
            )
        return affected


_LOCAL = threading.local()
_LOCKS: Dict[tuple[str, str], threading.Lock] = {}
_LOCKS_GUARD = threading.Lock()


def is_clickhouse_write() -> bool:
    value = str(getattr(config, "MARKET_WRITE_BACKEND", "mysql") or "mysql").strip().lower()
    if value not in {"mysql", "clickhouse"}:
        raise RuntimeError("MARKET_WRITE_BACKEND must be 'mysql' or 'clickhouse'")
    return value == "clickhouse"


def get_clickhouse_write_repository() -> ClickHouseMarketWriteRepository:
    if not config.CLICKHOUSE_URL:
        raise RuntimeError("CLICKHOUSE_URL is required when MARKET_WRITE_BACKEND=clickhouse")
    signature = (
        config.CLICKHOUSE_URL,
        config.CLICKHOUSE_DATABASE,
        config.CLICKHOUSE_USER,
        config.CLICKHOUSE_PASSWORD,
        config.CLICKHOUSE_WRITE_TIMEOUT_SECONDS,
        config.CLICKHOUSE_WRITE_RETRIES,
    )
    existing = getattr(_LOCAL, "market_write", None)
    if existing is not None and getattr(_LOCAL, "market_write_signature", None) == signature:
        return existing
    client = ClickHouseWriteClient(
        config.CLICKHOUSE_URL,
        config.CLICKHOUSE_DATABASE,
        config.CLICKHOUSE_USER,
        config.CLICKHOUSE_PASSWORD,
        timeout=(10.0, float(config.CLICKHOUSE_WRITE_TIMEOUT_SECONDS)),
        retries=config.CLICKHOUSE_WRITE_RETRIES,
    )
    repository = ClickHouseMarketWriteRepository(client)
    _LOCAL.market_write_client = client
    _LOCAL.market_write = repository
    _LOCAL.market_write_signature = signature
    return repository


def close_thread_local_write_client() -> None:
    client = getattr(_LOCAL, "market_write_client", None)
    if client is not None:
        client.close()
    _LOCAL.market_write_client = None
    _LOCAL.market_write = None
    _LOCAL.market_write_signature = None


def market_write_health() -> dict:
    """Read-only preflight for the configured ClickHouse market schema."""
    tables = tuple(MARKET_TABLE_COLUMNS)
    result = {
        'backend': getattr(config, 'MARKET_WRITE_BACKEND', 'mysql'),
        'configured': bool(config.CLICKHOUSE_URL),
        'tables': list(tables),
        'missing_tables': [],
        'missing_columns': {},
    }
    if not is_clickhouse_write():
        result['healthy'] = True
        result['message'] = 'ClickHouse market writes are disabled'
        return result
    try:
        client = get_clickhouse_write_repository().client
        rows = client.query_rows(
            "SELECT name, engine, sorting_key FROM system.tables "
            f"WHERE database = '{config.CLICKHOUSE_DATABASE}' "
            f"AND name IN ({', '.join(repr(table) for table in tables)})"
        )
        existing_rows = {str(row['name']): row for row in rows}
        existing = set(existing_rows)
        result['missing_tables'] = [table for table in tables if table not in existing]
        for table in tables:
            if table not in existing:
                continue
            schema = MARKET_TABLE_SCHEMA[table]
            row = existing_rows[table]
            expected_engine = str(schema['engine'])
            actual_engine = str(row.get('engine') or '')
            if actual_engine != expected_engine:
                result.setdefault('schema_errors', {}).setdefault(table, []).append(
                    f'engine={actual_engine!r}, expected={expected_engine!r}'
                )
            sorting_key = str(row.get('sorting_key') or '')
            missing_key_parts = [
                part for part in schema['sorting_key']
                if not re.search(rf'(?<![A-Za-z0-9_]){re.escape(part)}(?![A-Za-z0-9_])', sorting_key)
            ]
            if missing_key_parts:
                result.setdefault('schema_errors', {}).setdefault(table, []).append(
                    f'sorting_key missing {missing_key_parts!r}: {sorting_key!r}'
                )
            columns = client.query_rows(
                f"DESCRIBE TABLE {config.CLICKHOUSE_DATABASE}.{table}"
            )
            names = {str(row.get('name')) for row in columns}
            missing = [column for column in MARKET_TABLE_COLUMNS[table] if column not in names]
            if missing:
                result['missing_columns'][table] = missing
            expected_time_type = schema.get('time_type')
            if expected_time_type:
                time_column = 'updated_at' if table == 'market_tickers' else 'created_at'
                actual_type = next(
                    (str(column.get('type') or '') for column in columns if column.get('name') == time_column),
                    '',
                )
                if expected_time_type not in actual_type or 'Asia/Shanghai' not in actual_type:
                    result.setdefault('schema_errors', {}).setdefault(table, []).append(
                        f'{time_column} type={actual_type!r}, expected Asia/Shanghai DateTime64(3)'
                    )
        result['healthy'] = (
            not result['missing_tables']
            and not result['missing_columns']
            and not result.get('schema_errors')
        )
        return result
    except Exception as exc:
        result['healthy'] = False
        result['error'] = f'{type(exc).__name__}: {exc}'
        return result


def market_write_lock(table: str, scope: str):
    key = (str(table), str(scope))
    with _LOCKS_GUARD:
        lock = _LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _LOCKS[key] = lock
    return lock
