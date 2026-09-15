"""Small read-only database clients used by the migration validation path.

The application SQLAlchemy session remains the MySQL transactional session. These
clients deliberately expose queries only, so a read-only migration test cannot
accidentally INSERT, UPDATE, or DELETE production data.
"""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from threading import BoundedSemaphore
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pymysql
import requests

from coinx.config import (
    CLICKHOUSE_MAX_CONCURRENT_QUERIES,
    CLICKHOUSE_MAX_MEMORY_USAGE_BYTES,
    CLICKHOUSE_QUERY_MAX_THREADS,
)

logger = logging.getLogger(__name__)
_CLICKHOUSE_QUERY_CONTEXT = ContextVar('clickhouse_query_context', default={})
_CLICKHOUSE_QUERY_DEADLINE = ContextVar('clickhouse_query_deadline', default=None)
_CLICKHOUSE_QUERY_SEMAPHORE = BoundedSemaphore(max(1, int(CLICKHOUSE_MAX_CONCURRENT_QUERIES)))
_CLICKHOUSE_QUERY_SETTINGS = {
    "max_threads": max(1, int(CLICKHOUSE_QUERY_MAX_THREADS)),
}
if int(CLICKHOUSE_MAX_MEMORY_USAGE_BYTES) > 0:
    _CLICKHOUSE_QUERY_SETTINGS["max_memory_usage"] = int(CLICKHOUSE_MAX_MEMORY_USAGE_BYTES)


@contextmanager
def clickhouse_query_context(**fields):
    """Attach diagnostic fields to queries without sharing state across threads."""
    token = _CLICKHOUSE_QUERY_CONTEXT.set({**_CLICKHOUSE_QUERY_CONTEXT.get(), **fields})
    try:
        yield
    finally:
        _CLICKHOUSE_QUERY_CONTEXT.reset(token)


@contextmanager
def clickhouse_query_deadline(timeout_seconds):
    """Bound a group of ClickHouse reads by one monotonic deadline."""
    timeout = max(0.1, float(timeout_seconds))
    token = _CLICKHOUSE_QUERY_DEADLINE.set(time.monotonic() + timeout)
    try:
        yield
    finally:
        _CLICKHOUSE_QUERY_DEADLINE.reset(token)


def _remaining_query_timeout():
    deadline = _CLICKHOUSE_QUERY_DEADLINE.get()
    if deadline is None:
        return None
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError('ClickHouse homepage query deadline exceeded')
    return remaining


def _clickhouse_response_stats(response):
    headers = getattr(response, 'headers', None) or {}
    try:
        summary = json.loads(headers.get('X-ClickHouse-Summary', '{}'))
    except (TypeError, ValueError):
        summary = {}
    if not isinstance(summary, dict):
        summary = {}
    # Missing counters are unknown, not zero. In particular, a current memory
    # counter must never be presented as peak memory or as server-wide RSS.
    return {
        'http_status': getattr(response, 'status_code', None),
        'server_query_id': headers.get('X-ClickHouse-Query-Id'),
        'exception_code': headers.get('X-ClickHouse-Exception-Code'),
        **{key: summary.get(key) for key in (
            'read_rows', 'read_bytes', 'elapsed_ns', 'memory_usage', 'peak_memory_usage',
        )},
    }


class ReadOnlyQueryError(ValueError):
    """Raised when a client is asked to execute a non-read SQL statement."""


_READ_ONLY_PREFIX = re.compile(r"^(?:SELECT|WITH|SHOW|DESCRIBE|DESC|EXPLAIN)\b", re.IGNORECASE)


def assert_read_only(sql: str) -> str:
    statement = (sql or "").strip().rstrip(";").strip()
    if not statement or not _READ_ONLY_PREFIX.match(statement):
        raise ReadOnlyQueryError("only SELECT/WITH/SHOW/DESCRIBE/EXPLAIN queries are allowed")
    return statement


def parse_host_port(value: str, default_port: int) -> Tuple[str, int]:
    """Parse ``host`` or ``host:port`` values used by the test commands."""
    value = (value or "").strip()
    if not value:
        raise ValueError("database host cannot be empty")
    if value.startswith("[") and "]" in value:
        host, suffix = value[1:].split("]", 1)
        return host, int(suffix[1:]) if suffix.startswith(":") else default_port
    if value.count(":") == 1:
        host, port = value.rsplit(":", 1)
        if port.isdigit():
            return host, int(port)
    return value, default_port


class ClickHouseReadClient:
    """HTTP client with an intentionally read-only surface."""

    def __init__(
        self,
        url: str,
        database: str,
        user: str,
        password: str,
        timeout: Tuple[float, float] = (10.0, 120.0),
        session: Optional[requests.Session] = None,
    ) -> None:
        self.url = (url or "").rstrip("/")
        if not self.url:
            raise ValueError("ClickHouse URL cannot be empty")
        self.database = database
        self.auth = (user, password)
        self.timeout = timeout
        self.session = session or requests.Session()
        self._owns_session = session is None

    def query_rows(self, sql: str) -> List[Dict[str, Any]]:
        statement = assert_read_only(sql)
        try:
            return self._query_rows_once(statement)
        except requests.HTTPError as exc:
            # Older installations may still have the two tables that are
            # being upgraded as plain MergeTree.  ClickHouse rejects FINAL
            # for those tables; retrying the same read without FINAL is safe
            # for that engine because it has no version rows to collapse.  Do
            # not broaden this fallback to any other server error.
            if "ILLEGAL_FINAL" in str(exc) and re.search(r"\bFINAL\b", statement, re.IGNORECASE):
                fallback_statement = re.sub(r"\s+FINAL\b", "", statement, flags=re.IGNORECASE)
                logger.warning(
                    "ClickHouse table does not support FINAL; retrying read without FINAL: query_id=%s",
                    getattr(exc, 'clickhouse_query_id', None),
                )
                return self._query_rows_once(
                    fallback_statement, retry_of=getattr(exc, 'clickhouse_query_id', None),
                )
            raise

    def _query_rows_once(self, statement, retry_of=None):
        query_id = f'coinx_read_{uuid.uuid4().hex}'
        context = {
            **_CLICKHOUSE_QUERY_CONTEXT.get(),
            'query_id': query_id,
            'database': self.database,
        }
        if retry_of is not None:
            context['retry_of'] = retry_of
        response = None
        started = time.perf_counter()
        try:
            remaining = _remaining_query_timeout()
            with _CLICKHOUSE_QUERY_SEMAPHORE:
                remaining = _remaining_query_timeout()
                context['queue_wait_ms'] = round((time.perf_counter() - started) * 1000, 1)
                started = time.perf_counter()
                logger.info('ClickHouse query started: %s', json.dumps(context, ensure_ascii=False))
                read_timeout = self.timeout[1]
                if remaining is not None:
                    read_timeout = min(float(read_timeout), max(0.1, remaining))
                response = self.session.post(
                    self.url,
                    params={
                        'query': f'{statement} FORMAT JSONEachRow',
                        'database': self.database,
                        'query_id': query_id,
                        **_CLICKHOUSE_QUERY_SETTINGS,
                    },
                    auth=self.auth,
                    timeout=(self.timeout[0], read_timeout),
                )
            if not response.ok:
                detail = response.text.strip() or "ClickHouse returned no error details."
                raise requests.HTTPError(
                    f'ClickHouse HTTP {response.status_code}: {detail} [query_id={query_id}]',
                    response=response,
                )
            rows = [json.loads(line) for line in response.text.splitlines() if line.strip()]
        except Exception as exc:
            # Preserve exception type and response for existing callers while
            # allowing the enclosing homepage failure to identify this query.
            exc.clickhouse_query_id = query_id
            logger.error('ClickHouse query failed: %s', json.dumps({
                **context,
                **_clickhouse_response_stats(response),
                'duration_ms': round((time.perf_counter() - started) * 1000, 1),
                'error_type': type(exc).__name__,
                'error': str(exc),
            }, ensure_ascii=False))
            raise
        logger.info('ClickHouse query finished: %s', json.dumps({
            **context,
            **_clickhouse_response_stats(response),
            'duration_ms': round((time.perf_counter() - started) * 1000, 1),
            'result_count': len(rows),
        }, ensure_ascii=False))
        return rows

    def query_scalar(self, sql: str) -> Any:
        rows = self.query_rows(sql)
        if not rows:
            return None
        return next(iter(rows[0].values()))

    def close(self) -> None:
        if self._owns_session:
            self.session.close()

    def __enter__(self) -> "ClickHouseReadClient":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()


class MySQLReadClient:
    """PyMySQL read client kept separate from the application's ORM session."""

    def __init__(
        self,
        host: str,
        database: str,
        user: str,
        password: str,
        port: int = 3306,
        connect_timeout: int = 10,
    ) -> None:
        parsed_host, parsed_port = parse_host_port(host, port)
        self.connection = pymysql.connect(
            host=parsed_host,
            port=parsed_port,
            user=user,
            password=password,
            database=database,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
            connect_timeout=connect_timeout,
            read_timeout=120,
            write_timeout=120,
        )

    def query_rows(self, sql: str, args: Sequence[Any] = ()) -> List[Dict[str, Any]]:
        statement = assert_read_only(sql)
        with self.connection.cursor() as cursor:
            cursor.execute(statement, tuple(args))
            return list(cursor.fetchall())

    def query_scalar(self, sql: str, args: Sequence[Any] = ()) -> Any:
        rows = self.query_rows(sql, args)
        if not rows:
            return None
        return next(iter(rows[0].values()))

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "MySQLReadClient":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
