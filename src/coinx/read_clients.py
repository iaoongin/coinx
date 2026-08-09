"""Small read-only database clients used by the migration validation path.

The application SQLAlchemy session remains the MySQL transactional session. These
clients deliberately expose queries only, so a read-only migration test cannot
accidentally INSERT, UPDATE, or DELETE production data.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pymysql
import requests


logger = logging.getLogger(__name__)


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
        response = self.session.post(
            self.url,
            params={"query": f"{statement} FORMAT JSONEachRow", "database": self.database},
            auth=self.auth,
            timeout=self.timeout,
        )
        if not response.ok:
            detail = response.text.strip() or "ClickHouse returned no error details."
            # Older installations may still have the two tables that are
            # being upgraded as plain MergeTree.  ClickHouse rejects FINAL
            # for those tables; retrying the same read without FINAL is safe
            # for that engine because it has no version rows to collapse.  Do
            # not broaden this fallback to any other server error.
            if "ILLEGAL_FINAL" in detail and re.search(r"\bFINAL\b", statement, re.IGNORECASE):
                fallback_statement = re.sub(r"\s+FINAL\b", "", statement, flags=re.IGNORECASE)
                logger.warning("ClickHouse table does not support FINAL; retrying read without FINAL")
                response = self.session.post(
                    self.url,
                    params={"query": f"{fallback_statement} FORMAT JSONEachRow", "database": self.database},
                    auth=self.auth,
                    timeout=self.timeout,
                )
                if response.ok:
                    return [json.loads(line) for line in response.text.splitlines() if line.strip()]
                detail = response.text.strip() or "ClickHouse returned no error details."
            raise requests.HTTPError(
                f"ClickHouse HTTP {response.status_code}: {detail}", response=response
            )
        rows = []
        for line in response.text.splitlines():
            line = line.strip()
            if line:
                rows.append(json.loads(line))
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
