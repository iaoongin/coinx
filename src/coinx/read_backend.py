"""Primary market read-backend selection for API repository calls.

The selection is process-scoped. A MySQL instance and a ClickHouse instance
run the same application version with different backend configuration.
ClickHouse clients are thread-local because requests.Session is not intended
to be shared across concurrent worker threads.
"""

from __future__ import annotations

import threading
from typing import Optional

from coinx import config
from coinx.read_clients import ClickHouseReadClient
from coinx.repositories.market_read import ClickHouseMarketReadRepository


_LOCAL = threading.local()


def get_read_backend() -> str:
    # Environment variables are resolved once by config.py.
    value = getattr(config, "READ_BACKEND", "mysql")
    value = str(value or "mysql").strip().lower()
    if value not in {"mysql", "clickhouse"}:
        raise RuntimeError("READ_BACKEND must be 'mysql' or 'clickhouse'")
    return value


def is_clickhouse_read() -> bool:
    return get_read_backend() == "clickhouse"


def get_clickhouse_repository() -> ClickHouseMarketReadRepository:
    """Return a thread-local read-only ClickHouse repository."""
    if not config.CLICKHOUSE_URL:
        raise RuntimeError("CLICKHOUSE_URL is required when READ_BACKEND=clickhouse")

    signature = (
        config.CLICKHOUSE_URL,
        config.CLICKHOUSE_DATABASE,
        config.CLICKHOUSE_USER,
        config.CLICKHOUSE_PASSWORD,
        config.CLICKHOUSE_READ_TIMEOUT_SECONDS,
    )
    existing = getattr(_LOCAL, "clickhouse", None)
    if existing is not None and getattr(_LOCAL, "clickhouse_signature", None) == signature:
        return existing

    client = ClickHouseReadClient(
        config.CLICKHOUSE_URL,
        config.CLICKHOUSE_DATABASE,
        config.CLICKHOUSE_USER,
        config.CLICKHOUSE_PASSWORD,
        timeout=(10.0, float(config.CLICKHOUSE_READ_TIMEOUT_SECONDS)),
    )
    repository = ClickHouseMarketReadRepository(client, config.CLICKHOUSE_DATABASE)
    _LOCAL.clickhouse_client = client
    _LOCAL.clickhouse = repository
    _LOCAL.clickhouse_signature = signature
    return repository


def close_thread_local_read_client() -> None:
    client: Optional[ClickHouseReadClient] = getattr(_LOCAL, "clickhouse_client", None)
    if client is not None:
        client.close()
        _LOCAL.clickhouse_client = None
        _LOCAL.clickhouse = None
        _LOCAL.clickhouse_signature = None


def read_backend_health() -> dict:
    result = {
        "backend": get_read_backend(),
        "clickhouse_configured": bool(config.CLICKHOUSE_URL),
        "read_only": True,
    }
    if getattr(config, 'MARKET_WRITE_BACKEND', 'mysql') == 'clickhouse':
        from coinx.write_backend import market_write_health
        result['market_write'] = market_write_health()
    try:
        if is_clickhouse_read():
            if not config.CLICKHOUSE_URL:
                raise RuntimeError("CLICKHOUSE_URL is required for ClickHouse reads")
            value = get_clickhouse_repository().client.query_scalar("SELECT 1")
        else:
            from sqlalchemy import text
            from coinx.database import get_session

            session = get_session()
            try:
                value = session.execute(text("SELECT 1")).scalar()
            finally:
                session.close()
        result["healthy"] = (
            value is not None
            and result.get('market_write', {}).get('healthy', True)
        )
        if value is not None:
            result["probe"] = int(value)
    except Exception as exc:
        result["healthy"] = False
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result
