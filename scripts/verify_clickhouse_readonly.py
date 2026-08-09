#!/usr/bin/env python3
"""Compare read-only market results between MySQL and ClickHouse.

Example (PowerShell):

    python scripts/verify_clickhouse_readonly.py `
        --clickhouse-url http://10.0.0.128:8123 `
        --clickhouse-database coinx `
        --clickhouse-user $env:CLICKHOUSE_USER `
        --clickhouse-password $env:CLICKHOUSE_PASSWORD `
        --mysql-host 10.0.0.128:13306 `
        --mysql-database coinx `
        --mysql-user $env:MYSQL_USER `
        --mysql-password $env:MYSQL_PASSWORD `
        --symbols BTCUSDT,ETHUSDT

The script only executes SELECT queries. It does not create tables or mutate data.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from coinx.read_clients import ClickHouseReadClient, MySQLReadClient
from coinx.repositories.market_read import (
    ClickHouseMarketReadRepository,
    MySQLMarketReadRepository,
    TABLES,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clickhouse-url", default=os.getenv("COINX_CK_TEST_URL", "http://127.0.0.1:8123"))
    parser.add_argument("--clickhouse-database", default=os.getenv("COINX_CK_TEST_DATABASE", "coinx"))
    parser.add_argument("--clickhouse-user", default=os.getenv("COINX_CK_TEST_USER", "default"))
    parser.add_argument("--clickhouse-password", default=os.getenv("COINX_CK_TEST_PASSWORD", ""))
    parser.add_argument("--mysql-host", default=os.getenv("COINX_MYSQL_TEST_HOST", "127.0.0.1:3306"))
    parser.add_argument("--mysql-database", default=os.getenv("COINX_MYSQL_TEST_DATABASE", "coinx"))
    parser.add_argument("--mysql-user", default=os.getenv("COINX_MYSQL_TEST_USER", "root"))
    parser.add_argument("--mysql-password", default=os.getenv("COINX_MYSQL_TEST_PASSWORD"))
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--ticker-limit", type=int, default=20)
    parser.add_argument(
        "--allow-differences",
        action="store_true",
        help="keep exit code zero even when a comparison differs",
    )
    args = parser.parse_args()
    if not args.mysql_password:
        parser.error("--mysql-password or COINX_MYSQL_TEST_PASSWORD is required")
    if args.hours <= 0 or args.ticker_limit <= 0:
        parser.error("--hours and --ticker-limit must be positive")
    args.symbols = [symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()]
    if not args.symbols:
        parser.error("--symbols must contain at least one symbol")
    return args


def _same_number(left: Any, right: Any, tolerance: float = 1e-8) -> bool:
    if left is None or right is None:
        return left is right
    try:
        return abs(float(left) - float(right)) <= tolerance
    except (TypeError, ValueError):
        return left == right


def _compare_rows(left_rows: List[Dict[str, Any]], right_rows: List[Dict[str, Any]], key: str) -> Tuple[bool, str]:
    left = {row[key]: row for row in left_rows}
    right = {row[key]: row for row in right_rows}
    if set(left) != set(right):
        return False, f"keys differ: mysql_only={sorted(set(left) - set(right))[:5]} ck_only={sorted(set(right) - set(left))[:5]}"
    for row_key in left:
        for field in set(left[row_key]) | set(right[row_key]):
            if field == "updated_at" or field == "created_at":
                continue
            if not _same_number(left[row_key].get(field), right[row_key].get(field)):
                return False, f"{row_key}.{field}: mysql={left[row_key].get(field)!r} ck={right[row_key].get(field)!r}"
    return True, f"{len(left)} rows"


def main() -> int:
    args = parse_args()
    failures = 0
    checks = 0
    print("Connecting to MySQL and ClickHouse (read-only checks only)...", flush=True)
    with ClickHouseReadClient(
        args.clickhouse_url,
        args.clickhouse_database,
        args.clickhouse_user,
        args.clickhouse_password,
    ) as ck_client, MySQLReadClient(
        args.mysql_host,
        args.mysql_database,
        args.mysql_user,
        args.mysql_password,
    ) as mysql_client:
        ck = ClickHouseMarketReadRepository(ck_client, args.clickhouse_database)
        mysql = MySQLMarketReadRepository(mysql_client)

        for table in TABLES:
            checks += 1
            mysql_count = mysql.table_count(table)
            ck_count = ck.table_count(table)
            ok = mysql_count == ck_count
            print(f"[{('OK' if ok else 'DIFF')}] {table} count: MySQL={mysql_count} CK={ck_count}", flush=True)
            failures += 0 if ok else 1

        for table in TABLES:
            checks += 1
            mysql_bounds = mysql.time_bounds(table)
            ck_bounds = ck.time_bounds(table)
            ok = mysql_bounds == ck_bounds
            print(f"[{('OK' if ok else 'DIFF')}] {table} bounds: MySQL={mysql_bounds} CK={ck_bounds}", flush=True)
            failures += 0 if ok else 1

        checks += 1
        mysql_rates = mysql.latest_funding_rates(args.symbols)
        ck_rates = ck.latest_funding_rates(args.symbols)
        ok, detail = _compare_rows(list(mysql_rates.values()), list(ck_rates.values()), "symbol")
        print(f"[{('OK' if ok else 'DIFF')}] latest funding rates: {detail}", flush=True)
        failures += 0 if ok else 1

        for symbol in args.symbols:
            checks += 1
            mysql_rows = mysql.funding_rate_history(symbol, hours=args.hours)
            ck_rows = ck.funding_rate_history(symbol, hours=args.hours)
            ok, detail = _compare_rows(mysql_rows, ck_rows, "event_time")
            print(f"[{('OK' if ok else 'DIFF')}] funding history {symbol}: {detail}", flush=True)
            failures += 0 if ok else 1

            for table in ("market_klines", "market_open_interest_hist", "market_taker_buy_sell_vol"):
                checks += 1
                mysql_rows = mysql.series_window(table, symbol, hours=args.hours, exchange="binance")
                ck_rows = ck.series_window(table, symbol, hours=args.hours, exchange="binance")
                key = "open_time" if table == "market_klines" else "event_time"
                ok, detail = _compare_rows(mysql_rows, ck_rows, key)
                print(f"[{('OK' if ok else 'DIFF')}] {table} {symbol}: {detail}", flush=True)
                failures += 0 if ok else 1

        checks += 1
        mysql_tickers = mysql.latest_tickers(limit=args.ticker_limit)
        ck_tickers = ck.latest_tickers(limit=args.ticker_limit)
        ok, detail = _compare_rows(mysql_tickers, ck_tickers, "symbol")
        print(f"[{('OK' if ok else 'DIFF')}] latest tickers: {detail}", flush=True)
        failures += 0 if ok else 1

    print(f"Readonly verification complete: checks={checks} failures={failures}")
    return 1 if failures and not args.allow_differences else 0


if __name__ == "__main__":
    raise SystemExit(main())
