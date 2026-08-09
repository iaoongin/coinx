#!/usr/bin/env python3
"""Resumable, ID-range import of the current MySQL market data into ClickHouse.

Run after creating the ClickHouse schema and stopping application writes:

    python scripts/import_mysql_to_clickhouse.py `
        --clickhouse-url http://10.0.0.128:8123 `
        --clickhouse-database coinx `
        --clickhouse-user $env:CLICKHOUSE_USER `
        --clickhouse-password $env:CLICKHOUSE_PASSWORD `
        --mysql-host 10.0.0.128:13306 `
        --mysql-user $env:MYSQL_USER `
        --mysql-password $env:MYSQL_PASSWORD

        python scripts/import_mysql_to_clickhouse.py `
                --clickhouse-url http://10.0.0.128:8123 `
                --clickhouse-database coinx `
                --clickhouse-user root `
                --clickhouse-password root `
                --mysql-host 10.0.0.128:13306 ` 
                --mysql-user root `
                --mysql-password 'coin123321'

        curl.exe --user "${env:CLICKHOUSE_USER}:$env:CLICKHOUSE_PASSWORD" --data-binary "DROP DATABASE IF EXISTS mysql_source" http://10.0.0.128:8123/

The script records each source table's max(id) before importing. It advances its
checkpoint only after a batch INSERT succeeds, so rerunning resumes automatically.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import pymysql
from tqdm import tqdm


TABLES = (
    {
        "name": "market_klines",
        "insert_columns": "exchange, symbol, period, open_time, close_time, open_price, high_price, low_price, close_price, volume, quote_volume, trade_count, taker_buy_base_volume, taker_buy_quote_volume, created_at, updated_at",
        "select_columns": "exchange, symbol, period, open_time, close_time, open_price, high_price, low_price, close_price, volume, quote_volume, trade_count, taker_buy_base_volume, taker_buy_quote_volume, toDateTime64(created_at, 3, 'Asia/Shanghai'), toDateTime64(updated_at, 3, 'Asia/Shanghai')",
    },
    {
        "name": "market_open_interest_hist",
        "insert_columns": "exchange, symbol, period, event_time, sum_open_interest, sum_open_interest_value, created_at, updated_at",
        "select_columns": "exchange, symbol, period, event_time, sum_open_interest, sum_open_interest_value, toDateTime64(created_at, 3, 'Asia/Shanghai'), toDateTime64(updated_at, 3, 'Asia/Shanghai')",
    },
    {
        "name": "market_taker_buy_sell_vol",
        "insert_columns": "exchange, symbol, period, event_time, buy_sell_ratio, buy_vol, sell_vol, created_at, updated_at",
        "select_columns": "exchange, symbol, period, event_time, buy_sell_ratio, buy_vol, sell_vol, toDateTime64(created_at, 3, 'Asia/Shanghai'), toDateTime64(updated_at, 3, 'Asia/Shanghai')",
    },
    {
        "name": "market_funding_rate",
        "insert_columns": "exchange, symbol, period, event_time, funding_rate, predicted_rate, next_funding_time, mark_price, created_at, updated_at",
        "select_columns": "exchange, symbol, period, event_time, funding_rate, predicted_rate, next_funding_time, mark_price, toDateTime64(created_at, 3, 'Asia/Shanghai'), toDateTime64(created_at, 3, 'Asia/Shanghai')",
    },
    {
        "name": "market_snapshots",
        "insert_columns": "snapshot_time, symbol, batch_id, price, open_interest, open_interest_value, data_json, created_at",
        "select_columns": "snapshot_time, symbol, batch_id, price, open_interest, open_interest_value, ifNull(toString(data_json), '{}'), toDateTime64(created_at, 3, 'Asia/Shanghai')",
    },
    {
        "name": "market_tickers",
        "insert_columns": "close_time, symbol, price_change, price_change_percent, weighted_avg_price, last_price, last_qty, open_price, high_price, low_price, volume, quote_volume, open_time, first_id, last_id, count, created_at, updated_at",
        "select_columns": "close_time, symbol, price_change, price_change_percent, weighted_avg_price, last_price, last_qty, open_price, high_price, low_price, volume, quote_volume, open_time, first_id, last_id, count, toDateTime64(created_at, 3, 'Asia/Shanghai'), toDateTime64(created_at, 3, 'Asia/Shanghai')",
    },
)

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def status(message: str) -> None:
    print(message, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clickhouse-url", default="http://127.0.0.1:8123")
    parser.add_argument("--clickhouse-user", default="default")
    parser.add_argument("--clickhouse-password", default="")
    parser.add_argument("--clickhouse-database", default="coinx")
    parser.add_argument("--mysql-host", default="mysql:3306")
    parser.add_argument("--mysql-database", default="coinx")
    parser.add_argument("--mysql-user", default="coinx")
    parser.add_argument("--mysql-password", required=True)
    parser.add_argument("--batch-id-span", type=int, default=200_000)
    parser.add_argument("--max-threads", type=int, default=2)
    parser.add_argument("--state-file", type=Path, default=Path("data/clickhouse-migration-state.json"))
    parser.add_argument("--allow-non-empty-target", action="store_true")
    parser.add_argument("--reset-state", action="store_true")
    parser.add_argument(
        "--refresh-watermark",
        action="store_true",
        help="refresh saved source max(id) and import only IDs after each checkpoint",
    )
    args = parser.parse_args()

    for value in (args.clickhouse_database, args.mysql_database):
        if not IDENTIFIER.fullmatch(value):
            parser.error(f"Invalid database identifier: {value}")
    if not 10_000 <= args.batch_id_span <= 2_000_000:
        parser.error("--batch-id-span must be between 10,000 and 2,000,000")
    if not 1 <= args.max_threads <= 8:
        parser.error("--max-threads must be between 1 and 8")
    return args


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def load_state(path: Path, args: argparse.Namespace) -> dict[str, Any]:
    recovery_path = path.with_suffix(path.suffix + ".recovery")
    if args.reset_state and path.exists():
        path.unlink()
    if args.reset_state and recovery_path.exists():
        recovery_path.unlink()
    candidates = [candidate for candidate in (path, recovery_path) if candidate.exists()]
    if candidates:
        # A recovery file can be newer when Windows temporarily locked the main file.
        source_path = max(candidates, key=lambda candidate: candidate.stat().st_mtime_ns)
        return json.loads(source_path.read_text(encoding="utf-8"))
    return {
        "version": 1,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "source": {"mysql_host": args.mysql_host, "mysql_database": args.mysql_database},
        "tables": {},
    }


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    contents = json.dumps(state, ensure_ascii=False, indent=2) + "\n"
    temporary_path.write_text(contents, encoding="utf-8")
    for attempt in range(5):
        try:
            os.replace(temporary_path, path)
            return
        except PermissionError:
            if attempt == 4:
                recovery_path = path.with_suffix(path.suffix + ".recovery")
                recovery_path.write_text(contents, encoding="utf-8")
                status(
                    f"Warning: state file is locked; checkpoint saved to {recovery_path}. "
                    "Close programs holding the state file before resuming."
                )
                return
            time.sleep(0.5 * (attempt + 1))


class ClickHouse:
    def __init__(self, args: argparse.Namespace) -> None:
        self.url = args.clickhouse_url.rstrip("/")
        self.auth = (args.clickhouse_user, args.clickhouse_password)
        self.max_threads = args.max_threads

    def query(self, sql: str) -> str:
        response = requests.post(
            self.url,
            params={"query": sql, "max_threads": self.max_threads},
            auth=self.auth,
            timeout=(10, 3600),
        )
        if not response.ok:
            detail = response.text.strip() or "ClickHouse returned no error details."
            raise requests.HTTPError(
                f"HTTP {response.status_code} for ClickHouse query:\n{detail}",
                response=response,
            )
        return response.text

    def scalar_int(self, sql: str) -> int:
        value = self.query(f"{sql} FORMAT TSVRaw").strip()
        if not value:
            raise RuntimeError(f"Expected one scalar result for: {sql}")
        return int(value)


class MySqlMetadata:
    """Use MySQL directly for indexed ID metadata queries.

    ClickHouse's MySQL database engine can evaluate aggregates locally, which
    turns MAX(id) or COUNT(*) into an expensive remote table scan.
    """

    def __init__(self, args: argparse.Namespace) -> None:
        host, separator, port_text = args.mysql_host.rpartition(":")
        self.host = host if separator else args.mysql_host
        self.port = int(port_text) if separator else 3306
        self.connection = pymysql.connect(
            host=self.host,
            port=self.port,
            user=args.mysql_user,
            password=args.mysql_password,
            database=args.mysql_database,
            charset="utf8mb4",
            connect_timeout=10,
            read_timeout=120,
            write_timeout=10,
            autocommit=True,
        )

    def scalar_int(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        self.connection.ping(reconnect=True)
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            value = cursor.fetchone()[0]
        return int(value or 0)

    def max_id(self, table_name: str) -> int:
        return self.scalar_int(f"SELECT MAX(id) FROM `{table_name}`")

    def count_to_id(self, table_name: str, high_watermark: int) -> int:
        return self.scalar_int(f"SELECT COUNT(*) FROM `{table_name}` WHERE id <= %s", (high_watermark,))

    def count_range(self, table_name: str, lower_id: int, upper_id: int) -> int:
        return self.scalar_int(
            f"SELECT COUNT(*) FROM `{table_name}` WHERE id > %s AND id <= %s",
            (lower_id, upper_id),
        )

    def close(self) -> None:
        self.connection.close()


def format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def ensure_source_database(ch: ClickHouse, args: argparse.Namespace) -> str:
    source_database = "mysql_source"
    ch.query(
        f"CREATE DATABASE IF NOT EXISTS {source_database} ENGINE = MySQL("
        f"{sql_string(args.mysql_host)}, {sql_string(args.mysql_database)}, "
        f"{sql_string(args.mysql_user)}, {sql_string(args.mysql_password)})"
    )
    return source_database


def main() -> int:
    args = parse_args()
    ch = ClickHouse(args)
    state = load_state(args.state_file, args)

    status(f"Connecting to ClickHouse at {args.clickhouse_url}...")
    version = ch.query("SELECT version() FORMAT TSVRaw").strip()
    status(f"Connected to ClickHouse {version}.")

    status(f"Creating or reusing MySQL external source for {args.mysql_host}...")
    source_database = ensure_source_database(ch, args)
    status(f"MySQL external source '{source_database}' is ready.")
    status(f"Connecting directly to MySQL at {args.mysql_host} for ID checkpoints...")
    mysql_metadata = MySqlMetadata(args)
    status("Direct MySQL checkpoint connection is ready.")
    has_prior_state = bool(state["tables"])

    for table in TABLES:
        status(f"Checking target table {args.clickhouse_database}.{table['name']}...")
        target_rows = ch.scalar_int(f"SELECT count() FROM {args.clickhouse_database}.{table['name']}")
        if target_rows and not (args.allow_non_empty_target or has_prior_state):
            raise RuntimeError(
                f"Target {args.clickhouse_database}.{table['name']} already contains {target_rows} rows. "
                "Use empty target tables for a new migration."
            )

    # Capture every table's fixed import boundary before writing any data.
    for table in TABLES:
        name = table["name"]
        table_state = state["tables"].get(name)
        if table_state is None:
            status(f"[{name}] Reading source max(id) and row count from MySQL...")
            high_watermark = mysql_metadata.max_id(name)
            table_state = {
                "high_watermark_id": high_watermark,
                "last_success_id": 0,
                "imported_rows": 0,
                "source_total_rows": mysql_metadata.count_to_id(name, high_watermark),
                "elapsed_seconds": 0.0,
                "last_batch_rows": 0,
                "last_batch_seconds": 0.0,
                "completed": False,
            }
            state["tables"][name] = table_state
        else:
            high_watermark = int(table_state["high_watermark_id"])
            last_success_id = int(table_state["last_success_id"])
            if args.refresh_watermark:
                status(f"[{name}] Refreshing source max(id) after checkpoint {last_success_id}...")
                refreshed_watermark = mysql_metadata.max_id(name)
                if refreshed_watermark < last_success_id:
                    raise RuntimeError(
                        f"Source {name} max(id) {refreshed_watermark} is behind checkpoint "
                        f"{last_success_id}; use a new state file after verifying the source."
                    )
                if refreshed_watermark != high_watermark:
                    old_watermark = high_watermark
                    high_watermark = refreshed_watermark
                    table_state["high_watermark_id"] = high_watermark
                    table_state["source_total_rows"] = mysql_metadata.count_to_id(
                        name,
                        high_watermark,
                    )
                    table_state["completed"] = False
                    status(
                        f"[{name}] New source high watermark: {old_watermark} -> "
                        f"{high_watermark}; importing only new IDs."
                    )
            status(f"[{name}] Resuming from saved checkpoint ID {last_success_id} of {high_watermark}.")
            if "source_total_rows" not in table_state:
                table_state["source_total_rows"] = mysql_metadata.count_to_id(name, high_watermark)
            if "elapsed_seconds" not in table_state:
                table_state["elapsed_seconds"] = 0.0
            if "last_batch_rows" not in table_state:
                table_state["last_batch_rows"] = 0
            if "last_batch_seconds" not in table_state:
                table_state["last_batch_seconds"] = 0.0
        save_state(args.state_file, state)

    overall_total_rows = sum(int(state["tables"][table["name"]]["source_total_rows"]) for table in TABLES)
    overall_done_rows = sum(int(state["tables"][table["name"]]["imported_rows"]) for table in TABLES)
    overall_bar = tqdm(
        total=overall_total_rows,
        initial=overall_done_rows,
        desc="all tables",
        unit="rows",
        unit_scale=True,
        position=0,
    )

    for table in TABLES:
        name = table["name"]
        table_state = state["tables"][name]
        high_watermark = int(table_state["high_watermark_id"])
        last_success_id = int(table_state["last_success_id"])
        source_total_rows = int(table_state["source_total_rows"])
        print(f"[{name}] importing IDs {last_success_id + 1} through {high_watermark} ({source_total_rows:,} rows)")
        table_bar = tqdm(
            total=source_total_rows,
            initial=int(table_state["imported_rows"]),
            desc=name,
            unit="rows",
            unit_scale=True,
            leave=False,
            position=1,
        )

        while last_success_id < high_watermark:
            batch_started_at = time.perf_counter()
            batch_end = min(last_success_id + args.batch_id_span, high_watermark)
            where_clause = f"id > {last_success_id} AND id <= {batch_end}"
            source_rows = mysql_metadata.count_range(name, last_success_id, batch_end)
            if source_rows:
                ch.query(
                    f"INSERT INTO {args.clickhouse_database}.{name} ({table['insert_columns']}) "
                    f"SELECT {table['select_columns']} FROM {source_database}.{name} "
                    f"WHERE {where_clause}"
                )

            last_success_id = batch_end
            batch_seconds = time.perf_counter() - batch_started_at
            table_state["last_success_id"] = last_success_id
            table_state["imported_rows"] = int(table_state["imported_rows"]) + source_rows
            table_state["elapsed_seconds"] = float(table_state["elapsed_seconds"]) + batch_seconds
            table_state["last_batch_rows"] = source_rows
            table_state["last_batch_seconds"] = batch_seconds
            save_state(args.state_file, state)

            imported_rows = int(table_state["imported_rows"])
            table_progress = 100 if source_total_rows == 0 else imported_rows / source_total_rows * 100
            elapsed_seconds = float(table_state["elapsed_seconds"])
            rows_per_second = imported_rows / elapsed_seconds if elapsed_seconds else 0
            remaining_rows = max(0, source_total_rows - imported_rows)
            eta_seconds = remaining_rows / rows_per_second if rows_per_second else 0
            overall_done_rows = sum(int(state["tables"][item["name"]]["imported_rows"]) for item in TABLES)
            overall_progress = 100 if overall_total_rows == 0 else overall_done_rows / overall_total_rows * 100
            overall_elapsed_seconds = sum(float(state["tables"][item["name"]]["elapsed_seconds"]) for item in TABLES)
            overall_rows_per_second = overall_done_rows / overall_elapsed_seconds if overall_elapsed_seconds else 0
            overall_eta_seconds = (
                max(0, overall_total_rows - overall_done_rows) / overall_rows_per_second
                if overall_rows_per_second
                else 0
            )
            table_bar.update(source_rows)
            overall_bar.update(source_rows)
            table_bar.set_postfix(
                id=last_success_id,
                batch=f"{source_rows:,}/{batch_seconds:.1f}s",
                progress=f"{table_progress:.1f}%",
                eta=format_duration(eta_seconds),
            )
            overall_bar.set_postfix(
                progress=f"{overall_progress:.1f}%",
                eta=format_duration(overall_eta_seconds),
            )

        table_bar.close()
        table_state["completed"] = True
        save_state(args.state_file, state)

    overall_bar.close()
    print("\nImport complete. Source and target row counts:")
    for table in TABLES:
        name = table["name"]
        high_watermark = int(state["tables"][name]["high_watermark_id"])
        source_count = mysql_metadata.count_to_id(name, high_watermark)
        # ReplacingMergeTree may still contain old physical versions.  The
        # migration acceptance count must reflect the logical result users
        # read, so always count with FINAL.
        target_count = ch.scalar_int(f"SELECT count() FROM {args.clickhouse_database}.{name} FINAL")
        print(f"{name:<32} MySQL: {source_count:>12}  ClickHouse: {target_count:>12}")
    mysql_metadata.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except requests.RequestException as exc:
        print(f"ClickHouse request failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
    except Exception as exc:
        print(f"Migration failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
