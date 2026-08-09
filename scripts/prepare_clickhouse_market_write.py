#!/usr/bin/env python3
"""Prepare the two append-only market tables for idempotent CK writes.

The script creates staging tables, copies the current data, and atomically
renames the old tables aside.  Run it while collection is stopped:

    python scripts/prepare_clickhouse_market_write.py \
        --clickhouse-url http://10.0.0.128:8123 \
        --clickhouse-database coinx --clickhouse-user root --clickhouse-password root
"""

from __future__ import annotations

import argparse
import re
import time

import requests


IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ClickHouse:
    def __init__(self, args):
        self.url = args.clickhouse_url.rstrip('/')
        self.database = args.clickhouse_database
        self.auth = (args.clickhouse_user, args.clickhouse_password)
        self.timeout = (10, args.timeout)

    def execute(self, sql: str) -> str:
        response = requests.post(
            self.url,
            params={'query': sql, 'database': self.database, 'wait_end_of_query': 1},
            auth=self.auth,
            timeout=self.timeout,
        )
        if not response.ok:
            raise RuntimeError(response.text.strip() or f'HTTP {response.status_code}')
        return response.text


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--clickhouse-url', default='http://127.0.0.1:8123')
    parser.add_argument('--clickhouse-database', default='coinx')
    parser.add_argument('--clickhouse-user', default='default')
    parser.add_argument('--clickhouse-password', default='')
    parser.add_argument('--timeout', type=int, default=3600)
    parser.add_argument('--legacy-suffix', default=None)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    if not IDENTIFIER.fullmatch(args.clickhouse_database):
        parser.error('invalid ClickHouse database identifier')
    return args


def statements(database: str, legacy_suffix: str):
    ticker = f'{database}.market_tickers'
    snapshot = f'{database}.market_snapshots'
    ticker_v2 = f'{database}.market_tickers_v2'
    snapshot_v2 = f'{database}.market_snapshots_v2'
    ticker_legacy = f'{database}.market_tickers_{legacy_suffix}'
    snapshot_legacy = f'{database}.market_snapshots_{legacy_suffix}'
    return [
        # The staging names are never production tables.  Truncating them
        # makes a previously interrupted preparation safe to resume without
        # duplicating the backfill.
        f"DROP TABLE IF EXISTS {ticker_v2}",
        f"DROP TABLE IF EXISTS {snapshot_v2}",
        f"CREATE TABLE {ticker_v2} ("
        "close_time UInt64, symbol LowCardinality(String), "
        "price_change Nullable(Decimal(24,8)), price_change_percent Nullable(Decimal(20,8)), "
        "weighted_avg_price Nullable(Decimal(24,8)), last_price Nullable(Decimal(24,8)), "
        "last_qty Nullable(Decimal(24,8)), open_price Nullable(Decimal(24,8)), "
        "high_price Nullable(Decimal(24,8)), low_price Nullable(Decimal(24,8)), "
        "volume Nullable(Decimal(30,8)), quote_volume Nullable(Decimal(30,8)), "
        "open_time Nullable(UInt64), first_id Nullable(UInt64), last_id Nullable(UInt64), "
        "count Nullable(UInt64), created_at DateTime64(3,'Asia/Shanghai') DEFAULT now64(3), "
        "updated_at DateTime64(3,'Asia/Shanghai') DEFAULT now64(3)"
        ") ENGINE=ReplacingMergeTree(updated_at) "
        "PARTITION BY toYYYYMM(toDateTime(intDiv(close_time,1000),'Asia/Shanghai')) "
        "ORDER BY (symbol, close_time)",
        f"CREATE TABLE {snapshot_v2} ("
        "snapshot_time UInt64, symbol LowCardinality(String), batch_id String, "
        "price Nullable(Decimal(24,8)), open_interest Nullable(Decimal(24,8)), "
        "open_interest_value Nullable(Decimal(24,8)), data_json String DEFAULT '{}', "
        "created_at DateTime64(3,'Asia/Shanghai') DEFAULT now64(3)"
        ") ENGINE=ReplacingMergeTree(created_at) "
        "PARTITION BY toYYYYMM(toDateTime(intDiv(snapshot_time,1000),'Asia/Shanghai')) "
        "ORDER BY (symbol, snapshot_time, batch_id)",
        f"INSERT INTO {ticker_v2} (close_time,symbol,price_change,price_change_percent,"
        "weighted_avg_price,last_price,last_qty,open_price,high_price,low_price,volume,"
        "quote_volume,open_time,first_id,last_id,count,created_at,updated_at) "
        f"SELECT close_time,symbol,price_change,price_change_percent,weighted_avg_price,"
        f"last_price,last_qty,open_price,high_price,low_price,volume,quote_volume,open_time,"
        f"first_id,last_id,count,created_at,created_at FROM {ticker}",
        f"INSERT INTO {snapshot_v2} (snapshot_time,symbol,batch_id,price,open_interest,"
        f"open_interest_value,data_json,created_at) SELECT snapshot_time,symbol,batch_id,"
        f"price,open_interest,open_interest_value,ifNull(toString(data_json),'{{}}'),created_at FROM {snapshot}",
        # Keep the two production table swaps in one RENAME statement so a
        # failure cannot leave only one of the two tables on the new schema.
        f"RENAME TABLE {ticker} TO {ticker_legacy}, {ticker_v2} TO {ticker}, "
        f"{snapshot} TO {snapshot_legacy}, {snapshot_v2} TO {snapshot}",
    ]


def main() -> int:
    args = parse_args()
    suffix = args.legacy_suffix or time.strftime('legacy_%Y%m%d%H%M%S')
    statements_to_run = statements(args.clickhouse_database, suffix)
    for sql in statements_to_run:
        print(sql)
        if not args.dry_run:
            ClickHouse(args).execute(sql)
    print(f'ClickHouse market write schema prepared; legacy suffix={suffix}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
