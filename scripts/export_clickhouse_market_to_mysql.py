#!/usr/bin/env python3
"""Emergency recent-window CK -> MySQL market-data compensation.

This is intentionally dry-run unless ``--apply`` is supplied.  It exists for
the rollback path after the application has been switched to CK-only writes.
"""

from __future__ import annotations

import argparse
import json
import re
from typing import Any

import pymysql
import requests


TABLES = {
    'market_klines': (
        'open_time',
        ('exchange', 'symbol', 'period', 'open_time', 'close_time', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'quote_volume', 'trade_count', 'taker_buy_base_volume', 'taker_buy_quote_volume', 'created_at', 'updated_at'),
    ),
    'market_open_interest_hist': (
        'event_time',
        ('exchange', 'symbol', 'period', 'event_time', 'sum_open_interest', 'sum_open_interest_value', 'created_at', 'updated_at'),
    ),
    'market_taker_buy_sell_vol': (
        'event_time',
        ('exchange', 'symbol', 'period', 'event_time', 'buy_sell_ratio', 'buy_vol', 'sell_vol', 'created_at', 'updated_at'),
    ),
    'market_funding_rate': (
        'event_time',
        ('exchange', 'symbol', 'period', 'event_time', 'funding_rate', 'predicted_rate', 'next_funding_time', 'mark_price', 'created_at', 'updated_at'),
    ),
    'market_snapshots': (
        'snapshot_time',
        ('snapshot_time', 'symbol', 'batch_id', 'price', 'open_interest', 'open_interest_value', 'data_json', 'created_at'),
    ),
    'market_tickers': (
        'close_time',
        ('close_time', 'symbol', 'price_change', 'price_change_percent', 'weighted_avg_price', 'last_price', 'last_qty', 'open_price', 'high_price', 'low_price', 'volume', 'quote_volume', 'open_time', 'first_id', 'last_id', 'count', 'created_at'),
    ),
}
UNIQUE_KEYS = {
    'market_klines': ('exchange', 'symbol', 'period', 'open_time'),
    'market_open_interest_hist': ('exchange', 'symbol', 'period', 'event_time'),
    'market_taker_buy_sell_vol': ('exchange', 'symbol', 'period', 'event_time'),
    'market_funding_rate': ('symbol', 'period', 'event_time'),
}
IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def parse_host_port(value: str):
    if value.count(':') == 1:
        host, port = value.rsplit(':', 1)
        if port.isdigit():
            return host, int(port)
    return value, 3306


def args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--clickhouse-url', required=True)
    parser.add_argument('--clickhouse-database', default='coinx')
    parser.add_argument('--clickhouse-user', default='default')
    parser.add_argument('--clickhouse-password', default='')
    parser.add_argument('--mysql-host', required=True)
    parser.add_argument('--mysql-database', default='coinx')
    parser.add_argument('--mysql-user', required=True)
    parser.add_argument('--mysql-password', required=True)
    parser.add_argument('--since-ms', type=int, required=True)
    parser.add_argument('--until-ms', type=int)
    parser.add_argument('--tables', default=','.join(TABLES))
    parser.add_argument('--apply', action='store_true')
    parsed = parser.parse_args()
    for value in (parsed.clickhouse_database, parsed.mysql_database):
        if not IDENTIFIER.fullmatch(value):
            parser.error(f'invalid database identifier: {value}')
    return parsed


def fetch_rows(options, table, time_column):
    columns = ', '.join(TABLES[table][1])
    where = f'{time_column} >= {int(options.since_ms)}'
    if options.until_ms is not None:
        where += f' AND {time_column} <= {int(options.until_ms)}'
    sql = f'SELECT {columns} FROM {options.clickhouse_database}.{table} FINAL WHERE {where} ORDER BY {time_column}'
    response = requests.post(
        options.clickhouse_url.rstrip('/'),
        params={'query': f'{sql} FORMAT JSONEachRow', 'database': options.clickhouse_database},
        auth=(options.clickhouse_user, options.clickhouse_password),
        timeout=(10, 3600),
    )
    if not response.ok:
        raise RuntimeError(response.text.strip() or f'ClickHouse HTTP {response.status_code}')
    return [json.loads(line) for line in response.text.splitlines() if line.strip()]


def write_rows(connection, table: str, rows: list[dict[str, Any]], columns: tuple[str, ...], apply: bool):
    if not rows:
        return 0
    placeholders = ', '.join(['%s'] * len(columns))
    column_sql = ', '.join(f'`{column}`' for column in columns)
    sql = f'INSERT INTO `{table}` ({column_sql}) VALUES ({placeholders})'
    if table in UNIQUE_KEYS:
        key_columns = set(UNIQUE_KEYS[table])
        updates = [
            f'`{column}` = VALUES(`{column}`)'
            for column in columns
            if column not in key_columns and column != 'created_at'
        ]
        if updates:
            sql += ' ON DUPLICATE KEY UPDATE ' + ', '.join(updates)
    # The legacy ticker/snapshot tables have no unique key.  Replace the
    # compensated time window explicitly before inserting it.
    if apply and table in {'market_tickers', 'market_snapshots'}:
        time_column = TABLES[table][0]
        low = min(int(row[time_column]) for row in rows)
        high = max(int(row[time_column]) for row in rows)
        with connection.cursor() as cursor:
            cursor.execute(f'DELETE FROM `{table}` WHERE `{time_column}` BETWEEN %s AND %s', (low, high))
    if apply:
        values = [tuple(row.get(column) for column in columns) for row in rows]
        with connection.cursor() as cursor:
            for start in range(0, len(values), 1000):
                cursor.executemany(sql, values[start:start + 1000])
        connection.commit()
    return len(rows)


def main() -> int:
    options = args()
    selected = [table.strip() for table in options.tables.split(',') if table.strip()]
    unknown = set(selected) - set(TABLES)
    if unknown:
        raise SystemExit(f'unsupported tables: {sorted(unknown)}')
    host, port = parse_host_port(options.mysql_host)
    connection = pymysql.connect(
        host=host,
        port=port,
        user=options.mysql_user,
        password=options.mysql_password,
        database=options.mysql_database,
        charset='utf8mb4',
        autocommit=False,
    )
    try:
        total = 0
        for table in selected:
            time_column, columns = TABLES[table]
            rows = fetch_rows(options, table, time_column)
            count = write_rows(connection, table, rows, columns, options.apply)
            total += count
            print(f'{table}: {count:,} rows ' + ('applied' if options.apply else 'would apply'))
        print(f'total: {total:,} rows')
    finally:
        connection.close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
