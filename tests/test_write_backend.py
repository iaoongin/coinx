from datetime import datetime
from decimal import Decimal

import pytest

from coinx import config
from coinx.read_clients import ReadOnlyQueryError
from coinx.repositories import market_tickers, series
from coinx import utils
from coinx.write_backend import (
    ClickHouseMarketWriteRepository,
    ClickHouseWriteClient,
    ClickHouseWriteError,
    MARKET_TABLE_COLUMNS,
    market_write_health,
)


class _Response:
    def __init__(self, ok=True, status_code=200, text=''):
        self.ok = ok
        self.status_code = status_code
        self.text = text


class _Session:
    def __init__(self, responses=None):
        self.responses = list(responses or [_Response()])
        self.calls = []

    def post(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.responses[min(len(self.calls) - 1, len(self.responses) - 1)]

    def close(self):
        pass


def test_clickhouse_write_client_sends_json_each_row_and_query_id():
    session = _Session()
    client = ClickHouseWriteClient(
        'http://clickhouse:8123', 'coinx', 'root', 'secret', session=session, retries=1,
    )
    assert client.insert_json_each_row(
        'market_klines',
        ('exchange', 'symbol', 'period', 'open_time'),
        [{'exchange': 'binance', 'symbol': 'BTCUSDT', 'period': '5m', 'open_time': 1}],
        query_id='test-query',
    ) == 1
    _, kwargs = session.calls[0]
    assert kwargs['params']['query_id'] == 'test-query'
    assert kwargs['params']['query'].endswith('FORMAT JSONEachRow')
    assert b'"symbol":"BTCUSDT"' in kwargs['data']


def test_write_repository_chunks_rows_and_serializes_decimal_json(monkeypatch):
    session = _Session()
    client = ClickHouseWriteClient(
        'http://clickhouse:8123', 'coinx', 'root', 'secret', session=session, retries=1,
    )
    monkeypatch.setattr(config, 'CLICKHOUSE_WRITE_BATCH_SIZE', 1)
    repository = ClickHouseMarketWriteRepository(client)

    assert repository.insert_rows(
        'market_snapshots',
        ('snapshot_time', 'symbol', 'batch_id', 'data_json', 'created_at'),
        [
            {'snapshot_time': 1, 'symbol': 'BTCUSDT', 'batch_id': 'b1',
             'data_json': {'price': Decimal('1.23000000')}, 'created_at': datetime(2026, 1, 1)},
            {'snapshot_time': 2, 'symbol': 'ETHUSDT', 'batch_id': 'b1',
             'data_json': {'price': Decimal('2.34000000')}, 'created_at': datetime(2026, 1, 1)},
        ],
        batch_id='snapshots-b1',
    ) == 2
    assert len(session.calls) == 2
    assert session.calls[0][1]['params']['query_id'].endswith('_0')
    assert b'"data_json":"{\\"price\\":\\"1.23000000\\"}"' in session.calls[0][1]['data']


def test_clickhouse_write_client_retries_then_raises():
    session = _Session([_Response(False, 500, 'boom'), _Response(False, 500, 'boom')])
    client = ClickHouseWriteClient(
        'http://clickhouse:8123', 'coinx', 'root', 'secret', session=session, retries=2,
    )
    with pytest.raises(ClickHouseWriteError):
        client.insert_json_each_row(
            'market_tickers', ('close_time', 'symbol'),
            [{'close_time': 1, 'symbol': 'BTCUSDT'}],
        )
    assert len(session.calls) == 2


def test_series_writer_does_not_open_mysql_session(monkeypatch):
    class Repo:
        def __init__(self):
            self.calls = []

        def insert_rows(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            return len(args[2])

    repo = Repo()
    monkeypatch.setattr(config, 'MARKET_WRITE_BACKEND', 'clickhouse')
    monkeypatch.setattr('coinx.write_backend.get_clickhouse_write_repository', lambda: repo)
    monkeypatch.setattr('coinx.database.get_session', lambda: (_ for _ in ()).throw(AssertionError('MySQL opened')))

    affected = series.upsert_series_records(
        'binance', 'klines', [{
            'symbol': 'BTCUSDT', 'period': '5m', 'open_time': 1, 'close_time': 2,
            'open_price': 1, 'high_price': 2, 'low_price': 1, 'close_price': 2,
        }],
    )
    assert affected == 1
    assert repo.calls[0][0][0] == 'market_klines'


def test_funding_series_writer_includes_clickhouse_version_column():
    columns, rows = series._clickhouse_series_rows(
        'binance', 'funding_rate', [{
            'symbol': 'BTCUSDT', 'period': '5m', 'event_time': 1,
            'funding_rate': '0.001',
        }],
    )
    assert 'updated_at' in columns
    assert rows[0]['updated_at'] is not None


def test_ticker_and_snapshot_writers_use_clickhouse(monkeypatch):
    class Repo:
        def __init__(self):
            self.tables = []

        def insert_rows(self, table, columns, rows, **kwargs):
            self.tables.append((table, columns, rows))
            return len(rows)

    repo = Repo()
    monkeypatch.setattr(config, 'MARKET_WRITE_BACKEND', 'clickhouse')
    monkeypatch.setattr('coinx.write_backend.get_clickhouse_write_repository', lambda: repo)
    monkeypatch.setattr(utils, 'cleanup_old_data', lambda: 0)
    assert market_tickers.save_market_tickers(
        [{'symbol': 'BTCUSDT', 'last_price': 1}], collect_time=123,
    ) == 1
    assert utils.save_all_coins_data([
        {'symbol': 'BTCUSDT', 'current': {'price': 1}},
    ]) == 1
    assert [table for table, _, _ in repo.tables] == ['market_tickers', 'market_snapshots']


def test_ticker_collection_propagates_clickhouse_write_failure(monkeypatch):
    from coinx.collector.binance import service

    monkeypatch.setattr(config, 'MARKET_WRITE_BACKEND', 'clickhouse')
    monkeypatch.setattr(service, 'get_exchange_info', lambda: [{'symbol': 'BTCUSDT'}])
    monkeypatch.setattr(service, 'get_all_24hr_tickers', lambda: [{
        'symbol': 'BTCUSDT', 'priceChange': '0', 'priceChangePercent': '0',
        'weightedAvgPrice': '1', 'lastPrice': '1', 'lastQty': '1',
        'openPrice': '1', 'highPrice': '1', 'lowPrice': '1', 'volume': '1',
        'quoteVolume': '1', 'openTime': 1, 'closeTime': 2,
        'firstId': 1, 'lastId': 1, 'count': 1,
    }])
    def fail(*_args, **_kwargs):
        raise ClickHouseWriteError('insert failed')
    monkeypatch.setattr('coinx.repositories.market_tickers.save_market_tickers', fail)

    with pytest.raises(ClickHouseWriteError, match='insert failed'):
        service.update_market_tickers()


def test_market_write_health_validates_engine_sorting_key_and_time_type(monkeypatch):
    monkeypatch.setattr(config, 'MARKET_WRITE_BACKEND', 'clickhouse')
    monkeypatch.setattr(config, 'CLICKHOUSE_URL', 'http://clickhouse:8123')

    class Client:
        def query_rows(self, sql):
            if sql.startswith('SELECT name, engine, sorting_key'):
                return [
                    {'name': table, 'engine': 'ReplacingMergeTree',
                     'sorting_key': ', '.join(schema_key)}
                    for table, schema_key in {
                        'market_klines': ('exchange', 'symbol', 'period', 'open_time'),
                        'market_open_interest_hist': ('exchange', 'symbol', 'period', 'event_time'),
                        'market_taker_buy_sell_vol': ('exchange', 'symbol', 'period', 'event_time'),
                        'market_funding_rate': ('exchange', 'symbol', 'period', 'event_time'),
                        'market_snapshots': ('symbol', 'snapshot_time', 'batch_id'),
                        'market_tickers': ('symbol', 'close_time'),
                    }.items()
                ]
            table = sql.rsplit(' ', 1)[-1].split('.', 1)[-1]
            columns = [
                {'name': name, 'type': "DateTime64(3, 'Asia/Shanghai')" if name in {'created_at', 'updated_at'} else 'String'}
                for name in MARKET_TABLE_COLUMNS[table]
            ]
            return columns

    class Repo:
        client = Client()

    monkeypatch.setattr('coinx.write_backend.get_clickhouse_write_repository', lambda: Repo())
    result = market_write_health()
    assert result['healthy'] is True

    bad = Repo()
    original = bad.client.query_rows
    def bad_query(sql):
        rows = original(sql)
        if sql.startswith('SELECT name, engine, sorting_key'):
            rows[-1] = {**rows[-1], 'engine': 'MergeTree'}
        return rows
    bad.client.query_rows = bad_query
    monkeypatch.setattr('coinx.write_backend.get_clickhouse_write_repository', lambda: bad)
    result = market_write_health()
    assert result['healthy'] is False
    assert 'market_tickers' in result['schema_errors']
