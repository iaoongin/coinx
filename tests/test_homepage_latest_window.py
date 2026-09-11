"""Exercise homepage discovery bounds against rows, including historical replay."""

import sqlite3

import pytest

from coinx.repositories import homepage_series as homepage
from coinx.repositories.market_read import ClickHouseMarketReadRepository


@pytest.fixture
def latest_repository():
    # These MAX/GROUP BY queries are standard SQL. SQLite checks the actual
    # predicates against data; only ClickHouse execution SETTINGS are removed.
    connection = sqlite3.connect(':memory:')
    connection.row_factory = sqlite3.Row
    tables = {
        'market_open_interest_hist': 'event_time',
        'market_klines': 'open_time',
        'market_taker_buy_sell_vol': 'event_time',
    }
    for table, column in tables.items():
        connection.execute(
            f'CREATE TABLE {table} (symbol TEXT, exchange TEXT, period TEXT, {column} INTEGER)'
        )

    class Client:
        def query_rows(self, sql):
            return [dict(row) for row in connection.execute(sql.split(' SETTINGS ')[0])]

    repository = ClickHouseMarketReadRepository(Client(), 'main')

    def seed(rows):
        for table in tables:
            connection.executemany(f'INSERT INTO {table} VALUES (?, ?, ?, ?)', rows)

    yield repository, seed
    connection.close()


@pytest.mark.parametrize('window', ['8h', '10d'])
@pytest.mark.parametrize('replay', [False, True])
@pytest.mark.parametrize('loader', ['candidate', 'details'])
def test_latest_discovery_respects_configured_window_and_replay(
    monkeypatch, latest_repository, window, replay, loader,
):
    repository, seed = latest_repository
    wall_clock = 1_800_000_000_000
    cutoff = wall_clock - 60 * 24 * 60 * 60 * 1000 if replay else wall_clock
    horizon = homepage._interval_to_ms(window)
    lower = cutoff - horizon
    monkeypatch.setattr(homepage, 'MAX_TIME_INTERVAL_MS', horizon)
    monkeypatch.setattr(homepage.time, 'time', lambda: wall_clock / 1000)
    monkeypatch.setattr(homepage, 'get_clickhouse_repository', lambda: repository)
    monkeypatch.setattr(homepage, '_has_unreliable_taker_source', lambda exchange: False)
    # Detail calculations are covered in test_homepage_series_repository;
    # exercise both entry points that discover anchors here.
    monkeypatch.setattr(repository, 'market_rows', lambda *args, **kwargs: [])
    seed([
        ('ACTIVE', 'binance', '5m', cutoff - homepage.FIVE_MINUTES_MS),
        ('ACTIVE', 'binance', '5m', cutoff),
        ('ACTIVE', 'binance', '5m', cutoff + 1),
        ('BOUNDARY', 'binance', '5m', lower),
        ('STALE', 'binance', '5m', lower - 1),
        ('FUTURE', 'binance', '5m', cutoff + 1),
        ('OTHER_EXCHANGE', 'okx', '5m', cutoff),
        ('OTHER_PERIOD', 'binance', '1h', cutoff),
    ])
    symbols = ['ACTIVE', 'BOUNDARY', 'STALE', 'FUTURE', 'OTHER_EXCHANGE', 'OTHER_PERIOD']
    kwargs = {'upper_bound': cutoff} if replay else {}
    if loader == 'candidate':
        result = homepage._load_clickhouse_exchange_candidate_latest('binance', symbols, **kwargs)
    else:
        result = homepage._load_homepage_exchange_maps_clickhouse('binance', symbols, **kwargs)[3]

    assert result == {'ACTIVE': cutoff, 'BOUNDARY': lower}


def test_stale_only_series_does_not_fall_back_to_full_history(monkeypatch, latest_repository):
    repository, seed = latest_repository
    cutoff = 1_700_000_000_000
    seed([('STALE', 'binance', '5m', cutoff - homepage.MAX_TIME_INTERVAL_MS - 1)])
    monkeypatch.setattr(homepage, 'get_clickhouse_repository', lambda: repository)
    monkeypatch.setattr(homepage, '_has_unreliable_taker_source', lambda exchange: False)

    def unexpected_details(*args, **kwargs):
        pytest.fail('No recent anchor: must not read historical detail rows')

    monkeypatch.setattr(repository, 'market_rows', unexpected_details)
    assert homepage._load_clickhouse_exchange_candidate_latest(
        'binance', ['STALE'], upper_bound=cutoff,
    ) == {}
    oi, klines, _net, latest = homepage._load_homepage_exchange_maps_clickhouse(
        'binance', ['STALE'], upper_bound=cutoff,
    )
    assert oi == klines == {'STALE': {}}
    assert latest == {}
