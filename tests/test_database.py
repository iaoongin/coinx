from sqlalchemy import MetaData, Table, Column, Integer

from coinx import config
from coinx.database import MARKET_TABLE_NAMES, tables_for_initialization


def _metadata_with_market_tables():
    metadata = MetaData()
    for name in ('coins', 'alert_rules', 'market_klines', 'market_tickers'):
        Table(name, metadata, Column('id', Integer))
    return metadata


def test_clickhouse_market_write_excludes_market_tables_from_mysql_init(monkeypatch):
    monkeypatch.setattr(config, 'MARKET_WRITE_BACKEND', 'clickhouse')

    names = {table.name for table in tables_for_initialization(_metadata_with_market_tables())}

    assert 'coins' in names
    assert 'alert_rules' in names
    assert 'market_klines' not in names
    assert 'market_tickers' not in names
    assert MARKET_TABLE_NAMES >= {'market_klines', 'market_tickers'}


def test_mysql_market_write_keeps_market_tables_in_init(monkeypatch):
    monkeypatch.setattr(config, 'MARKET_WRITE_BACKEND', 'mysql')

    names = {table.name for table in tables_for_initialization(_metadata_with_market_tables())}

    assert names == {'coins', 'alert_rules', 'market_klines', 'market_tickers'}
