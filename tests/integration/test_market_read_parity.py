"""Opt-in MySQL/ClickHouse read parity tests.

Set the COINX_*_TEST_* environment variables before running this module. The
tests are skipped by default and never issue mutating SQL.
"""

import os

import pytest

from coinx.read_clients import ClickHouseReadClient, MySQLReadClient
from coinx.repositories.market_read import ClickHouseMarketReadRepository, MySQLMarketReadRepository


def _required_env():
    names = (
        "COINX_CK_TEST_URL",
        "COINX_CK_TEST_DATABASE",
        "COINX_CK_TEST_USER",
        "COINX_CK_TEST_PASSWORD",
        "COINX_MYSQL_TEST_HOST",
        "COINX_MYSQL_TEST_DATABASE",
        "COINX_MYSQL_TEST_USER",
        "COINX_MYSQL_TEST_PASSWORD",
    )
    return names if all(os.getenv(name) is not None for name in names) else None


pytestmark = pytest.mark.skipif(
    _required_env() is None,
    reason="set COINX_*_TEST_* environment variables to run database parity tests",
)


@pytest.fixture()
def repositories():
    with ClickHouseReadClient(
        os.environ["COINX_CK_TEST_URL"],
        os.environ["COINX_CK_TEST_DATABASE"],
        os.environ["COINX_CK_TEST_USER"],
        os.environ["COINX_CK_TEST_PASSWORD"],
    ) as ck_client, MySQLReadClient(
        os.environ["COINX_MYSQL_TEST_HOST"],
        os.environ["COINX_MYSQL_TEST_DATABASE"],
        os.environ["COINX_MYSQL_TEST_USER"],
        os.environ["COINX_MYSQL_TEST_PASSWORD"],
    ) as mysql_client:
        yield (
            MySQLMarketReadRepository(mysql_client),
            ClickHouseMarketReadRepository(ck_client, os.environ["COINX_CK_TEST_DATABASE"]),
        )


def test_market_table_counts_match(repositories):
    mysql, ck = repositories
    for table in (
        "market_klines",
        "market_open_interest_hist",
        "market_taker_buy_sell_vol",
        "market_funding_rate",
        "market_snapshots",
        "market_tickers",
    ):
        assert mysql.table_count(table) == ck.table_count(table), table


def test_latest_funding_rates_match(repositories):
    mysql, ck = repositories
    assert mysql.latest_funding_rates(["BTCUSDT", "ETHUSDT"]) == ck.latest_funding_rates(
        ["BTCUSDT", "ETHUSDT"]
    )


def test_latest_tickers_have_same_symbols_and_window(repositories):
    mysql, ck = repositories
    mysql_rows = mysql.latest_tickers(limit=20)
    ck_rows = ck.latest_tickers(limit=20)
    assert [row["symbol"] for row in mysql_rows] == [row["symbol"] for row in ck_rows]
    assert {row["close_time"] for row in mysql_rows} == {row["close_time"] for row in ck_rows}
