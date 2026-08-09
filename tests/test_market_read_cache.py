"""Process-level ClickHouse read-cache isolation and concurrency tests."""

from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import time

from coinx.repositories.market_read import (
    ClickHouseMarketReadRepository,
    clear_market_read_cache,
)


class FakeClickHouseClient:
    url = "http://clickhouse.test:8123"
    auth = ("root", "secret")

    def __init__(self, *, delay=0.0):
        self.delay = delay
        self.calls = []
        self._calls_lock = Lock()

    def query_scalar(self, sql):
        with self._calls_lock:
            self.calls.append(("scalar", sql))
        return 1_000

    def query_rows(self, sql):
        if self.delay:
            time.sleep(self.delay)
        with self._calls_lock:
            self.calls.append(("rows", sql))
        if "market_funding_rate" in sql:
            return [{
                "symbol": "BTCUSDT",
                "event_time": 1_000,
                "funding_rate": 0.001,
                "predicted_rate": None,
                "next_funding_time": 2_000,
                "mark_price": 100.0,
            }]
        return [{
            "symbol": "BTCUSDT",
            "price_change": 1.0,
            "price_change_percent": 2.0,
            "weighted_avg_price": 100.0,
            "last_price": 101.0,
            "last_qty": 1.0,
            "open_price": 99.0,
            "high_price": 102.0,
            "low_price": 98.0,
            "volume": 10.0,
            "quote_volume": 1_000.0,
            "open_time": 0,
            "close_time": 1_000,
            "first_id": 1,
            "last_id": 2,
            "count": 2,
        }]


def setup_function():
    clear_market_read_cache()


def teardown_function():
    clear_market_read_cache()


def test_funding_cache_is_shared_by_repositories_for_same_endpoint():
    first_client = FakeClickHouseClient()
    second_client = FakeClickHouseClient()
    first = ClickHouseMarketReadRepository(first_client, "coinx")
    second = ClickHouseMarketReadRepository(second_client, "coinx")

    first_result = first.latest_funding_rates(["BTCUSDT"], as_of_ms=1_000)
    second_result = second.latest_funding_rates(["BTCUSDT"], as_of_ms=1_000)

    assert first_result == second_result
    assert len(first_client.calls) == 1
    assert second_client.calls == []
    first_result["BTCUSDT"]["funding_rate"] = 99.0
    assert second.latest_funding_rates(["BTCUSDT"], as_of_ms=1_000)["BTCUSDT"]["funding_rate"] == 0.001


def test_ticker_cache_is_shared_and_as_of_is_part_of_key():
    first_client = FakeClickHouseClient()
    second_client = FakeClickHouseClient()
    first = ClickHouseMarketReadRepository(first_client, "coinx")
    second = ClickHouseMarketReadRepository(second_client, "coinx")

    first.latest_tickers(limit=1, as_of_ms=1_000)
    second.latest_tickers(limit=1, as_of_ms=1_000)
    second.latest_tickers(limit=1, as_of_ms=900)

    assert len(first_client.calls) == 2
    assert len(second_client.calls) == 2


def test_concurrent_funding_requests_fill_cache_once():
    clients = [FakeClickHouseClient(delay=0.05), FakeClickHouseClient(delay=0.05)]
    repositories = [ClickHouseMarketReadRepository(client, "coinx") for client in clients]

    def load(repo):
        return repo.latest_funding_rates(["BTCUSDT"], as_of_ms=1_000)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(load, repositories))

    assert results[0] == results[1]
    assert sum(len(client.calls) for client in clients) == 1
