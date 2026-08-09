from coinx.repositories.market_read import ClickHouseMarketReadRepository
from coinx.repositories import market_structure_series


class FakeClickHouseClient:
    url = "http://clickhouse.test:8123"
    auth = ("root", "secret")

    def __init__(self):
        self.sql = []

    def query_rows(self, sql):
        self.sql.append(sql)
        if "quote_volume_24h" in sql:
            return [{"symbol": "BTCUSDT", "quote_volume_24h": "1200"}]
        return [{
            "symbol": "BTCUSDT",
            "bucket_time": 1_700_000_000_000,
            "high_price": "105",
            "low_price": "95",
            "close_price": "102",
            "quote_volume": "1200",
        }]


def test_clickhouse_aggregation_returns_buckets_and_validates_complete_points():
    client = FakeClickHouseClient()
    repository = ClickHouseMarketReadRepository(client, "coinx")

    rows = repository.aggregate_kline_rows(
        symbols=["BTCUSDT"],
        exchange="binance",
        interval_ms=60 * 60 * 1000,
        lower_bound=1_699_000_000_000,
        upper_bound=1_700_000_000_000,
    )

    assert rows == [{
        "symbol": "BTCUSDT",
        "open_time": 1_700_000_000_000,
        "high_price": "105",
        "low_price": "95",
        "close_price": "102",
        "quote_volume": "1200",
    }]
    sql = client.sql[0]
    assert "argMax(close_price, open_time)" in sql
    assert "GROUP BY symbol, bucket_time" in sql
    assert "HAVING count() = 12" in sql
    assert "max(open_time) - min(open_time) = 3300000" in sql
    assert "FROM coinx.market_klines FINAL" in sql


def test_clickhouse_quote_volume_is_aggregated_server_side():
    client = FakeClickHouseClient()
    repository = ClickHouseMarketReadRepository(client, "coinx")

    result = repository.kline_quote_volume_by_symbol(
        symbols=["BTCUSDT"],
        exchange="binance",
        lower_bound=1_699_000_000_000,
        upper_bound=1_700_000_000_000,
    )

    assert result == {"BTCUSDT": 1200.0}
    sql = client.sql[0]
    assert "sum(ifNull(quote_volume, 0)) AS quote_volume_24h" in sql
    assert "GROUP BY symbol" in sql


def test_market_structure_clickhouse_path_uses_server_side_aggregation(monkeypatch):
    class Repository:
        def __init__(self):
            self.aggregate_calls = []

        def aggregate_kline_rows(self, **kwargs):
            self.aggregate_calls.append(kwargs)
            return [{
                "symbol": "BTCUSDT",
                "open_time": 1_700_000_000_000,
                "high_price": "105",
                "low_price": "95",
                "close_price": "102",
                "quote_volume": "1200",
            }]

        def market_rows(self, *args, **kwargs):
            raise AssertionError("raw ClickHouse K-lines must not be loaded for aggregation")

    repository = Repository()
    monkeypatch.setattr(market_structure_series, "is_clickhouse_read", lambda: True)
    monkeypatch.setattr(market_structure_series, "get_clickhouse_repository", lambda: repository)

    result = market_structure_series.load_market_structure_aggregated_kline_maps(
        session=None,
        exchange="binance",
        symbols=["BTCUSDT"],
        upper_bound=1_700_000_000_000,
        intervals={"1h": 60 * 60 * 1000},
        lookback_points=72,
    )

    assert result["1h"]["BTCUSDT"][1_700_000_000_000].close_price == 102.0
    assert repository.aggregate_calls[0]["interval_ms"] == 60 * 60 * 1000
