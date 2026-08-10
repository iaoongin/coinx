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
    assert "argMax(high_price, updated_at) AS high_price" in sql
    assert "argMax(low_price, updated_at) AS low_price" in sql
    assert "FROM coinx.market_klines" in sql
    assert "FINAL" not in sql
    assert "max_bytes_before_external_group_by" in sql


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
    assert "argMax(quote_volume, updated_at) AS quote_volume" in sql
    assert "FINAL" not in sql


def test_latest_ticker_snapshot_filter_is_not_shadowed_by_argmax_alias():
    class TickerClient(FakeClickHouseClient):
        def query_scalar(self, sql):
            self.sql.append(sql)
            return 1_700_000_000_000

        def query_rows(self, sql):
            self.sql.append(sql)
            return []

    client = TickerClient()
    repository = ClickHouseMarketReadRepository(client, "coinx")

    repository._load_latest_tickers_uncached(
        rank_type="quote_volume",
        direction="up",
        limit=10,
        close_time=None,
        as_of_ms=None,
    )

    sql = client.sql[1]
    assert "WHERE mt.close_time = 1700000000000" in sql
    assert "GROUP BY mt.symbol" in sql


def test_clickhouse_price_volume_metrics_limits_and_deduplicates_kline_scan():
    client = FakeClickHouseClient()
    repository = ClickHouseMarketReadRepository(client, "coinx")

    # The fake only needs to capture SQL here; the result shape is not relevant
    # to this query-plan regression test.
    client.query_rows = lambda sql: client.sql.append(sql) or []
    assert repository.price_volume_metrics(scope_limit=10, as_of_ms=1_700_000_000_000) == {}

    sql = client.sql[0]
    assert "deduplicated_klines AS" in sql
    assert "argMax(k.open_price, k.updated_at) AS open_price" in sql
    assert "argMax(k.close_price, k.updated_at) AS close_price" in sql
    assert "FROM coinx.market_klines" in sql
    assert "FINAL" not in sql
    assert "max_bytes_before_external_group_by" in sql


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
