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
    assert "argMax(tupleElement(latest_row, 3), open_time)" in sql
    assert "GROUP BY symbol, bucket_time" in sql
    assert "HAVING count() = 12" in sql
    assert "max(open_time) - min(open_time) = 3300000" in sql
    assert "argMax(tuple(high_price, low_price, close_price, quote_volume), updated_at) AS latest_row" in sql
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
    assert "sum(ifNull(tupleElement(latest_row, 1), 0)) AS quote_volume_24h" in sql
    assert "GROUP BY symbol" in sql
    assert "argMax(tuple(quote_volume), updated_at) AS latest_row" in sql
    assert "FINAL" not in sql


def test_clickhouse_available_structure_symbols_is_server_side_and_cached():
    class SymbolClient(FakeClickHouseClient):
        def query_rows(self, sql):
            self.sql.append(sql)
            return [{'symbol': 'BTCUSDT'}, {'symbol': 'ETHUSDT'}]

    client = SymbolClient()
    repository = ClickHouseMarketReadRepository(client, "coinx")

    first = repository.available_market_structure_symbols(
        exchanges=['binance', 'okx'],
        upper_bound=1_700_000_000_000,
    )
    second = repository.available_market_structure_symbols(
        exchanges=['binance', 'okx'],
        upper_bound=1_700_000_000_000,
    )

    assert first == second == ['BTCUSDT', 'ETHUSDT']
    assert len(client.sql) == 1
    sql = client.sql[0]
    assert 'uniqExact(open_time) AS kline_points' in sql
    assert 'HAVING kline_points >= 60' in sql
    assert 'FROM coinx.market_klines' in sql
    assert 'FROM coinx.market_open_interest_hist' in sql
    assert 'PREWHERE exchange IN (\'binance\', \'okx\')' in sql
    assert 'max_threads = 2' in sql


def test_clickhouse_market_rows_deduplicates_without_final():
    client = FakeClickHouseClient()
    repository = ClickHouseMarketReadRepository(client, "coinx")

    repository.market_rows(
        "market_klines",
        "symbol, open_time, high_price, close_price, quote_volume",
        symbols=["BTCUSDT"],
        exchange="binance",
        period="5m",
        time_column="open_time",
        lower_bound=1_700_000_000_000,
        upper_bound=1_700_000_300_000,
        order_by="symbol, open_time",
        deduplicate=True,
    )

    sql = client.sql[0]
    assert "FINAL" not in sql
    assert "argMax(tuple(high_price, close_price, quote_volume), updated_at) AS _latest_row" in sql
    assert "GROUP BY exchange, symbol, period, open_time" in sql
    assert "PREWHERE symbol IN ('BTCUSDT')" in sql
    assert "max_threads = 2" in sql
    assert "max_bytes_before_external_group_by" in sql


def test_clickhouse_market_rows_keeps_final_for_detail_reads():
    client = FakeClickHouseClient()
    repository = ClickHouseMarketReadRepository(client, "coinx")

    repository.market_rows(
        "market_klines",
        "symbol, open_time, close_price",
        symbols=["BTCUSDT"],
        exchange="binance",
        period="5m",
        time_column="open_time",
        lower_bound=1_700_000_000_000,
        upper_bound=1_700_000_300_000,
    )

    assert "FROM coinx.market_klines FINAL" in client.sql[0]


def test_clickhouse_market_rows_can_deduplicate_key_only_reads():
    client = FakeClickHouseClient()
    repository = ClickHouseMarketReadRepository(client, "coinx")

    repository.market_rows(
        "market_taker_buy_sell_vol",
        "symbol, event_time",
        symbols=["BTCUSDT"],
        exchange="binance",
        period="5m",
        time_column="event_time",
        lower_bound=1_700_000_000_000,
        upper_bound=1_700_000_300_000,
        deduplicate=True,
    )

    sql = client.sql[0]
    assert "FINAL" not in sql
    assert "SELECT exchange, symbol, period, event_time FROM coinx.market_taker_buy_sell_vol" in sql
    assert "GROUP BY exchange, symbol, period, event_time" in sql


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
    assert "argMax(tuple(k.open_price, k.close_price, k.quote_volume), k.updated_at) AS latest_row" in sql
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


def test_market_structure_clickhouse_aggregation_batches_symbols_and_merges_results(monkeypatch):
    class Repository:
        def __init__(self):
            self.aggregate_calls = []

        def aggregate_kline_rows(self, **kwargs):
            self.aggregate_calls.append(kwargs)
            return [
                {
                    "symbol": symbol,
                    "open_time": 1_700_000_000_000,
                    "high_price": "105",
                    "low_price": "95",
                    "close_price": "102",
                    "quote_volume": "1200",
                }
                for symbol in kwargs["symbols"]
            ]

    repository = Repository()
    monkeypatch.setattr(market_structure_series, "is_clickhouse_read", lambda: True)
    monkeypatch.setattr(market_structure_series, "get_clickhouse_repository", lambda: repository)
    monkeypatch.setattr(market_structure_series, "CLICKHOUSE_AGGREGATION_SYMBOL_BATCH_SIZE", 2)

    result = market_structure_series.load_market_structure_aggregated_kline_maps(
        session=None,
        exchange="binance",
        symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT"],
        upper_bound=1_700_000_000_000,
        intervals={"1h": 60 * 60 * 1000},
        lookback_points=72,
    )

    assert [call["symbols"] for call in repository.aggregate_calls] == [
        ["BTCUSDT", "ETHUSDT"],
        ["SOLUSDT", "XRPUSDT"],
        ["ADAUSDT"],
    ]
    assert sorted(result["1h"]) == ["ADAUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
