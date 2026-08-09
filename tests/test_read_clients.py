import pytest

from coinx.read_clients import (
    ClickHouseReadClient,
    ReadOnlyQueryError,
    assert_read_only,
    parse_host_port,
)


def test_parse_host_port_accepts_host_and_host_port():
    assert parse_host_port("mysql", 3306) == ("mysql", 3306)
    assert parse_host_port("10.0.0.128:13306", 3306) == ("10.0.0.128", 13306)
    assert parse_host_port("[::1]:13306", 3306) == ("::1", 13306)


@pytest.mark.parametrize("sql", ["INSERT INTO t VALUES (1)", "UPDATE t SET a=1", "DELETE FROM t"])
def test_assert_read_only_rejects_mutations(sql):
    with pytest.raises(ReadOnlyQueryError):
        assert_read_only(sql)


def test_clickhouse_client_parses_json_each_row(monkeypatch):
    class Response:
        ok = True
        status_code = 200
        text = '{"symbol":"BTCUSDT","event_time":1}\n'

    class Session:
        def __init__(self):
            self.calls = []

        def post(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            return Response()

        def close(self):
            pass

    session = Session()
    client = ClickHouseReadClient(
        "http://clickhouse:8123",
        "coinx",
        "root",
        "secret",
        session=session,
    )
    assert client.query_rows("SELECT symbol, event_time FROM market_klines") == [
        {"symbol": "BTCUSDT", "event_time": 1}
    ]
    _, kwargs = session.calls[0]
    assert kwargs["auth"] == ("root", "secret")
    assert kwargs["params"]["database"] == "coinx"
    assert kwargs["params"]["query"].endswith("FORMAT JSONEachRow")


def test_clickhouse_client_compatibly_retries_old_mergetree_without_final():
    class Response:
        def __init__(self, ok, text):
            self.ok = ok
            self.status_code = 500 if not ok else 200
            self.text = text

    class Session:
        def __init__(self):
            self.calls = []

        def post(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            if len(self.calls) == 1:
                return Response(False, 'Code: 181. DB::Exception: Storage MergeTree does not support FINAL. (ILLEGAL_FINAL)')
            return Response(True, '{"symbol":"BTCUSDT"}\n')

        def close(self):
            pass

    session = Session()
    client = ClickHouseReadClient('http://clickhouse:8123', 'coinx', 'root', 'secret', session=session)
    assert client.query_rows('SELECT symbol FROM coinx.market_tickers FINAL') == [{'symbol': 'BTCUSDT'}]
    assert len(session.calls) == 2
    assert 'FINAL' not in session.calls[1][1]['params']['query']
