from types import SimpleNamespace

from coinx import read_backend


def test_clickhouse_health_runs_read_only_probe(monkeypatch):
    calls = []

    class Client:
        def query_scalar(self, sql):
            calls.append(sql)
            return 1

    monkeypatch.setattr(read_backend, 'get_read_backend', lambda: 'clickhouse')
    monkeypatch.setattr(read_backend.config, 'CLICKHOUSE_URL', 'http://clickhouse:8123')
    monkeypatch.setattr(read_backend, 'get_clickhouse_repository', lambda: SimpleNamespace(client=Client()))

    result = read_backend.read_backend_health()

    assert result['healthy'] is True
    assert result['probe'] == 1
    assert calls == ['SELECT 1']


def test_health_reports_backend_failure(monkeypatch):
    monkeypatch.setattr(read_backend, 'get_read_backend', lambda: 'clickhouse')
    monkeypatch.setattr(read_backend.config, 'CLICKHOUSE_URL', 'http://clickhouse:8123')

    class Client:
        def query_scalar(self, _sql):
            raise RuntimeError('connection refused')

    monkeypatch.setattr(read_backend, 'get_clickhouse_repository', lambda: SimpleNamespace(client=Client()))

    result = read_backend.read_backend_health()

    assert result['healthy'] is False
    assert 'connection refused' in result['error']
