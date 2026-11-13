from types import SimpleNamespace

import pytest

from coinx.repositories import retention_optimize


class FakeResult:
    def __init__(self, value):
        self.value = value

    def first(self):
        return self.value

    def scalar(self):
        return self.value

    def mappings(self):
        return self

    def all(self):
        return self.value


class FakeConnection:
    def __init__(self, stats, optimize_rows=None):
        self.stats = stats
        self.optimize_rows = optimize_rows or [{'Msg_type': 'status', 'Msg_text': 'OK'}]
        self.optimized = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def invalidate(self):
        pass

    def execute(self, statement, params=None):
        if params:
            if 'INNODB_TABLES' in str(statement):
                return FakeResult('Single')
            return FakeResult(self.stats[params['table_name']])
        return FakeResult(None)

    def exec_driver_sql(self, statement):
        self.optimized.append(statement)
        return FakeResult(self.optimize_rows)


def setup_fake_mysql(monkeypatch, stats, optimize_rows=None):
    connection = FakeConnection(stats, optimize_rows=optimize_rows)
    dialect = SimpleNamespace(
        name='mysql',
        identifier_preparer=SimpleNamespace(quote=lambda name: f'`{name}`'),
    )
    bind = SimpleNamespace(dialect=dialect, connect=lambda: connection)
    monkeypatch.setattr(retention_optimize, 'get_session', lambda: SimpleNamespace(
        get_bind=lambda: bind, close=lambda: None,
    ))
    monkeypatch.setattr(retention_optimize.config, 'DATA_CLEANUP_OPTIMIZE_FREE_PERCENT', 10)
    monkeypatch.setattr(retention_optimize.config, 'DATA_CLEANUP_OPTIMIZE_RESERVE_GB', 0)
    return connection


def test_only_tables_above_threshold_are_optimized(monkeypatch):
    connection = setup_fake_mysql(monkeypatch, {
        'market_klines': (800, 100, 100),
        'market_funding_rate': (800, 100, 101),
    })
    monkeypatch.setattr(retention_optimize, '_disk_paths', lambda _connection: ('data', 'tmp'))
    monkeypatch.setattr(retention_optimize.shutil, 'disk_usage', lambda _path: SimpleNamespace(free=10000))

    results = retention_optimize.optimize_cleaned_tables(['market_klines', 'market_funding_rate'])

    assert [result['status'] for result in results] == ['skipped', 'success']
    assert connection.optimized == ['OPTIMIZE TABLE `market_funding_rate`']


def test_missing_disk_mapping_skips_optimization(monkeypatch):
    connection = setup_fake_mysql(monkeypatch, {'market_klines': (800, 100, 200)})
    monkeypatch.setattr(retention_optimize.config, 'DATA_CLEANUP_OPTIMIZE_DATADIR_PATH', '')
    monkeypatch.setattr(retention_optimize.config, 'DATA_CLEANUP_OPTIMIZE_TMPDIR_PATH', '')

    result = retention_optimize.optimize_cleaned_tables(['market_klines'])[0]

    assert result['status'] == 'skipped'
    assert '未配置' in result['reason']
    assert connection.optimized == []


def test_insufficient_disk_skips_optimization(monkeypatch):
    connection = setup_fake_mysql(monkeypatch, {'market_klines': (800, 100, 200)})
    monkeypatch.setattr(retention_optimize, '_disk_paths', lambda _connection: ('data', 'tmp'))
    monkeypatch.setattr(retention_optimize.shutil, 'disk_usage', lambda _path: SimpleNamespace(free=1000))

    result = retention_optimize.optimize_cleaned_tables(['market_klines'])[0]

    assert result['status'] == 'skipped'
    assert '不足' in result['reason']
    assert connection.optimized == []


def test_mysql_error_does_not_report_success(monkeypatch):
    connection = setup_fake_mysql(monkeypatch, {'market_klines': (800, 100, 200)}, [
        {'Msg_type': 'error', 'Msg_text': 'disk full'},
    ])
    monkeypatch.setattr(retention_optimize, '_disk_paths', lambda _connection: ('data', 'tmp'))
    monkeypatch.setattr(retention_optimize.shutil, 'disk_usage', lambda _path: SimpleNamespace(free=10000))

    result = retention_optimize.optimize_cleaned_tables(['market_klines'])[0]

    assert result['status'] == 'error'
    assert 'disk full' in result['reason']
    assert len(connection.optimized) == 1


def test_data_directory_must_match_mysql_instance(monkeypatch, tmp_path):
    (tmp_path / 'auto.cnf').write_text('[auto]\nserver-uuid=first-instance\n', encoding='utf-8')
    monkeypatch.setattr(retention_optimize.config, 'DATA_CLEANUP_OPTIMIZE_DATADIR_PATH', str(tmp_path))
    monkeypatch.setattr(retention_optimize.config, 'DATA_CLEANUP_OPTIMIZE_TMPDIR_PATH', str(tmp_path))
    connection = SimpleNamespace(execute=lambda _statement: FakeResult('second-instance'))

    with pytest.raises(ValueError, match='不匹配'):
        retention_optimize._disk_paths(connection)

    connection = SimpleNamespace(execute=lambda _statement: FakeResult('first-instance'))
    assert retention_optimize._disk_paths(connection) == (str(tmp_path), str(tmp_path))
