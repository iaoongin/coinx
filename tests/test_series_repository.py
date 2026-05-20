from types import SimpleNamespace

from sqlalchemy.exc import OperationalError

from coinx.repositories.series import upsert_series_records


class _FakeMysqlSession:
    def __init__(self, failures_before_success=0):
        self.bind = SimpleNamespace(dialect=SimpleNamespace(name='mysql'))
        self.failures_before_success = failures_before_success
        self.execute_calls = 0
        self.commit_calls = 0
        self.rollback_calls = 0
        self.closed = False

    def execute(self, _statement):
        self.execute_calls += 1
        if self.execute_calls <= self.failures_before_success:
            raise OperationalError(
                statement='INSERT ... ON DUPLICATE KEY UPDATE',
                params={},
                orig=Exception(1213, 'Deadlock found when trying to get lock; try restarting transaction'),
            )

    def commit(self):
        self.commit_calls += 1

    def rollback(self):
        self.rollback_calls += 1

    def close(self):
        self.closed = True


def test_upsert_series_records_retries_mysql_deadlock(monkeypatch):
    session = _FakeMysqlSession(failures_before_success=2)
    sleep_calls = []
    monkeypatch.setattr('coinx.repositories.series.time.sleep', lambda seconds: sleep_calls.append(seconds))

    affected = upsert_series_records(
        'binance',
        'klines',
        [
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'open_time': 1711526400000,
                'close_time': 1711526699999,
                'open_price': 68000.1,
                'high_price': 68100.2,
                'low_price': 67950.3,
                'close_price': 68020.4,
            }
        ],
        session=session,
    )

    assert affected == 1
    assert session.execute_calls == 3
    assert session.commit_calls == 1
    assert session.rollback_calls == 2
    assert sleep_calls == [0.2, 0.4]

