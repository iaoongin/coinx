from types import SimpleNamespace

from sqlalchemy.exc import OperationalError

from coinx.repositories.series import upsert_series_records, upsert_series_records_in_batches


class _FakeMysqlSession:
    def __init__(self, failures_before_success=0, failure_error_code=1213, lock_result=1):
        self.bind = SimpleNamespace(dialect=SimpleNamespace(name='mysql'))
        self.failures_before_success = failures_before_success
        self.failure_error_code = failure_error_code
        self.lock_result = lock_result
        self.execute_calls = 0
        self.commit_calls = 0
        self.rollback_calls = 0
        self.closed = False
        self.executed_statements = []

    class _ScalarResult:
        def __init__(self, value):
            self._value = value

        def scalar(self):
            return self._value

    def execute(self, statement, params=None):
        statement_text = str(statement)
        self.executed_statements.append((statement_text, params))
        if 'GET_LOCK' in statement_text:
            return self._ScalarResult(self.lock_result)
        if 'RELEASE_LOCK' in statement_text:
            return self._ScalarResult(1)
        self.execute_calls += 1
        if self.execute_calls <= self.failures_before_success:
            raise OperationalError(
                statement='INSERT ... ON DUPLICATE KEY UPDATE',
                params={},
                orig=Exception(self.failure_error_code, 'retryable lock error'),
            )
        return self._ScalarResult(1)

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


def test_upsert_series_records_retries_mysql_lock_wait_timeout(monkeypatch):
    session = _FakeMysqlSession(failures_before_success=2, failure_error_code=1205)
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


def test_upsert_series_records_in_batches_commits_once(monkeypatch):
    session = _FakeMysqlSession()
    sleep_calls = []
    monkeypatch.setattr('coinx.repositories.series.time.sleep', lambda seconds: sleep_calls.append(seconds))

    affected = upsert_series_records_in_batches(
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
            },
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'open_time': 1711526700000,
                'close_time': 1711526999999,
                'open_price': 68020.4,
                'high_price': 68110.2,
                'low_price': 68000.0,
                'close_price': 68080.4,
            },
        ],
        batch_size=1,
        session=session,
    )

    assert affected == 2
    assert session.execute_calls == 2
    assert session.commit_calls == 1
    assert session.rollback_calls == 0
    assert sleep_calls == []


def test_upsert_series_records_uses_mysql_named_lock():
    session = _FakeMysqlSession()

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
    assert any('GET_LOCK' in statement for statement, _ in session.executed_statements)
    assert any('RELEASE_LOCK' in statement for statement, _ in session.executed_statements)


def test_upsert_series_records_in_batches_uses_mysql_named_lock_once():
    session = _FakeMysqlSession()

    affected = upsert_series_records_in_batches(
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
            },
            {
                'symbol': 'BTCUSDT',
                'period': '5m',
                'open_time': 1711526700000,
                'close_time': 1711526999999,
                'open_price': 68020.4,
                'high_price': 68110.2,
                'low_price': 68000.0,
                'close_price': 68080.4,
            },
        ],
        batch_size=1,
        session=session,
    )

    get_lock_calls = [statement for statement, _ in session.executed_statements if 'GET_LOCK' in statement]
    release_lock_calls = [statement for statement, _ in session.executed_statements if 'RELEASE_LOCK' in statement]

    assert affected == 2
    assert len(get_lock_calls) == 1
    assert len(release_lock_calls) == 1
