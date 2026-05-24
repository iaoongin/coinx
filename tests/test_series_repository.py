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
        self.get_lock_calls = 0

    class _ScalarResult:
        def __init__(self, value):
            self._value = value

        def scalar(self):
            return self._value

    def execute(self, statement, params=None):
        statement_text = str(statement)
        self.executed_statements.append((statement_text, params))
        if 'GET_LOCK' in statement_text:
            self.get_lock_calls += 1
            if isinstance(self.lock_result, (list, tuple)):
                index = min(self.get_lock_calls - 1, len(self.lock_result) - 1)
                return self._ScalarResult(self.lock_result[index])
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

    def get_bind(self):
        owner = self

        class _FakeBind:
            dialect = SimpleNamespace(name='mysql')

            def connect(self_inner):
                return owner

        return _FakeBind()

    def begin(self):
        session = self

        class _Txn:
            def commit(self_inner):
                session.commit_calls += 1

            def rollback(self_inner):
                session.rollback_calls += 1

        return _Txn()


class _FakeMysqlBoundConnection(_FakeMysqlSession):
    def __init__(self, owner):
        super().__init__(
            failures_before_success=owner.failures_before_success,
            failure_error_code=owner.failure_error_code,
            lock_result=owner.lock_result,
        )
        self.owner = owner
        self.transactions_started = 0
        self.transactions_committed = 0
        self.transactions_rolled_back = 0

    def execute(self, statement, params=None):
        result = super().execute(statement, params=params)
        self.owner.executed_statements.extend(
            (f'connection:{statement_text}', statement_params)
            for statement_text, statement_params in self.executed_statements[len(self.owner.executed_statements):]
        )
        return result

    def begin(self):
        self.transactions_started += 1
        connection = self

        class _Txn:
            def commit(self_inner):
                connection.transactions_committed += 1

            def rollback(self_inner):
                connection.transactions_rolled_back += 1

        return _Txn()

    def close(self):
        self.closed = True
        self.owner.connection_closed = True


class _FakeMysqlPinnedSession(_FakeMysqlSession):
    def __init__(self, failures_before_success=0, failure_error_code=1213, lock_result=1):
        super().__init__(
            failures_before_success=failures_before_success,
            failure_error_code=failure_error_code,
            lock_result=lock_result,
        )
        self.connection = _FakeMysqlBoundConnection(self)
        self.connection_closed = False

    def get_bind(self):
        owner = self

        class _FakeBind:
            dialect = SimpleNamespace(name='mysql')

            def connect(self_inner):
                return owner.connection

        return _FakeBind()


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


def test_upsert_series_records_retries_mysql_named_lock_timeout(monkeypatch):
    session = _FakeMysqlSession(lock_result=(0, 1))
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

    get_lock_calls = [statement for statement, _ in session.executed_statements if 'GET_LOCK' in statement]
    release_lock_calls = [statement for statement, _ in session.executed_statements if 'RELEASE_LOCK' in statement]

    assert affected == 1
    assert len(get_lock_calls) == 2
    assert len(release_lock_calls) == 1
    assert sleep_calls == [0.2]


def test_upsert_series_records_releases_mysql_named_lock_on_same_connection():
    session = _FakeMysqlPinnedSession()

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

    get_lock_calls = [statement for statement, _ in session.connection.executed_statements if 'GET_LOCK' in statement]
    release_lock_calls = [statement for statement, _ in session.connection.executed_statements if 'RELEASE_LOCK' in statement]

    assert affected == 1
    assert len(get_lock_calls) == 1
    assert len(release_lock_calls) == 1
    assert session.connection.transactions_started == 1
    assert session.connection.transactions_committed == 1
    assert session.connection_closed is True
