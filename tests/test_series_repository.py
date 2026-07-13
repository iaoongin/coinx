from types import SimpleNamespace

from sqlalchemy.exc import InvalidRequestError, OperationalError

from coinx.repositories.series import upsert_series_records, upsert_series_records_in_batches
from coinx.models import MarketKline, MarketOpenInterestHist


def test_open_interest_is_normalized_from_value_and_matching_kline_before_insert(db_session):
    timestamp = 1711526400000
    db_session.add(MarketKline(
        exchange='gate', symbol='BTCUSDT', period='5m', open_time=timestamp,
        close_time=timestamp + 299999, open_price=100, high_price=101,
        low_price=99, close_price=100, volume=1,
    ))
    db_session.commit()

    upsert_series_records('gate', 'open_interest_hist', [{
        'symbol': 'BTCUSDT',
        'period': '5m',
        'event_time': timestamp,
        'sum_open_interest': 1_000_000,
        'sum_open_interest_value': 2_500,
    }], session=db_session)

    row = db_session.query(MarketOpenInterestHist).one()
    assert float(row.sum_open_interest) == 25.0


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
        self.autobegin_started = False

    def execute(self, statement, params=None):
        self.autobegin_started = True
        result = super().execute(statement, params=params)
        self.owner.executed_statements.extend(
            (f'connection:{statement_text}', statement_params)
            for statement_text, statement_params in self.executed_statements[len(self.owner.executed_statements):]
        )
        return result

    def begin(self):
        if self.autobegin_started:
            raise InvalidRequestError(
                "This connection has already initialized a SQLAlchemy Transaction() object via begin() or autobegin;"
                " can't call begin() here unless rollback() or commit() is called first."
            )
        self.transactions_started += 1
        connection = self

        class _Txn:
            def commit(self_inner):
                connection.transactions_committed += 1
                connection.autobegin_started = False

            def rollback(self_inner):
                connection.transactions_rolled_back += 1
                connection.autobegin_started = False

        return _Txn()

    def commit(self):
        self.commit_calls += 1
        self.autobegin_started = False

    def rollback(self):
        self.rollback_calls += 1
        self.autobegin_started = False

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
    monkeypatch.setattr('coinx.repositories.series.DB_TYPE', 'mysql')
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
    monkeypatch.setattr('coinx.repositories.series.DB_TYPE', 'mysql')
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
    monkeypatch.setattr('coinx.repositories.series.DB_TYPE', 'mysql')
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


def test_upsert_series_records_uses_mysql_named_lock(monkeypatch):
    monkeypatch.setattr('coinx.repositories.series.DB_TYPE', 'mysql')
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


def test_upsert_series_records_in_batches_uses_mysql_named_lock_once(monkeypatch):
    monkeypatch.setattr('coinx.repositories.series.DB_TYPE', 'mysql')
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
    monkeypatch.setattr('coinx.repositories.series.DB_TYPE', 'mysql')
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


def test_upsert_series_records_releases_mysql_named_lock_on_same_connection(monkeypatch):
    monkeypatch.setattr('coinx.repositories.series.DB_TYPE', 'mysql')
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
    assert session.connection.commit_calls == 1
    assert session.connection.rollback_calls == 0
    assert session.connection_closed is True


# ---------- StarRocks dialect tests ----------

class _FakeStarrocksSession:
    """Simulate StarRocks session (INSERT ON DUPLICATE KEY UPDATE, no GET_LOCK)"""

    def __init__(self, failures_before_success=0, failure_error_code=1213):
        self.bind = SimpleNamespace(dialect=SimpleNamespace(name='mysql'))
        self.failures_before_success = failures_before_success
        self.failure_error_code = failure_error_code
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


def _starrocks_sample_record():
    return {
        'symbol': 'BTCUSDT',
        'period': '5m',
        'open_time': 1711526400000,
        'close_time': 1711526699999,
        'open_price': 68000.1,
        'high_price': 68100.2,
        'low_price': 67950.3,
        'close_price': 68020.4,
    }


def test_starrocks_upsert_series_records_basic(monkeypatch):
    """StarRocks 方言下 upsert 不调用 GET_LOCK/RELEASE_LOCK"""
    monkeypatch.setattr('coinx.repositories.series.DB_TYPE', 'starrocks')
    session = _FakeStarrocksSession()

    affected = upsert_series_records(
        'binance',
        'klines',
        [_starrocks_sample_record()],
        session=session,
    )

    assert affected == 1
    assert session.execute_calls == 1
    assert session.commit_calls == 1
    assert session.rollback_calls == 0
    # 确认没有 GET_LOCK / RELEASE_LOCK 调用
    assert not any('GET_LOCK' in s for s, _ in session.executed_statements)
    assert not any('RELEASE_LOCK' in s for s, _ in session.executed_statements)


def test_starrocks_upsert_series_records_retries_on_deadlock(monkeypatch):
    """StarRocks 方言下 deadlock 也应重试（与 MySQL 行为一致）"""
    monkeypatch.setattr('coinx.repositories.series.DB_TYPE', 'starrocks')
    session = _FakeStarrocksSession(failures_before_success=2)
    sleep_calls = []
    monkeypatch.setattr('coinx.repositories.series.time.sleep', lambda seconds: sleep_calls.append(seconds))

    affected = upsert_series_records(
        'binance',
        'klines',
        [_starrocks_sample_record()],
        session=session,
    )

    assert affected == 1
    assert session.execute_calls == 3
    assert session.commit_calls == 1
    assert session.rollback_calls == 2
    assert sleep_calls == [0.2, 0.4]


def test_starrocks_upsert_series_records_in_batches_commits_once(monkeypatch):
    """StarRocks 方言下批量 upsert 只提交一次（与 MySQL 行为一致）"""
    monkeypatch.setattr('coinx.repositories.series.DB_TYPE', 'starrocks')
    session = _FakeStarrocksSession()

    affected = upsert_series_records_in_batches(
        'binance',
        'klines',
        [
            _starrocks_sample_record(),
            {
                **_starrocks_sample_record(),
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
    # 确认没有 GET_LOCK / RELEASE_LOCK 调用
    assert not any('GET_LOCK' in s for s, _ in session.executed_statements)
    assert not any('RELEASE_LOCK' in s for s, _ in session.executed_statements)
