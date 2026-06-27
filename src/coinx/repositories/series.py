import threading
import time
from datetime import datetime

from sqlalchemy import text

from coinx.config import DB_TYPE
from coinx.database import get_session
from coinx.models import MarketFundingRate, MarketKline, MarketOpenInterestHist, MarketTakerBuySellVol
from coinx.utils import logger


SERIES_MODEL_MAP = {
    'klines': MarketKline,
    'open_interest_hist': MarketOpenInterestHist,
    'taker_buy_sell_vol': MarketTakerBuySellVol,
    'funding_rate': MarketFundingRate,
}

SERIES_KEY_FIELDS = {
    'klines': ('exchange', 'symbol', 'period', 'open_time'),
    'open_interest_hist': ('exchange', 'symbol', 'period', 'event_time'),
    'taker_buy_sell_vol': ('exchange', 'symbol', 'period', 'event_time'),
    'funding_rate': ('exchange', 'symbol', 'period', 'event_time'),
}

MYSQL_DEADLOCK_ERROR_CODE = 1213
MYSQL_LOCK_WAIT_TIMEOUT_ERROR_CODE = 1205
MYSQL_DEADLOCK_MAX_RETRIES = 3
MYSQL_DEADLOCK_RETRY_DELAY_SECONDS = 0.2
MYSQL_NAMED_LOCK_TIMEOUT_SECONDS = 30
MYSQL_NAMED_LOCK_MAX_RETRIES = 3


def _dialect_name(db):
    """获取数据库方言名称"""
    bind = getattr(db, 'bind', None)
    if bind is None:
        bind = db.get_bind()
    return getattr(bind.dialect, 'name', '') if bind else ''


def _is_mysql_compatible_dialect(db):
    """判断是否支持 INSERT ON DUPLICATE KEY UPDATE（MySQL 和 StarRocks 均支持）"""
    dialect = _dialect_name(db)
    return dialect in ('mysql',) or (DB_TYPE == 'starrocks' and dialect != 'sqlite')


# StarRocks 应用级锁（StarRocks 不支持 GET_LOCK）
_starrocks_locks = {}
_starrocks_locks_mutex = threading.Lock()


def _get_starrocks_lock(exchange, series_type):
    key = (exchange, series_type)
    with _starrocks_locks_mutex:
        if key not in _starrocks_locks:
            _starrocks_locks[key] = threading.Lock()
        return _starrocks_locks[key]


def get_series_model(series_type):
    try:
        return SERIES_MODEL_MAP[series_type]
    except KeyError as exc:
        raise ValueError(f'unsupported market series type: {series_type}') from exc


def _is_mysql_retryable_lock_error(exc):
    original = getattr(exc, 'orig', None)
    if original is None:
        return False
    args = getattr(original, 'args', ()) or ()
    if not args:
        return False
    try:
        return int(args[0]) in (MYSQL_DEADLOCK_ERROR_CODE, MYSQL_LOCK_WAIT_TIMEOUT_ERROR_CODE)
    except (TypeError, ValueError):
        return False


def _series_lock_name(exchange, series_type):
    return f'coinx:series:{exchange}:{series_type}'


def _acquire_mysql_named_lock(db, exchange, series_type, timeout_seconds=MYSQL_NAMED_LOCK_TIMEOUT_SECONDS):
    lock_name = _series_lock_name(exchange, series_type)
    for attempt in range(1, MYSQL_NAMED_LOCK_MAX_RETRIES + 1):
        result = db.execute(
            text('SELECT GET_LOCK(:lock_name, :timeout_seconds)'),
            {
                'lock_name': lock_name,
                'timeout_seconds': int(timeout_seconds),
            },
        ).scalar()
        if result == 1:
            return lock_name
        if attempt >= MYSQL_NAMED_LOCK_MAX_RETRIES:
            break
        logger.warning(
            'MySQL named lock retry for series write exchange=%s series_type=%s attempt=%d/%d',
            exchange,
            series_type,
            attempt,
            MYSQL_NAMED_LOCK_MAX_RETRIES,
        )
        time.sleep(MYSQL_DEADLOCK_RETRY_DELAY_SECONDS * attempt)
    raise TimeoutError(
        f'failed to acquire MySQL named lock for exchange={exchange} series_type={series_type}'
    )


def _release_mysql_named_lock(db, lock_name):
    if not lock_name:
        return
    db.execute(
        text('SELECT RELEASE_LOCK(:lock_name)'),
        {'lock_name': lock_name},
    )


def _with_mysql_named_lock(db, exchange, series_type, callback):
    connection = db.get_bind().connect()
    lock_name = None
    try:
        lock_name = _acquire_mysql_named_lock(connection, exchange, series_type)
        return callback(connection)
    finally:
        try:
            if lock_name:
                _release_mysql_named_lock(connection, lock_name)
        finally:
            connection.close()


def _with_starrocks_lock(exchange, series_type, callback):
    """StarRocks 使用进程内 threading.Lock 替代 MySQL 命名锁"""
    lock = _get_starrocks_lock(exchange, series_type)
    with lock:
        return callback()


def _with_write_lock(db, exchange, series_type, callback):
    """根据数据库类型选择合适的锁策略"""
    if DB_TYPE == 'starrocks':
        # StarRocks 不支持 GET_LOCK，使用进程内锁
        return _with_starrocks_lock(exchange, series_type, lambda: callback(db))
    if DB_TYPE == 'mysql':
        return _with_mysql_named_lock(db, exchange, series_type, callback)
    return callback(db)


def _build_values_list(model, exchange, records):
    values_list = []
    model_columns = {column.name for column in model.__table__.columns}
    has_updated_at = 'updated_at' in model_columns
    for record in records:
        values = dict(record)
        values['exchange'] = exchange
        if has_updated_at:
            values['updated_at'] = datetime.now()
        values = {key: value for key, value in values.items() if key in model_columns}
        values_list.append(values)
    return values_list


def _sort_values_list(series_type, values_list):
    key_fields = SERIES_KEY_FIELDS[series_type]
    return sorted(values_list, key=lambda values: tuple(values[field] for field in key_fields))


def _upsert_values_on_mysql_compatible(model, exchange, series_type, values_list, db, commit=True):
    """MySQL: INSERT ... ON DUPLICATE KEY UPDATE; StarRocks: INSERT（主键自动覆盖）"""
    columns = [c.name for c in model.__table__.columns]
    table_name = model.__tablename__

    row_placeholders = []
    params = {}
    for i, values in enumerate(values_list):
        cols = []
        for col in columns:
            key = f'{col}_{i}'
            params[key] = values.get(col)
            cols.append(f':{key}')
        row_placeholders.append(f"({', '.join(cols)})")

    if DB_TYPE == 'starrocks':
        insert_columns = [c for c in columns if c != 'id']
        sr_row_placeholders = []
        sr_params = {}
        for i, values in enumerate(values_list):
            cols = []
            for col in insert_columns:
                key = f'{col}_{i}'
                sr_params[key] = values.get(col)
                cols.append(f':{key}')
            sr_row_placeholders.append(f"({', '.join(cols)})")
        sql = f"INSERT INTO {table_name} ({', '.join(insert_columns)}) VALUES {', '.join(sr_row_placeholders)}"
        params = sr_params
    else:
        updatable = [c for c in columns if c not in ('id', 'created_at')]
        sql = (
            f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES "
            f"{', '.join(row_placeholders)} "
            f"ON DUPLICATE KEY UPDATE "
            f"{', '.join(f'{col} = VALUES({col})' for col in updatable)}"
        )

    for attempt in range(1, MYSQL_DEADLOCK_MAX_RETRIES + 1):
        try:
            db.execute(text(sql), params)
            if commit:
                db.commit()
            return len(values_list)
        except Exception as exc:
            db.rollback()
            if not _is_mysql_retryable_lock_error(exc) or attempt >= MYSQL_DEADLOCK_MAX_RETRIES:
                raise
            logger.warning(
                'MySQL/StarRocks lock retry for series write exchange=%s series_type=%s records=%d attempt=%d/%d',
                exchange,
                series_type,
                len(values_list),
                attempt,
                MYSQL_DEADLOCK_MAX_RETRIES,
            )
            time.sleep(MYSQL_DEADLOCK_RETRY_DELAY_SECONDS * attempt)


def upsert_series_records_in_batches(exchange, series_type, records, batch_size, session=None):
    if not records:
        return 0

    model = get_series_model(series_type)
    own_session = session is None
    db = session or get_session()
    effective_batch_size = max(1, int(batch_size or 1))

    try:
        if _is_mysql_compatible_dialect(db):
            def _write_batches(connection):
                try:
                    batch_affected = 0
                    for index in range(0, len(records), effective_batch_size):
                        batch_records = records[index:index + effective_batch_size]
                        values_list = _sort_values_list(series_type, _build_values_list(model, exchange, batch_records))
                        batch_affected += _upsert_values_on_mysql_compatible(
                            model,
                            exchange,
                            series_type,
                            values_list,
                            connection,
                            commit=False,
                        )
                    connection.commit()
                    return batch_affected
                except Exception:
                    connection.rollback()
                    raise

            return _with_write_lock(db, exchange, series_type, _write_batches)

        affected = 0
        for index in range(0, len(records), effective_batch_size):
            batch_records = records[index:index + effective_batch_size]
            affected += upsert_series_records(exchange, series_type, batch_records, session=db)
        return affected
    except Exception:
        db.rollback()
        raise
    finally:
        if own_session:
            db.close()


def upsert_series_records(exchange, series_type, records, session=None):
    if not records:
        return 0

    model = get_series_model(series_type)
    key_fields = SERIES_KEY_FIELDS[series_type]

    own_session = session is None
    db = session or get_session()
    try:
        values_list = _sort_values_list(series_type, _build_values_list(model, exchange, records))

        if _is_mysql_compatible_dialect(db):
            def _write_records(connection):
                try:
                    affected = _upsert_values_on_mysql_compatible(
                        model,
                        exchange,
                        series_type,
                        values_list,
                        connection,
                        commit=False,
                    )
                    connection.commit()
                    return affected
                except Exception:
                    connection.rollback()
                    raise

            return _with_write_lock(db, exchange, series_type, _write_records)

        # SQLite 等不支持 ON DUPLICATE KEY UPDATE 的方言，走 ORM 读改写
        key_fields_list = list(key_fields)
        key_columns = [getattr(model, field) for field in key_fields_list]
        from sqlalchemy import or_, and_
        _OR_BATCH = 50
        all_key_tuples = [
            tuple(values[field] for field in key_fields_list)
            for values in values_list
        ]
        existing_by_key = {}
        for batch_start in range(0, len(all_key_tuples), _OR_BATCH):
            batch_tuples = all_key_tuples[batch_start:batch_start + _OR_BATCH]
            or_conditions = [
                and_(*[col == val for col, val in zip(key_columns, kt)])
                for kt in batch_tuples
            ]
            rows = db.query(model).filter(or_(*or_conditions)).all()
            for row in rows:
                key = tuple(getattr(row, field) for field in key_fields)
                existing_by_key[key] = row

        affected = 0
        for values in values_list:
            unique_key = tuple(values[field] for field in key_fields)
            instance = existing_by_key.get(unique_key)
            if instance is None:
                db.add(model(**values))
            else:
                for key, value in values.items():
                    setattr(instance, key, value)
            affected += 1

        db.commit()
        return affected
    except Exception:
        db.rollback()
        raise
    finally:
        if own_session:
            db.close()


def get_existing_series_timestamps(exchange, series_type, symbols, timestamps, period='5m', session=None):
    if not symbols or not timestamps:
        return {symbol: set() for symbol in symbols or []}

    model = get_series_model(series_type)
    timestamp_field = 'open_time' if series_type == 'klines' else 'event_time'
    time_column = getattr(model, timestamp_field)

    own_session = session is None
    db = session or get_session()

    try:
        rows = (
            db.query(model.symbol, time_column)
            .filter(
                model.exchange == exchange,
                model.symbol.in_(symbols),
                model.period == period,
                time_column.in_(timestamps),
            )
            .all()
        )
        existing = {symbol: set() for symbol in symbols}
        for symbol, timestamp in rows:
            if timestamp is not None:
                existing.setdefault(symbol, set()).add(int(timestamp))
        return existing
    finally:
        if own_session:
            db.close()
