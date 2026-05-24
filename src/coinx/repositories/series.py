import time

from sqlalchemy import text, tuple_
from sqlalchemy.dialects.mysql import insert as mysql_insert

from coinx.database import get_session
from coinx.models import MarketKline, MarketOpenInterestHist, MarketTakerBuySellVol
from coinx.utils import logger


SERIES_MODEL_MAP = {
    'klines': MarketKline,
    'open_interest_hist': MarketOpenInterestHist,
    'taker_buy_sell_vol': MarketTakerBuySellVol,
}

SERIES_KEY_FIELDS = {
    'klines': ('exchange', 'symbol', 'period', 'open_time'),
    'open_interest_hist': ('exchange', 'symbol', 'period', 'event_time'),
    'taker_buy_sell_vol': ('exchange', 'symbol', 'period', 'event_time'),
}

MYSQL_DEADLOCK_ERROR_CODE = 1213
MYSQL_LOCK_WAIT_TIMEOUT_ERROR_CODE = 1205
MYSQL_DEADLOCK_MAX_RETRIES = 3
MYSQL_DEADLOCK_RETRY_DELAY_SECONDS = 0.2
MYSQL_NAMED_LOCK_TIMEOUT_SECONDS = 30
MYSQL_NAMED_LOCK_MAX_RETRIES = 3


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


def _build_values_list(model, exchange, records):
    values_list = []
    model_columns = {column.name for column in model.__table__.columns}
    for record in records:
        values = dict(record)
        values['exchange'] = exchange
        values = {key: value for key, value in values.items() if key in model_columns}
        values_list.append(values)
    return values_list


def _sort_values_list(series_type, values_list):
    key_fields = SERIES_KEY_FIELDS[series_type]
    return sorted(values_list, key=lambda values: tuple(values[field] for field in key_fields))


def _upsert_mysql_values(model, exchange, series_type, values_list, db, commit=True):
    statement = mysql_insert(model).values(values_list)
    update_columns = {
        column.name: statement.inserted[column.name]
        for column in model.__table__.columns
        if column.name not in ('id', 'created_at')
    }
    for attempt in range(1, MYSQL_DEADLOCK_MAX_RETRIES + 1):
        try:
            db.execute(statement.on_duplicate_key_update(**update_columns))
            if commit:
                db.commit()
            return len(values_list)
        except Exception as exc:
            db.rollback()
            if not _is_mysql_retryable_lock_error(exc) or attempt >= MYSQL_DEADLOCK_MAX_RETRIES:
                raise
            logger.warning(
                'MySQL lock retry for series write exchange=%s series_type=%s records=%d attempt=%d/%d',
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
        affected = 0
        if db.bind and db.bind.dialect.name == 'mysql':
            def _write_batches(connection):
                transaction = connection.begin()
                try:
                    mysql_affected = 0
                    for index in range(0, len(records), effective_batch_size):
                        batch_records = records[index:index + effective_batch_size]
                        values_list = _sort_values_list(series_type, _build_values_list(model, exchange, batch_records))
                        mysql_affected += _upsert_mysql_values(
                            model,
                            exchange,
                            series_type,
                            values_list,
                            connection,
                            commit=False,
                        )
                    transaction.commit()
                    return mysql_affected
                except Exception:
                    transaction.rollback()
                    raise

            return _with_mysql_named_lock(db, exchange, series_type, _write_batches)

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

        if db.bind and db.bind.dialect.name == 'mysql':
            def _write_records(connection):
                transaction = connection.begin()
                try:
                    affected = _upsert_mysql_values(
                        model,
                        exchange,
                        series_type,
                        values_list,
                        connection,
                        commit=False,
                    )
                    transaction.commit()
                    return affected
                except Exception:
                    transaction.rollback()
                    raise

            return _with_mysql_named_lock(db, exchange, series_type, _write_records)

        key_values = [tuple(values[field] for field in key_fields) for values in values_list]
        key_columns = [getattr(model, field) for field in key_fields]
        existing_rows = (
            db.query(model)
            .filter(tuple_(*key_columns).in_(key_values))
            .all()
        )
        existing_by_key = {
            tuple(getattr(row, field) for field in key_fields): row
            for row in existing_rows
        }

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
