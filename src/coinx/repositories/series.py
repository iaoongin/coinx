from sqlalchemy import tuple_
from sqlalchemy.dialects.mysql import insert as mysql_insert

from coinx.database import get_session
from coinx.models import MarketKline, MarketOpenInterestHist, MarketTakerBuySellVol


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


def get_series_model(series_type):
    try:
        return SERIES_MODEL_MAP[series_type]
    except KeyError as exc:
        raise ValueError(f"不支持的市场序列类型: {series_type}") from exc


def upsert_series_records(exchange, series_type, records, session=None):
    if not records:
        return 0

    model = get_series_model(series_type)
    key_fields = SERIES_KEY_FIELDS[series_type]

    own_session = session is None
    db = session or get_session()

    try:
        values_list = []
        model_columns = {column.name for column in model.__table__.columns}
        for record in records:
            values = dict(record)
            values['exchange'] = exchange
            values = {key: value for key, value in values.items() if key in model_columns}
            values_list.append(values)

        if db.bind and db.bind.dialect.name == 'mysql':
            statement = mysql_insert(model).values(values_list)
            update_columns = {
                column.name: statement.inserted[column.name]
                for column in model.__table__.columns
                if column.name not in ('id', 'created_at')
            }
            db.execute(statement.on_duplicate_key_update(**update_columns))
            db.commit()
            return len(values_list)

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
