from sqlalchemy import func

from coinx.database import get_session
from coinx.models import (
    BinanceTopLongShortPositionRatio,
    BinanceTopLongShortAccountRatio,
    BinanceOpenInterestHist,
    BinanceKline,
    BinanceGlobalLongShortAccountRatio,
    BinanceTakerBuySellVol,
)


SERIES_MODEL_MAP = {
    'top_long_short_position_ratio': BinanceTopLongShortPositionRatio,
    'top_long_short_account_ratio': BinanceTopLongShortAccountRatio,
    'open_interest_hist': BinanceOpenInterestHist,
    'klines': BinanceKline,
    'global_long_short_account_ratio': BinanceGlobalLongShortAccountRatio,
    'taker_buy_sell_vol': BinanceTakerBuySellVol,
}

SERIES_KEY_FIELDS = {
    'top_long_short_position_ratio': ('symbol', 'period', 'event_time'),
    'top_long_short_account_ratio': ('symbol', 'period', 'event_time'),
    'open_interest_hist': ('symbol', 'period', 'event_time'),
    'klines': ('symbol', 'period', 'open_time'),
    'global_long_short_account_ratio': ('symbol', 'period', 'event_time'),
    'taker_buy_sell_vol': ('symbol', 'period', 'event_time'),
}


def get_series_model(series_type):
    """根据序列类型返回对应模型类。"""
    try:
        return SERIES_MODEL_MAP[series_type]
    except KeyError as exc:
        raise ValueError(f"不支持的序列类型: {series_type}") from exc


def upsert_series_records(series_type, records, session=None):
    """按唯一业务键对序列数据做幂等写入。"""
    if not records:
        return 0

    model = get_series_model(series_type)
    key_fields = SERIES_KEY_FIELDS[series_type]

    own_session = session is None
    db = session or get_session()

    try:
        affected = 0
        for record in records:
            unique_filter = {field: record[field] for field in key_fields}
            instance = db.query(model).filter_by(**unique_filter).one_or_none()

            if instance is None:
                db.add(model(**record))
            else:
                for key, value in record.items():
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


def get_latest_series_timestamp(series_type, symbol, period='5m', session=None):
    """Return the latest local timestamp for a series."""
    model = get_series_model(series_type)
    timestamp_field = 'open_time' if series_type == 'klines' else 'event_time'

    own_session = session is None
    db = session or get_session()

    try:
        column = getattr(model, timestamp_field)
        return (
            db.query(func.max(column))
            .filter(model.symbol == symbol, model.period == period)
            .scalar()
        )
    finally:
        if own_session:
            db.close()


def get_earliest_series_timestamp(series_type, symbol, period='5m', session=None):
    """Return the earliest local timestamp for a series."""
    model = get_series_model(series_type)
    timestamp_field = 'open_time' if series_type == 'klines' else 'event_time'

    own_session = session is None
    db = session or get_session()

    try:
        column = getattr(model, timestamp_field)
        return (
            db.query(func.min(column))
            .filter(model.symbol == symbol, model.period == period)
            .scalar()
        )
    finally:
        if own_session:
            db.close()
