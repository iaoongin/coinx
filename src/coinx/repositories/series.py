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
        affected = 0
        for record in records:
            values = dict(record)
            values['exchange'] = exchange
            unique_filter = {field: values[field] for field in key_fields}
            instance = db.query(model).filter_by(**unique_filter).one_or_none()

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
