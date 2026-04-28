from coinx.models import BinanceTopLongShortPositionRatio, BinanceKline
from coinx.repositories.binance_series import upsert_series_records


def test_upsert_series_records_inserts_and_updates_ratio_rows(db_session):
    inserted = upsert_series_records(
        "top_long_short_position_ratio",
        [
            {
                "symbol": "BTCUSDT",
                "period": "5m",
                "event_time": 1711526400000,
                "long_short_ratio": 1.5,
                "long_account": 0.6,
                "short_account": 0.4,
            }
        ],
        session=db_session,
    )

    updated = upsert_series_records(
        "top_long_short_position_ratio",
        [
            {
                "symbol": "BTCUSDT",
                "period": "5m",
                "event_time": 1711526400000,
                "long_short_ratio": 1.8,
                "long_account": 0.64,
                "short_account": 0.36,
            }
        ],
        session=db_session,
    )

    rows = db_session.query(BinanceTopLongShortPositionRatio).all()

    assert inserted == 1
    assert updated == 1
    assert len(rows) == 1
    assert float(rows[0].long_short_ratio) == 1.8


def test_upsert_series_records_uses_kline_unique_key(db_session):
    upsert_series_records(
        "klines",
        [
            {
                "symbol": "BTCUSDT",
                "period": "5m",
                "open_time": 1711526400000,
                "close_time": 1711526699999,
                "open_price": 68000.1,
                "high_price": 68100.2,
                "low_price": 67950.3,
                "close_price": 68020.4,
                "volume": 123.45,
                "quote_volume": 8398765.43,
                "trade_count": 9876,
                "taker_buy_base_volume": 60.7,
                "taker_buy_quote_volume": 4123456.78,
            },
            {
                "symbol": "BTCUSDT",
                "period": "5m",
                "open_time": 1711526700000,
                "close_time": 1711526999999,
                "open_price": 68020.4,
                "high_price": 68120.2,
                "low_price": 68000.3,
                "close_price": 68050.4,
                "volume": 100.0,
                "quote_volume": 7000000.0,
                "trade_count": 9000,
                "taker_buy_base_volume": 50.7,
                "taker_buy_quote_volume": 3000000.0,
            },
        ],
        session=db_session,
    )

    rows = (
        db_session.query(BinanceKline)
        .order_by(BinanceKline.open_time.asc())
        .all()
    )

    assert len(rows) == 2
    assert rows[0].open_time == 1711526400000
    assert rows[1].open_time == 1711526700000
