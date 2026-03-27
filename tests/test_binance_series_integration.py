from coinx.collector.binance.series import collect_and_store_series
from coinx.models import BinanceKline


def test_collect_and_store_series_fetches_parses_and_saves(db_session, monkeypatch):
    payload = [
        [
            1711526400000,
            "68000.10",
            "68100.20",
            "67950.30",
            "68020.40",
            "123.45",
            1711526699999,
            "8398765.43",
            9876,
            "60.70",
            "4123456.78",
            "0",
        ]
    ]

    def fake_fetch(series_type, symbol, period, limit, session=None):
        assert series_type == "klines"
        assert symbol == "BTCUSDT"
        assert period == "5m"
        assert limit == 1
        return payload

    monkeypatch.setattr("coinx.collector.binance.series.fetch_series_payload", fake_fetch)

    result = collect_and_store_series(
        series_type="klines",
        symbol="BTCUSDT",
        period="5m",
        limit=1,
        db_session=db_session,
    )

    rows = db_session.query(BinanceKline).all()

    assert result["affected"] == 1
    assert len(rows) == 1
    assert rows[0].symbol == "BTCUSDT"
    assert rows[0].period == "5m"
    assert rows[0].open_time == 1711526400000
