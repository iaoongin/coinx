from types import SimpleNamespace

from coinx.repositories import market_shadow


def test_shadow_is_disabled_by_default(monkeypatch):
    monkeypatch.setattr(market_shadow.config, "CLICKHOUSE_READ_SHADOW", False)
    called = []
    monkeypatch.setattr(market_shadow, "_submit", lambda *args: called.append(args))

    market_shadow.shadow_latest_funding_rates({"BTCUSDT": {"event_time": 1}}, ["BTCUSDT"])
    market_shadow.shadow_latest_tickers([SimpleNamespace(symbol="BTCUSDT")])

    assert called == []


def test_compare_dicts_reports_missing_and_value_differences():
    differences = market_shadow._compare_dicts(
        {"BTCUSDT": {"event_time": 1, "funding_rate": 0.1}, "ETHUSDT": {}},
        {"BTCUSDT": {"event_time": 2, "funding_rate": 0.1}, "BNBUSDT": {}},
        ("event_time", "funding_rate"),
    )

    assert "ETHUSDT: missing_in_clickhouse" in differences
    assert "BNBUSDT: missing_in_mysql" in differences
    assert any(item.startswith("BTCUSDT.event_time:") for item in differences)
