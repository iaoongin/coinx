from flask import Flask
from types import SimpleNamespace

from coinx.web.routes.api_data import api_data_bp
from coinx.web.routes.api_funding_rate import api_funding_rate_bp


def _client():
    app = Flask(__name__)
    app.register_blueprint(api_data_bp)
    app.register_blueprint(api_funding_rate_bp)
    return app.test_client()


def test_invalid_as_of_ms_is_a_client_error():
    response = _client().get('/api/funding-rate/history/BTCUSDT?as_of_ms=not-a-time')
    assert response.status_code == 400
    assert 'as_of_ms' in response.get_json()['message']


def test_funding_history_forwards_as_of_ms(monkeypatch):
    captured = {}

    def fake_history(symbol, hours=1, as_of_ms=None):
        captured.update({'symbol': symbol, 'hours': hours, 'as_of_ms': as_of_ms})
        return []

    monkeypatch.setattr(
        'coinx.web.routes.api_funding_rate.load_funding_rate_history', fake_history
    )
    response = _client().get('/api/funding-rate/history/BTCUSDT?hours=24&as_of_ms=1711526400000')
    assert response.status_code == 200
    assert captured == {'symbol': 'BTCUSDT', 'hours': 24, 'as_of_ms': 1711526400000}


def test_homepage_replay_uses_as_of_anchor_and_snapshot(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        'coinx.web.routes.api_data.get_active_coins', lambda: [],
    )
    monkeypatch.setattr(
        'coinx.web.routes.api_data.get_homepage_series_snapshot',
        lambda symbols, now_ms=None: captured.update({'symbols': symbols, 'now_ms': now_ms})
        or {'data': [], 'cache_update_time': None},
    )
    response = _client().get('/api/coins?nocache=1&as_of_ms=1711526400000')
    assert response.status_code == 200
    assert captured == {'symbols': [], 'now_ms': 1711526400000}


def test_auto_as_of_uses_latest_common_five_minute_boundary(monkeypatch):
    import coinx.read_clients as read_clients
    import coinx.repositories.market_read as market_read
    from scripts.verify_api_readonly import _resolve_auto_as_of

    bounds = {
        'market_klines': (1000, 1785604200000),
        'market_open_interest_hist': (1000, 1785604200000),
        'market_taker_buy_sell_vol': (1000, 1785604200000),
        'market_funding_rate': (1000, 1785593537002),
        'market_snapshots': (None, None),
        'market_tickers': (1000, 1785593537962),
    }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    class FakeRepository:
        def __init__(self, *args, **kwargs):
            pass

        def time_bounds(self, table):
            minimum, maximum = bounds[table]
            return {'min_time': minimum, 'max_time': maximum}

    monkeypatch.setattr(read_clients, 'ClickHouseReadClient', FakeClient)
    monkeypatch.setattr(read_clients, 'MySQLReadClient', FakeClient)
    monkeypatch.setattr(market_read, 'ClickHouseMarketReadRepository', FakeRepository)
    monkeypatch.setattr(market_read, 'MySQLMarketReadRepository', FakeRepository)

    result = _resolve_auto_as_of(SimpleNamespace(
        clickhouse_url='http://ck', clickhouse_database='coinx', clickhouse_user='root',
        clickhouse_password='secret', mysql_host='mysql', mysql_database='coinx',
        mysql_user='root', mysql_password='secret',
    ))

    assert result['common_upper_ms'] == 1785593537002
    assert result['as_of_ms'] == 1785593400000
    assert set(result['time_bounds']) == set(bounds)
