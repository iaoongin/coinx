from flask import Flask
import werkzeug

from coinx.web.routes.api_data import api_data_bp


def create_test_client():
    if not hasattr(werkzeug, '__version__'):
        werkzeug.__version__ = '3'
    app = Flask(__name__)
    app.register_blueprint(api_data_bp)
    return app.test_client()


def test_collect_binance_series_api_returns_summary(monkeypatch):
    def fake_collect(series_type, symbol, period, limit):
        assert series_type == 'klines'
        assert symbol == 'BTCUSDT'
        assert period == '5m'
        assert limit == 20
        return {
            'series_type': series_type,
            'symbol': symbol,
            'period': period,
            'limit': limit,
            'affected': 20,
            'records': [],
        }

    monkeypatch.setattr('coinx.web.routes.api_data.collect_and_store_series', fake_collect)
    client = create_test_client()

    response = client.post(
        '/api/binance-series/collect',
        json={
            'series_type': 'klines',
            'symbol': 'BTCUSDT',
            'period': '5m',
            'limit': 20,
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['status'] == 'success'
    assert payload['data']['affected'] == 20


def test_collect_binance_series_api_validates_required_fields():
    client = create_test_client()

    response = client.post(
        '/api/binance-series/collect',
        json={'series_type': 'klines', 'symbol': 'BTCUSDT'},
    )

    assert response.status_code == 400
    payload = response.get_json()
    assert payload['status'] == 'error'


def test_collect_binance_series_batch_api_returns_summary(monkeypatch):
    def fake_collect_batch(symbols, periods, series_types=None, limit=30):
        assert symbols == ['BTCUSDT', 'ETHUSDT']
        assert periods == ['5m', '1h']
        assert series_types == ['klines', 'open_interest_hist']
        assert limit == 10
        return {
            'symbols': symbols,
            'periods': periods,
            'series_types': series_types,
            'limit': limit,
            'success_count': 4,
            'failure_count': 0,
            'results': [],
        }

    monkeypatch.setattr('coinx.web.routes.api_data.collect_series_batch', fake_collect_batch)
    client = create_test_client()

    response = client.post(
        '/api/binance-series/batch-collect',
        json={
            'symbols': ['BTCUSDT', 'ETHUSDT'],
            'periods': ['5m', '1h'],
            'series_types': ['klines', 'open_interest_hist'],
            'limit': 10,
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['status'] == 'success'
    assert payload['data']['success_count'] == 4


def test_collect_binance_series_batch_api_validates_required_fields():
    client = create_test_client()

    response = client.post(
        '/api/binance-series/batch-collect',
        json={'symbols': ['BTCUSDT']},
    )

    assert response.status_code == 400
    payload = response.get_json()
    assert payload['status'] == 'error'
