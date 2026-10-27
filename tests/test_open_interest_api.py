from flask import Flask

from coinx.web.routes.api_data import api_data_bp


def test_open_interest_api_returns_aggregated_page(monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(api_data_bp)
    expected = {
        'data': [{
            'symbol': 'BTCUSDT',
            'total_open_interest': 150.0,
            'total_open_interest_value': 150000.0,
            'latest_price': 2000.0,
            'circulating_market_cap': 2000000.0,
            'circulating_supply': 1000.0,
            'oi_circulating_ratio': 0.075,
            'oi_circulating_percent': 7.5,
            'exchanges': ['binance', 'okx'],
            'has_time_mismatch': True,
            'stale_exchanges': ['okx'],
        }],
        'total_count': 1,
        'page': 1,
        'page_size': 50,
        'period': '5m',
        'snapshot_time': 100,
        'stats': {'total_symbols': 1, 'with_supply': 1, 'stale_symbols': 1, 'exchanges': ['binance', 'okx']},
    }
    monkeypatch.setattr(
        'coinx.web.routes.api_data.get_open_interest_page',
        lambda **kwargs: expected,
    )

    response = app.test_client().get('/api/open-interest?period=5m&keyword=BTC')

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['status'] == 'success'
    assert payload['data'][0]['symbol'] == 'BTCUSDT'
    assert payload['data'][0]['total_open_interest'] == 150.0


def test_open_interest_api_passes_latest_time_filter(monkeypatch):
    captured = {}

    def fake_get_open_interest_page(**kwargs):
        captured.update(kwargs)
        return {
            'data': [],
            'total_count': 0,
            'page': 1,
            'page_size': 50,
            'period': '5m',
            'snapshot_time': None,
            'stats': {},
        }

    monkeypatch.setattr(
        'coinx.web.routes.api_data.get_open_interest_page',
        fake_get_open_interest_page,
    )

    response = Flask(__name__)
    response.register_blueprint(api_data_bp)
    result = response.test_client().get('/api/open-interest?only_latest_time=true')

    assert result.status_code == 200
    assert captured['only_latest_time'] is True
