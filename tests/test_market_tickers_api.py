import pytest
from flask import Flask
import werkzeug

from coinx.web.routes.api_data import api_data_bp


def create_test_client():
    if not hasattr(werkzeug, '__version__'):
        werkzeug.__version__ = '3'
    app = Flask(__name__)
    app.register_blueprint(api_data_bp)
    return app.test_client()


class MockTicker:
    def __init__(self, symbol, price_change_percent, last_price, volume, quote_volume):
        self.symbol = symbol
        self.price_change_percent = price_change_percent
        self.last_price = last_price
        self.volume = volume
        self.quote_volume = quote_volume


def test_get_market_rank_api_default(monkeypatch):
    mock_data = [
        MockTicker('BTCUSDT', -2.0, 49000.0, 1000.0, 50000000.0),
        MockTicker('ETHUSDT', 1.5, 3050.0, 2000.0, 6000000.0),
    ]
    
    def mock_get_tickers(**kwargs):
        return mock_data
    
    monkeypatch.setattr('coinx.web.routes.api_data.get_market_tickers', mock_get_tickers)
    monkeypatch.setattr('coinx.web.routes.api_data.get_latest_close_time', lambda: 1700100000000)
    
    client = create_test_client()
    response = client.get('/api/market-rank')
    
    assert response.status_code == 200
    data = response.get_json()
    assert data['status'] == 'success'
    assert len(data['data']) == 2
    assert data['data'][0]['symbol'] == 'BTCUSDT'


def test_get_market_rank_api_with_params(monkeypatch):
    mock_data = [
        MockTicker('ETHUSDT', 1.5, 3050.0, 2000.0, 6000000.0),
        MockTicker('BTCUSDT', -2.0, 49000.0, 1000.0, 50000000.0),
    ]
    
    def mock_get_tickers(**kwargs):
        return mock_data
    
    monkeypatch.setattr('coinx.web.routes.api_data.get_market_tickers', mock_get_tickers)
    monkeypatch.setattr('coinx.web.routes.api_data.get_latest_close_time', lambda: 1700100000000)
    
    client = create_test_client()
    response = client.get('/api/market-rank?type=quote_volume&direction=up&limit=50')
    
    assert response.status_code == 200
    data = response.get_json()
    assert data['status'] == 'success'
    assert data['snapshot_time'] == 1700100000000


def test_get_market_rank_api_empty(monkeypatch):
    def mock_get_tickers(**kwargs):
        return []
    
    monkeypatch.setattr('coinx.web.routes.api_data.get_market_tickers', mock_get_tickers)
    monkeypatch.setattr('coinx.web.routes.api_data.get_latest_close_time', lambda: None)
    
    client = create_test_client()
    response = client.get('/api/market-rank')
    
    assert response.status_code == 200
    data = response.get_json()
    assert data['status'] == 'success'
    assert data['data'] == []


def test_get_market_rank_api_error(monkeypatch):
    def mock_error(**kwargs):
        raise Exception('DB error')
    
    monkeypatch.setattr('coinx.web.routes.api_data.get_market_tickers', mock_error)
    
    client = create_test_client()
    response = client.get('/api/market-rank')
    
    assert response.status_code == 500
    data = response.get_json()
    assert data['status'] == 'error'


def test_refresh_market_rank_api_success(monkeypatch):
    monkeypatch.setattr(
        'coinx.web.routes.api_data.refresh_market_tickers',
        lambda: {
            'status': 'success',
            'message': 'market rank snapshot refreshed',
            'saved_count': 2,
            'snapshot_time': 1700100000000,
        },
    )

    client = create_test_client()
    response = client.post('/api/market-rank/refresh')

    assert response.status_code == 200
    data = response.get_json()
    assert data['status'] == 'success'
    assert data['data']['saved_count'] == 2
    assert data['data']['snapshot_time'] == 1700100000000


def test_refresh_market_rank_api_error(monkeypatch):
    monkeypatch.setattr(
        'coinx.web.routes.api_data.refresh_market_tickers',
        lambda: {
            'status': 'error',
            'message': 'market rank refresh failed',
            'saved_count': 0,
            'snapshot_time': None,
        },
    )

    client = create_test_client()
    response = client.post('/api/market-rank/refresh')

    assert response.status_code == 500
    data = response.get_json()
    assert data['status'] == 'error'
    assert 'failed to refresh market rank' in data['message']
