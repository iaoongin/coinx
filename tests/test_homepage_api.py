from flask import Flask
import werkzeug

from coinx.web.routes.api_data import api_data_bp


def create_test_client():
    if not hasattr(werkzeug, '__version__'):
        werkzeug.__version__ = '3'
    app = Flask(__name__)
    app.register_blueprint(api_data_bp)
    return app.test_client()


def test_get_coins_uses_homepage_series_repository(monkeypatch):
    monkeypatch.setattr('coinx.web.routes.api_data.get_active_coins', lambda: ['BTCUSDT'])
    monkeypatch.setattr(
        'coinx.web.routes.api_data.get_homepage_series_data',
        lambda symbols: (_ for _ in ()).throw(AssertionError('should not load homepage data separately')),
    )
    monkeypatch.setattr(
        'coinx.web.routes.api_data.get_homepage_series_update_time',
        lambda symbols: (_ for _ in ()).throw(AssertionError('should not load homepage update time separately')),
    )
    monkeypatch.setattr(
        'coinx.web.routes.api_data.get_homepage_series_snapshot',
        lambda symbols: {
            'data': [
                {
                    'symbol': 'BTCUSDT',
                    'current_open_interest': 100.0,
                    'current_open_interest_formatted': '100.00',
                    'current_open_interest_value': 200.0,
                    'current_open_interest_value_formatted': '200.00',
                    'current_price': 2.0,
                    'current_price_formatted': '2.00',
                    'price_change': 1.0,
                    'price_change_percent': 100.0,
                    'price_change_formatted': '1.00',
                    'net_inflow': {'5m': 12.0},
                    'changes': {
                        '5m': {
                            'ratio': 5.0,
                            'value_ratio': 6.0,
                            'open_interest': 95.0,
                            'open_interest_formatted': '95.00',
                            'open_interest_value': 188.0,
                            'open_interest_value_formatted': '188.00',
                            'price_change': 0.1,
                            'price_change_percent': 5.0,
                            'price_change_formatted': '0.10',
                            'current_price': 1.9,
                            'current_price_formatted': '1.90',
                        }
                    },
                }
            ],
            'cache_update_time': 1234567890000,
        },
    )
    client = create_test_client()

    response = client.get('/api/coins')

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['status'] == 'success'
    assert payload['cache_update_time'] == 1234567890000
    assert payload['data'][0]['symbol'] == 'BTCUSDT'
    assert payload['data'][0]['changes'][0]['interval'] == '5m'


def test_update_data_uses_homepage_series_refresh(monkeypatch):
    captured = {}

    class FakeThread:
        def __init__(self, target=None, kwargs=None):
            captured['target'] = target
            captured['kwargs'] = kwargs or {}
            self.daemon = False

        def start(self):
            captured['started'] = True

    monkeypatch.setattr('coinx.web.routes.api_data.get_active_coins', lambda: ['BTCUSDT'])
    monkeypatch.setattr('coinx.web.routes.api_data.should_refresh_homepage_series', lambda symbols: True)
    monkeypatch.setattr('coinx.web.routes.api_data.threading.Thread', FakeThread)
    monkeypatch.setattr(
        'coinx.web.routes.api_data.repair_tracked_symbols',
        lambda **kwargs: {'status': 'success', 'results': [], **kwargs},
    )
    client = create_test_client()

    response = client.get('/api/update')

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['status'] == 'success'
    assert captured['target'] is not None
    assert captured['kwargs']['symbols'] == ['BTCUSDT']
    assert captured['kwargs']['series_types'] == ['klines', 'open_interest_hist']
    assert captured['started'] is True
