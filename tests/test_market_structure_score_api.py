import threading

from flask import Flask
import werkzeug

from coinx.web.routes.api_data import api_data_bp


def create_test_client():
    if not hasattr(werkzeug, '__version__'):
        werkzeug.__version__ = '3'
    app = Flask(__name__)
    app.register_blueprint(api_data_bp)
    return app.test_client()


def test_market_structure_score_api_returns_snapshot(monkeypatch):
    captured = {}

    monkeypatch.setattr(
        'coinx.web.routes.api_data.get_market_structure_score_symbols',
        lambda: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'],
    )
    def fake_snapshot(symbols=None):
        captured['symbols'] = symbols
        return {
            'data': [{'symbol': 'BTCUSDT', 'total_score': 72.4}],
            'cache_update_time': 1711526400000,
            'summary': {'total_symbols': 1, 'strong_long_count': 1},
        }

    monkeypatch.setattr('coinx.web.routes.api_data.get_market_structure_score_snapshot', fake_snapshot)
    client = create_test_client()

    response = client.get('/api/market-structure-score?limit=1')

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['status'] == 'success'
    assert payload['data'][0]['symbol'] == 'BTCUSDT'
    assert payload['cache_update_time'] == 1711526400000


def test_market_structure_score_api_defaults_to_first_hundred(monkeypatch):
    captured = {}

    monkeypatch.setattr(
        'coinx.web.routes.api_data.get_market_structure_score_symbols',
        lambda: [f'SYM{i:03d}USDT' for i in range(100)],
    )
    def fake_snapshot(symbols=None):
        captured['symbols'] = symbols
        return {
            'data': [],
            'cache_update_time': None,
            'summary': {'total_symbols': 0},
        }

    monkeypatch.setattr('coinx.web.routes.api_data.get_market_structure_score_snapshot', fake_snapshot)
    client = create_test_client()

    response = client.get('/api/market-structure-score')

    assert response.status_code == 200
    assert len(captured['symbols']) == 100
    assert captured['symbols'][0] == 'SYM000USDT'
    assert captured['symbols'][-1] == 'SYM099USDT'


def test_market_structure_score_refresh_repairs_market_series_only(monkeypatch):
    captured = {'market': None, 'sentiment': 0}

    def fake_market_repair(**kwargs):
        captured['market'] = kwargs
        return {
            'status': 'success',
            'mode': 'history',
            'symbols': kwargs.get('symbols'),
            'series_types': kwargs.get('series_types'),
            'exchanges': kwargs.get('exchanges'),
            'success_count': 3,
            'failure_count': 0,
            'skipped_count': 0,
            'results': [
                {'component': 'market', 'status': 'success', 'affected': 2, 'records': 2},
                {'component': 'market', 'status': 'success', 'affected': 1, 'records': 1},
            ],
        }

    monkeypatch.setattr('coinx.web.routes.api_data.get_market_structure_score_symbols', lambda: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'])
    monkeypatch.setattr('coinx.web.routes.api_data.repair_rolling_tracked_symbols', fake_market_repair)
    client = create_test_client()

    response = client.post('/api/market-structure-score/refresh?wait=true')

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['status'] == 'success'
    assert captured['market']['symbols'] == ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']
    assert set(captured['market']['series_types']) == {'open_interest_hist', 'klines', 'taker_buy_sell_vol'}
    assert captured['market']['exchanges']
    assert set(payload['data']['series_types']) == {'open_interest_hist', 'klines', 'taker_buy_sell_vol'}
    assert payload['data']['success_count'] == 3
    assert len(payload['data']['components']) == 1
    assert payload['data']['components'][0]['component'] == 'market_series'
    assert payload['data']['stats']['affected'] == 3
    assert payload['data']['stats']['records'] == 3
    assert payload['data']['stats']['no_data_count'] == 0
    assert payload['data']['stats']['latest_event_time'] is None


def test_market_structure_score_refresh_waits_for_existing_run(monkeypatch):
    lock = threading.Lock()
    lock.acquire()

    monkeypatch.setattr('coinx.web.routes.api_data.MARKET_STRUCTURE_REFRESH_LOCK', lock)
    monkeypatch.setattr(
        'coinx.web.routes.api_data.MARKET_STRUCTURE_LAST_REFRESH_SUMMARY',
        {
            'status': 'success',
            'message': 'existing run finished',
            'results': [],
            'success_count': 1,
            'failure_count': 0,
            'skipped_count': 0,
        },
    )
    monkeypatch.setattr('coinx.web.routes.api_data.time.sleep', lambda seconds: lock.release())
    client = create_test_client()

    response = client.post('/api/market-structure-score/refresh?wait=true')

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['status'] == 'success'
    assert payload['message'] == 'existing run finished'
