from flask import Flask
import werkzeug

from coinx.config import BINANCE_SERIES_TYPES
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


def test_market_structure_score_refresh_repairs_market_and_sentiment_series(monkeypatch):
    captured = {'market': None, 'sentiment': None}

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

    def fake_sentiment_repair(**kwargs):
        captured['sentiment'] = kwargs
        return {
            'status': 'success',
            'mode': 'rolling',
            'symbols': kwargs.get('symbols'),
            'series_types': kwargs.get('series_types'),
            'success_count': 1,
            'failure_count': 0,
            'skipped_count': 8,
            'results': [
                {
                    'symbol': 'BTCUSDT',
                    'series_type': 'top_long_short_position_ratio',
                    'status': 'success',
                    'affected': 1,
                    'records': 1,
                    'latest_event_time': 1711526400000,
                },
                {
                    'symbol': 'ETHUSDT',
                    'series_type': 'top_long_short_account_ratio',
                    'status': 'skipped',
                    'reason': 'no_data',
                    'affected': 0,
                    'records': 0,
                    'latest_event_time': None,
                },
            ],
        }

    monkeypatch.setattr('coinx.web.routes.api_data.get_market_structure_score_symbols', lambda: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'])
    monkeypatch.setattr('coinx.web.routes.api_data.repair_rolling_tracked_symbols', fake_market_repair)
    monkeypatch.setattr('coinx.web.routes.api_data.repair_binance_rolling_tracked_symbols', fake_sentiment_repair)
    client = create_test_client()

    response = client.post('/api/market-structure-score/refresh?wait=true')

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['status'] == 'success'
    assert captured['market']['symbols'] == ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']
    assert captured['sentiment']['symbols'] == ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']
    expected_series_types = BINANCE_SERIES_TYPES.split(',') if isinstance(BINANCE_SERIES_TYPES, str) else list(BINANCE_SERIES_TYPES)
    assert payload['data']['series_types'] == expected_series_types
    assert captured['market']['series_types'] == ['open_interest_hist', 'klines', 'taker_buy_sell_vol']
    assert captured['sentiment']['series_types'] == [
        'top_long_short_position_ratio',
        'top_long_short_account_ratio',
        'global_long_short_account_ratio',
    ]
    assert captured['market']['exchanges']
    assert payload['data']['success_count'] == 4
    assert len(payload['data']['components']) == 2
    assert payload['data']['stats']['affected'] == 4
    assert payload['data']['stats']['records'] == 4
    assert payload['data']['stats']['no_data_count'] == 1
    assert payload['data']['stats']['latest_event_time'] == 1711526400000
    sentiment_component = payload['data']['components'][1]
    assert sentiment_component['component'] == 'binance_sentiment_series'
    assert sentiment_component['stats']['affected'] == 1
    assert sentiment_component['stats']['records'] == 1
    assert sentiment_component['stats']['no_data_count'] == 1
    assert sentiment_component['stats']['latest_event_time'] == 1711526400000
