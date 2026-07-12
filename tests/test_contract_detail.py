from flask import Flask
import werkzeug

from coinx.repositories.contract_detail import get_contract_detail
from coinx.web.routes.api_data import api_data_bp


def _homepage_snapshot(status='complete'):
    return {
        'cache_update_time': 1711526400000,
        'data': [{
            'symbol': 'BTCUSDT',
            'status': status,
            'included_exchanges': ['binance', 'bybit'],
            'missing_exchanges': ['okx'] if status == 'partial' else [],
            'current_price': 69000.0,
            'current_price_formatted': '69,000.00',
            'price_change_percent': 2.5,
            'current_open_interest': 1000.0,
            'current_open_interest_formatted': '1,000.00',
            'current_open_interest_value': 69000000.0,
            'current_open_interest_value_formatted': '$69.00M',
            'funding_rate': 0.0001,
            'funding_rate_formatted': '0.0100%',
            'predicted_funding_rate': 0.0002,
            'predicted_funding_rate_formatted': '0.0200%',
            'next_funding_time': 1711530000000,
            'next_funding_time_formatted': '1h',
            'exchange_open_interest': [{
                'exchange': 'binance',
                'open_interest': 600.0,
                'open_interest_formatted': '600.00',
                'open_interest_value': 41400000.0,
                'open_interest_value_formatted': '$41.40M',
                'share_percent': 60.0,
            }],
            'net_inflow': {'5m': 10.0, '1h': 100.0},
            'net_inflow_value': {'5m': 690000.0, '1h': 6900000.0},
            'net_inflow_value_formatted': {'5m': '$690.00K', '1h': '$6.90M'},
            'changes': {
                '5m': {'ratio': 1.0, 'value_ratio': 1.5, 'price_change_percent': 0.5},
                '1h': {'ratio': 4.0, 'value_ratio': 6.0, 'price_change_percent': 2.0},
            },
        }],
    }


def test_contract_detail_combines_existing_snapshots():
    result = get_contract_detail(
        'btcusdt',
        homepage_loader=lambda symbols: _homepage_snapshot(),
        score_loader=lambda symbols: {
            'cache_update_time': 1711526400000,
            'data': [{'symbol': 'BTCUSDT', 'total_score': 72.4, 'trade_signal': 'long'}],
        },
        funding_loader=lambda symbols: {'BTCUSDT': {'event_time': 1711526400000, 'funding_rate': 0.0001}},
    )

    assert result['symbol'] == 'BTCUSDT'
    assert result['data_status'] == 'complete'
    assert result['summary']['latest_price'] == 69000.0
    assert result['summary']['quote_volume_24h'] is None
    assert result['intervals'][3]['interval'] == '1h'
    assert result['intervals'][3]['net_inflow_value'] == 6900000.0
    assert result['exchange_distribution'][0]['exchange'] == 'binance'
    assert result['structure_score']['total_score'] == 72.4


def test_contract_detail_preserves_partial_data_without_score():
    result = get_contract_detail(
        'BTCUSDT',
        homepage_loader=lambda symbols: _homepage_snapshot(status='partial'),
        score_loader=lambda symbols: {'data': [], 'cache_update_time': None},
        funding_loader=lambda symbols: {},
    )

    assert result['data_status'] == 'partial'
    assert result['missing_exchanges'] == ['okx']
    assert result['structure_score'] is None


def _client():
    if not hasattr(werkzeug, '__version__'):
        werkzeug.__version__ = '3'
    app = Flask(__name__)
    app.register_blueprint(api_data_bp)
    return app.test_client()


def test_coin_detail_api_rejects_invalid_symbol():
    response = _client().get('/api/coin-detail/BTC%20USDT')
    assert response.status_code == 400


def test_coin_detail_api_returns_404_when_no_stored_data(monkeypatch):
    monkeypatch.setattr('coinx.web.routes.api_data.get_contract_detail', lambda symbol: None)
    response = _client().get('/api/coin-detail/UNKNOWNUSDT')
    assert response.status_code == 404


def test_coin_detail_api_returns_repository_payload(monkeypatch):
    monkeypatch.setattr(
        'coinx.web.routes.api_data.get_contract_detail',
        lambda symbol: {'symbol': symbol, 'data_status': 'complete'},
    )
    response = _client().get('/api/coin-detail/btcusdt')
    assert response.status_code == 200
    assert response.get_json()['data']['symbol'] == 'BTCUSDT'
