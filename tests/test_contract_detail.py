from flask import Flask
import werkzeug

from coinx.models import MarketFundingRate, MarketKline, MarketOpenInterestHist, MarketTakerBuySellVol
from coinx.repositories.contract_detail import get_contract_detail, get_contract_structure_score, load_contract_chart_series
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
            'exchange_statuses': [
                {
                    'exchange': 'binance',
                    'status': 'included',
                    'open_interest': 600.0,
                    'open_interest_formatted': '600.00',
                    'open_interest_value': 41400000.0,
                    'open_interest_value_formatted': '$41.40M',
                    'share_percent': 60.0,
                },
                {
                    'exchange': 'okx',
                    'status': 'excluded',
                    'open_interest': 400.0,
                    'open_interest_formatted': '400.00',
                    'open_interest_value': 27600000.0,
                    'open_interest_value_formatted': '$27.60M',
                    'share_percent': None,
                },
            ],
            'net_inflow': {'5m': 10.0, '1h': 100.0},
            'net_inflow_value': {'5m': 690000.0, '1h': 6900000.0},
            'net_inflow_value_formatted': {'5m': '$690.00K', '1h': '$6.90M'},
            'changes': {
                '5m': {'ratio': 1.0, 'value_ratio': 1.5, 'open_interest': 990.0, 'open_interest_formatted': '990.00', 'open_interest_value': 67965000.0, 'open_interest_value_formatted': '$67.97M', 'price_change': 100.0, 'price_change_percent': 0.5, 'current_price': 68900.0, 'current_price_formatted': '68,900.00'},
                '1h': {'ratio': 4.0, 'value_ratio': 6.0, 'open_interest': 960.0, 'open_interest_formatted': '960.00', 'open_interest_value': 65000000.0, 'open_interest_value_formatted': '$65.00M', 'price_change': 1200.0, 'price_change_percent': 2.0, 'current_price': 67800.0, 'current_price_formatted': '67,800.00'},
            },
        }],
    }


def test_contract_detail_combines_existing_snapshots():
    result = get_contract_detail(
        'btcusdt',
        homepage_loader=lambda symbols: _homepage_snapshot(),
        funding_loader=lambda symbols: {'BTCUSDT': {'event_time': 1711526400000, 'funding_rate': 0.0001}},
    )

    assert result['symbol'] == 'BTCUSDT'
    assert result['data_status'] == 'complete'
    assert result['summary']['latest_price'] == 69000.0
    assert result['summary']['quote_volume_24h'] is None
    assert result['intervals'][3]['interval'] == '1h'
    assert result['intervals'][3]['net_inflow_value'] == 6900000.0
    assert result['intervals'][3]['price_change'] == 1200.0
    assert result['intervals'][3]['open_interest_change'] == 40.0
    assert result['intervals'][3]['open_interest_value_change'] == 4000000.0
    assert result['intervals'][3]['current_price_formatted'] == '67,800.00'
    assert result['intervals'][3]['open_interest_formatted'] == '960.00'
    assert result['intervals'][3]['open_interest_value_formatted'] == '$65.00M'
    assert [item['exchange'] for item in result['exchange_distribution']] == ['binance', 'okx']
    assert result['exchange_distribution'][1]['status'] == 'excluded'
    assert result['exchange_distribution'][0]['snapshot_share_percent'] == 60.0
    assert result['exchange_distribution'][1]['snapshot_share_percent'] == 40.0


def test_contract_detail_preserves_partial_data_without_score():
    result = get_contract_detail(
        'BTCUSDT',
        homepage_loader=lambda symbols: _homepage_snapshot(status='partial'),
        funding_loader=lambda symbols: {},
    )

    assert result['data_status'] == 'partial'
    assert result['missing_exchanges'] == ['okx']


def test_contract_structure_score_is_loaded_independently():
    result = get_contract_structure_score('btcusdt', score_loader=lambda symbols: {
        'cache_update_time': 1711526400000,
        'data': [{'symbol': 'BTCUSDT', 'total_score': 72.4}],
    })
    assert result['symbol'] == 'BTCUSDT'
    assert result['structure_score']['total_score'] == 72.4


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


def test_contract_chart_series_aggregates_exchanges(db_session):
    timestamp = 1711526400000
    db_session.add_all([
        MarketKline(exchange='binance', symbol='BTCUSDT', period='5m', open_time=timestamp, close_time=timestamp + 299999, open_price=100, high_price=102, low_price=99, close_price=101, volume=100),
        MarketKline(exchange='bybit', symbol='BTCUSDT', period='5m', open_time=timestamp, close_time=timestamp + 299999, open_price=99, high_price=102, low_price=98, close_price=100, volume=200),
        MarketOpenInterestHist(exchange='binance', symbol='BTCUSDT', period='5m', event_time=timestamp, sum_open_interest=10, sum_open_interest_value=1010),
        MarketOpenInterestHist(exchange='bybit', symbol='BTCUSDT', period='5m', event_time=timestamp, sum_open_interest=20, sum_open_interest_value=2000),
        MarketTakerBuySellVol(exchange='binance', symbol='BTCUSDT', period='5m', event_time=timestamp, buy_vol=8, sell_vol=3),
        MarketTakerBuySellVol(exchange='bybit', symbol='BTCUSDT', period='5m', event_time=timestamp, buy_vol=4, sell_vol=6),
        MarketFundingRate(exchange='binance', symbol='BTCUSDT', period='5m', event_time=timestamp, funding_rate=.0001, predicted_rate=.0002),
    ])
    db_session.commit()

    result = load_contract_chart_series('BTCUSDT', range_key='1h', session=db_session)

    assert result['market'][0]['price'] == 101.0
    assert result['market'][0]['volume'] == 300.0
    assert result['market'][0]['open_interest_value'] == 3010.0
    assert result['market'][0]['open_interest'] == 30.0
    assert result['flow'][0]['buy_volume'] == 12.0
    assert result['flow'][0]['net_inflow'] == 3.0
    assert result['funding_rate'][0]['predicted_rate'] == .0002


def test_coin_detail_series_api_validates_range(monkeypatch):
    response = _client().get('/api/coin-detail/BTCUSDT/series?range=30d')
    assert response.status_code == 400

    monkeypatch.setattr('coinx.web.routes.api_data.load_contract_chart_series', lambda symbol, range_key: {'range': range_key, 'market': []})
    response = _client().get('/api/coin-detail/BTCUSDT/series?range=4h')
    assert response.status_code == 200
    assert response.get_json()['data']['range'] == '4h'


def test_coin_detail_structure_score_api(monkeypatch):
    monkeypatch.setattr('coinx.web.routes.api_data.get_contract_structure_score', lambda symbol: {'symbol': symbol, 'structure_score': {'total_score': 66}})
    response = _client().get('/api/coin-detail/BTCUSDT/structure-score')
    assert response.status_code == 200
    assert response.get_json()['data']['structure_score']['total_score'] == 66
