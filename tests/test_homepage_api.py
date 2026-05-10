from flask import Flask
import werkzeug

from coinx.repositories.homepage_series import get_homepage_series_snapshot as repository_get_homepage_series_snapshot
from homepage_contracts import assert_complete_interval_contract, seed_complete_homepage_series
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
                    'included_exchanges': ['binance'],
                    'missing_exchanges': [],
                    'status': 'complete',
                    'current_open_interest': 100.0,
                    'current_open_interest_formatted': '100.00',
                    'current_open_interest_value': 200.0,
                    'current_open_interest_value_formatted': '200.00',
                    'exchange_open_interest': [
                        {
                            'exchange': 'binance',
                            'open_interest': 60.0,
                            'open_interest_formatted': '60.00',
                            'open_interest_value': 120.0,
                            'open_interest_value_formatted': '$120.00',
                            'share_percent': 60.0,
                            'quantity_share_percent': 60.0,
                        }
                    ],
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
    monkeypatch.setattr('coinx.web.routes.api_data._start_homepage_refresh_async', lambda *args, **kwargs: False)
    client = create_test_client()

    response = client.get('/api/coins?wait=true')

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['status'] == 'success'
    assert payload['cache_update_time'] == 1234567890000
    assert payload['data'][0]['symbol'] == 'BTCUSDT'
    assert payload['data'][0]['exchange_open_interest'][0]['exchange'] == 'binance'
    assert payload['data'][0]['changes'][0]['interval'] == '5m'


def test_get_coins_reuses_snapshot_cache_within_same_anchor(monkeypatch):
    module = __import__('coinx.web.routes.api_data', fromlist=['_clear_homepage_snapshot_cache'])
    module._clear_homepage_snapshot_cache()
    calls = {'snapshot': 0}

    def fake_snapshot(symbols):
        calls['snapshot'] += 1
        return {
            'data': [
                {
                    'symbol': 'BTCUSDT',
                    'included_exchanges': ['binance'],
                    'missing_exchanges': [],
                    'status': 'complete',
                    'current_open_interest': 100.0,
                    'current_open_interest_formatted': '100.00',
                    'current_open_interest_value': 200.0,
                    'current_open_interest_value_formatted': '200.00',
                    'exchange_open_interest': [],
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
        }

    monkeypatch.setattr('coinx.web.routes.api_data.get_active_coins', lambda: ['BTCUSDT'])
    monkeypatch.setattr('coinx.web.routes.api_data._get_homepage_cache_anchor', lambda: 123)
    monkeypatch.setattr('coinx.web.routes.api_data.get_homepage_series_snapshot', fake_snapshot)
    monkeypatch.setattr('coinx.web.routes.api_data._start_homepage_refresh_async', lambda *args, **kwargs: False)
    client = create_test_client()

    first = client.get('/api/coins?wait=true')
    second = client.get('/api/coins?wait=true')

    assert first.status_code == 200
    assert second.status_code == 200
    assert calls['snapshot'] == 1
    module._clear_homepage_snapshot_cache()


def test_get_coins_cache_expires_when_anchor_changes(monkeypatch):
    module = __import__('coinx.web.routes.api_data', fromlist=['_clear_homepage_snapshot_cache'])
    module._clear_homepage_snapshot_cache()
    anchors = [123, 456]
    calls = {'snapshot': 0}

    def fake_snapshot(symbols):
        calls['snapshot'] += 1
        return {
            'data': [
                {
                    'symbol': 'BTCUSDT',
                    'included_exchanges': ['binance'],
                    'missing_exchanges': [],
                    'status': 'complete',
                    'current_open_interest': 100.0,
                    'current_open_interest_formatted': '100.00',
                    'current_open_interest_value': 200.0,
                    'current_open_interest_value_formatted': '200.00',
                    'exchange_open_interest': [],
                    'current_price': 2.0,
                    'current_price_formatted': '2.00',
                    'price_change': 1.0,
                    'price_change_percent': 100.0,
                    'price_change_formatted': '1.00',
                    'net_inflow': {'5m': 12.0},
                    'changes': {'5m': {'ratio': 5.0, 'value_ratio': 6.0, 'open_interest': 95.0, 'open_interest_formatted': '95.00', 'open_interest_value': 188.0, 'open_interest_value_formatted': '188.00', 'price_change': 0.1, 'price_change_percent': 5.0, 'price_change_formatted': '0.10', 'current_price': 1.9, 'current_price_formatted': '1.90'}},
                }
            ],
            'cache_update_time': 1234567890000,
        }

    monkeypatch.setattr('coinx.web.routes.api_data.get_active_coins', lambda: ['BTCUSDT'])
    monkeypatch.setattr('coinx.web.routes.api_data._get_homepage_cache_anchor', lambda: anchors.pop(0))
    monkeypatch.setattr('coinx.web.routes.api_data.get_homepage_series_snapshot', fake_snapshot)
    monkeypatch.setattr('coinx.web.routes.api_data._start_homepage_refresh_async', lambda *args, **kwargs: False)
    client = create_test_client()

    assert client.get('/api/coins?wait=true').status_code == 200
    assert client.get('/api/coins?wait=true').status_code == 200
    assert calls['snapshot'] == 2
    module._clear_homepage_snapshot_cache()


def test_update_data_uses_homepage_series_refresh(monkeypatch):
    captured = {}

    monkeypatch.setattr('coinx.web.routes.api_data.get_active_coins', lambda: ['BTCUSDT'])
    monkeypatch.setattr('coinx.web.routes.api_data.should_refresh_homepage_series', lambda symbols: True)
    monkeypatch.setattr(
        'coinx.web.routes.api_data._start_homepage_refresh_async',
        lambda symbols=None, series_types=None, latest_only=False: captured.update(
            {
                'symbols': symbols,
                'series_types': series_types,
                'latest_only': latest_only,
            }
        ) or True,
    )
    client = create_test_client()

    response = client.get('/api/update')

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['status'] == 'success'
    assert captured['symbols'] == ['BTCUSDT']
    assert captured['series_types'] == ['klines', 'open_interest_hist', 'taker_buy_sell_vol']
    assert captured['latest_only'] is False
    assert payload['message'] == 'homepage rolling repair triggered'


def test_update_data_can_wait_for_homepage_series_refresh(monkeypatch):
    calls = []
    import threading

    def fake_repair(**kwargs):
        calls.append(kwargs)
        return {
            'status': 'success',
            'symbols': kwargs['symbols'],
            'series_types': kwargs['series_types'],
            'period': '5m',
            'success_count': 1,
            'failure_count': 0,
            'skipped_count': 0,
            'results': [],
        }

    monkeypatch.setattr('coinx.web.routes.api_data.get_active_coins', lambda: ['BTCUSDT'])
    monkeypatch.setattr('coinx.web.routes.api_data.should_refresh_homepage_series', lambda symbols: True)
    monkeypatch.setattr('coinx.web.routes.api_data.HOME_PAGE_REFRESH_LOCK', threading.Lock())
    monkeypatch.setattr('coinx.web.routes.api_data.repair_rolling_tracked_symbols', fake_repair)
    monkeypatch.setattr(
        'coinx.web.routes.api_data.threading.Thread',
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('should not create thread when wait=true')),
    )

    client = create_test_client()

    response = client.get('/api/update?force=true&wait=true')

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['status'] == 'success'
    assert payload['message'] == 'homepage series refresh completed'
    assert calls[0]['points'] == 5


def test_update_data_waits_for_existing_homepage_refresh(monkeypatch):
    import threading

    lock = threading.Lock()
    lock.acquire()

    monkeypatch.setattr('coinx.web.routes.api_data.get_active_coins', lambda: ['BTCUSDT'])
    monkeypatch.setattr('coinx.web.routes.api_data.HOME_PAGE_REFRESH_LOCK', lock)
    monkeypatch.setattr(
        'coinx.web.routes.api_data.HOME_PAGE_LAST_REFRESH_SUMMARY',
        {
            'status': 'success',
            'message': 'existing homepage run finished',
            'results': [],
            'success_count': 1,
            'failure_count': 0,
            'skipped_count': 0,
        },
    )
    monkeypatch.setattr('coinx.web.routes.api_data.time.sleep', lambda seconds: lock.release())
    client = create_test_client()

    response = client.get('/api/update?force=true&wait=true')

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['status'] == 'success'
    assert payload['message'] == 'existing homepage run finished'
    assert payload['data']['success_count'] == 1


def test_get_coins_returns_complete_interval_contract(db_session, monkeypatch):
    seed_complete_homepage_series(db_session)

    monkeypatch.setattr('coinx.repositories.homepage_series.ENABLED_EXCHANGES', ['binance'])
    monkeypatch.setattr('coinx.web.routes.api_data.get_active_coins', lambda: ['BTCUSDT'])
    monkeypatch.setattr(
        'coinx.web.routes.api_data.get_homepage_series_snapshot',
        lambda symbols: repository_get_homepage_series_snapshot(symbols=symbols, session=db_session),
    )
    monkeypatch.setattr('coinx.web.routes.api_data._start_homepage_refresh_async', lambda *args, **kwargs: False)

    client = create_test_client()

    response = client.get('/api/coins?wait=true')

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['status'] == 'success'
    assert payload['cache_update_time'] is not None
    assert len(payload['data']) == 1
    assert_complete_interval_contract(payload['data'][0])


def test_get_coins_treats_partial_net_inflow_as_complete_when_changes_are_full(monkeypatch):
    def build_coin():
        changes = {
            interval: {
                'ratio': 1.0,
                'value_ratio': 1.0,
                'open_interest': 100.0,
                'open_interest_formatted': '100.00',
                'open_interest_value': 200.0,
                'open_interest_value_formatted': '200.00',
                'price_change': 1.0,
                'price_change_percent': 1.0,
                'price_change_formatted': '1.00',
                'current_price': 10.0,
                'current_price_formatted': '10.00',
            }
            for interval in ['5m', '15m', '30m', '1h', '4h', '12h', '24h', '48h', '72h', '168h']
        }

        return {
            'symbol': 'BTCUSDT',
            'included_exchanges': ['binance'],
            'missing_exchanges': [],
            'status': 'complete',
            'current_open_interest': 100.0,
            'current_open_interest_formatted': '100.00',
            'current_open_interest_value': 200.0,
            'current_open_interest_value_formatted': '200.00',
            'current_price': 10.0,
            'current_price_formatted': '10.00',
            'price_change': 1.0,
            'price_change_percent': 1.0,
            'price_change_formatted': '1.00',
            'net_inflow': {'5m': 12.0},
            'changes': changes,
        }

    monkeypatch.setattr('coinx.web.routes.api_data.get_active_coins', lambda: ['BTCUSDT'])
    monkeypatch.setattr('coinx.web.routes.api_data.get_homepage_series_snapshot', lambda symbols: {'data': [build_coin()], 'cache_update_time': 1234567890000})
    monkeypatch.setattr(
        'coinx.web.routes.api_data._start_homepage_refresh_async',
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('should not refresh when changes are complete')),
    )

    client = create_test_client()

    response = client.get('/api/coins?wait=true')

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['status'] == 'success'
    assert payload['homepage_complete'] is True
    assert payload['data'][0]['changes'][0]['interval'] == '5m'


def test_get_coins_triggers_background_repair_when_homepage_series_is_incomplete(monkeypatch):
    state = {'repaired': False}
    score_refresh = {}

    def build_coin():
        base_changes = {
            '5m': {
                'ratio': 1.0,
                'value_ratio': 1.0,
                'open_interest': 100.0,
                'open_interest_formatted': '100.00',
                'open_interest_value': 200.0,
                'open_interest_value_formatted': '200.00',
                'price_change': 1.0,
                'price_change_percent': 1.0,
                'price_change_formatted': '1.00',
                'current_price': 10.0,
                'current_price_formatted': '10.00',
            }
        }
        if state['repaired']:
            base_changes['168h'] = {
                'ratio': 8.0,
                'value_ratio': 9.0,
                'open_interest': 120.0,
                'open_interest_formatted': '120.00',
                'open_interest_value': 240.0,
                'open_interest_value_formatted': '240.00',
                'price_change': 8.0,
                'price_change_percent': 8.0,
                'price_change_formatted': '8.00',
                'current_price': 12.0,
                'current_price_formatted': '12.00',
            }

        net_inflow = {'5m': 12.0}
        if state['repaired']:
            net_inflow['168h'] = 128.0

        return {
            'symbol': 'BTCUSDT',
            'included_exchanges': ['binance'],
            'missing_exchanges': [],
            'status': 'complete' if state['repaired'] else 'partial',
            'current_open_interest': 100.0,
            'current_open_interest_formatted': '100.00',
            'current_open_interest_value': 200.0,
            'current_open_interest_value_formatted': '200.00',
            'current_price': 10.0,
            'current_price_formatted': '10.00',
            'price_change': 1.0,
            'price_change_percent': 1.0,
            'price_change_formatted': '1.00',
            'net_inflow': net_inflow,
            'changes': base_changes,
        }

    monkeypatch.setattr('coinx.web.routes.api_data.get_active_coins', lambda: ['BTCUSDT'])
    monkeypatch.setattr('coinx.web.routes.api_data.get_market_structure_score_symbols', lambda: ['BTCUSDT', 'ETHUSDT'])
    monkeypatch.setattr('coinx.web.routes.api_data.should_refresh_homepage_series', lambda symbols: not state['repaired'])

    def fake_repair(**kwargs):
        state['repaired'] = True
        return {
            'status': 'success',
            'symbols': kwargs['symbols'],
            'series_types': kwargs['series_types'],
            'period': '5m',
            'success_count': 1,
            'failure_count': 0,
            'skipped_count': 0,
            'results': [],
        }

    monkeypatch.setattr('coinx.web.routes.api_data.repair_rolling_tracked_symbols', fake_repair)
    monkeypatch.setattr(
        'coinx.web.routes.api_data._run_market_structure_refresh',
        lambda **kwargs: score_refresh.update(kwargs) or {'status': 'success', 'results': []},
    )
    started = {}

    class FakeThread:
        def __init__(self, target=None, kwargs=None, daemon=None):
            self.target = target
            self.kwargs = kwargs or {}
            self.daemon = daemon

        def start(self):
            started['called'] = True
            self.target(**self.kwargs)

    monkeypatch.setattr('coinx.web.routes.api_data.threading.Thread', FakeThread)
    monkeypatch.setattr(
        'coinx.web.routes.api_data.get_homepage_series_snapshot',
        lambda symbols: {'data': [build_coin()], 'cache_update_time': 1234567890000},
    )

    client = create_test_client()

    response = client.get('/api/coins?wait=true')

    assert response.status_code == 200
    payload = response.get_json()
    assert started['called'] is True
    assert state['repaired'] is True
    assert score_refresh['symbols'] == ['ETHUSDT']
    assert set(score_refresh['series_types']) == {'taker_buy_sell_vol', 'klines', 'open_interest_hist'}
    coin = payload['data'][0]
    assert coin['symbol'] == 'BTCUSDT'
    assert all(change['interval'] != '168h' for change in coin['changes'])
    assert '168h' not in coin['net_inflow']


def test_get_coins_deduplicates_market_structure_refresh_symbols_against_homepage_refresh(monkeypatch):
    started = {}
    score_refresh = {}

    monkeypatch.setattr('coinx.web.routes.api_data.get_active_coins', lambda: ['BTCUSDT', 'ETHUSDT'])
    monkeypatch.setattr('coinx.web.routes.api_data.get_market_structure_score_symbols', lambda: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'])
    monkeypatch.setattr(
        'coinx.web.routes.api_data.get_homepage_series_snapshot',
        lambda symbols: {
            'data': [
                {
                    'symbol': 'BTCUSDT',
                    'included_exchanges': ['binance'],
                    'missing_exchanges': [],
                    'status': 'partial',
                    'current_open_interest': 100.0,
                    'current_open_interest_formatted': '100.00',
                    'current_open_interest_value': 200.0,
                    'current_open_interest_value_formatted': '200.00',
                    'current_price': 10.0,
                    'current_price_formatted': '10.00',
                    'price_change': 1.0,
                    'price_change_percent': 1.0,
                    'price_change_formatted': '1.00',
                    'net_inflow': {'5m': 12.0},
                    'changes': {
                        '5m': {
                            'ratio': 1.0,
                            'value_ratio': 1.0,
                            'open_interest': 100.0,
                            'open_interest_formatted': '100.00',
                            'open_interest_value': 200.0,
                            'open_interest_value_formatted': '200.00',
                            'price_change': 1.0,
                            'price_change_percent': 1.0,
                            'price_change_formatted': '1.00',
                            'current_price': 10.0,
                            'current_price_formatted': '10.00',
                        }
                    },
                }
            ],
            'cache_update_time': 1234567890000,
        },
    )
    monkeypatch.setattr(
        'coinx.web.routes.api_data._start_homepage_refresh_async',
        lambda symbols=None, series_types=None, latest_only=False: started.update(
            {'symbols': symbols, 'series_types': series_types, 'latest_only': latest_only}
        ) or True,
    )
    monkeypatch.setattr(
        'coinx.web.routes.api_data._start_market_structure_refresh_async',
        lambda symbols=None, series_types=None, exchanges=None: score_refresh.update(
            {'symbols': symbols, 'series_types': series_types, 'exchanges': exchanges}
        ) or True,
    )

    client = create_test_client()

    response = client.get('/api/coins?wait=true')

    assert response.status_code == 200
    assert started['symbols'] == ['BTCUSDT', 'ETHUSDT']
    assert score_refresh['symbols'] == ['SOLUSDT']
    assert set(score_refresh['series_types']) == {'taker_buy_sell_vol', 'klines', 'open_interest_hist'}


def test_homepage_refresh_skips_duplicate_inflight_requests(monkeypatch):
    import threading

    started = threading.Event()
    release = threading.Event()
    calls = []

    def fake_repair(**kwargs):
        calls.append(kwargs)
        started.set()
        release.wait(timeout=2)
        return {
            'status': 'success',
            'symbols': kwargs['symbols'],
            'series_types': kwargs['series_types'],
            'period': '5m',
            'success_count': 1,
            'failure_count': 0,
            'skipped_count': 0,
            'results': [],
        }

    monkeypatch.setattr('coinx.web.routes.api_data.repair_rolling_tracked_symbols', fake_repair)

    thread = threading.Thread(
        target=lambda: __import__('coinx.web.routes.api_data', fromlist=['_run_homepage_refresh'])._run_homepage_refresh(
            ['BTCUSDT'],
            ['klines', 'open_interest_hist', 'taker_buy_sell_vol'],
        )
    )
    thread.start()
    assert started.wait(timeout=2) is True

    module = __import__('coinx.web.routes.api_data', fromlist=['_run_homepage_refresh'])
    second_result = module._run_homepage_refresh(
        ['BTCUSDT'],
        ['klines', 'open_interest_hist', 'taker_buy_sell_vol'],
    )

    release.set()
    thread.join(timeout=2)

    assert second_result['status'] == 'skipped'
    assert len(calls) == 1
