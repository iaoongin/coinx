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
    monkeypatch.setattr('coinx.web.routes.api_data._start_homepage_refresh_async', lambda *args, **kwargs: False)
    client = create_test_client()

    response = client.get('/api/coins?wait=true')

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
    assert captured['kwargs']['series_types'] == ['klines', 'open_interest_hist', 'taker_buy_sell_vol']
    assert captured['started'] is True


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
    monkeypatch.setattr('coinx.web.routes.api_data.repair_tracked_symbols', fake_repair)
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
    assert payload['data']['success_count'] == 1
    assert calls == [
        {
            'symbols': ['BTCUSDT'],
            'series_types': ['klines', 'open_interest_hist', 'taker_buy_sell_vol'],
        }
    ]


def test_get_coins_returns_complete_interval_contract(db_session, monkeypatch):
    seed_complete_homepage_series(db_session)

    monkeypatch.setattr('coinx.web.routes.api_data.get_active_coins', lambda: ['BTCUSDT'])
    monkeypatch.setattr(
        'coinx.web.routes.api_data.get_homepage_series_snapshot',
        lambda symbols: repository_get_homepage_series_snapshot(symbols=symbols, session=db_session),
    )

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

    monkeypatch.setattr('coinx.web.routes.api_data.repair_tracked_symbols', fake_repair)
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
    coin = payload['data'][0]
    assert coin['symbol'] == 'BTCUSDT'
    assert all(change['interval'] != '168h' for change in coin['changes'])
    assert '168h' not in coin['net_inflow']


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

    monkeypatch.setattr('coinx.web.routes.api_data.repair_tracked_symbols', fake_repair)

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
