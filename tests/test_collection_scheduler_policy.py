from flask import Flask
import werkzeug
from types import SimpleNamespace

from coinx.web.routes.api_config import api_config_bp
from coinx.web.routes.api_data import api_data_bp
from coinx.web.routes.api_funding_rate import api_funding_rate_bp
from coinx.web.routes.api_rss import api_rss_bp


def create_test_client():
    if not hasattr(werkzeug, '__version__'):
        werkzeug.__version__ = '3'
    app = Flask(__name__)
    app.register_blueprint(api_config_bp)
    app.register_blueprint(api_data_bp)
    app.register_blueprint(api_funding_rate_bp)
    app.register_blueprint(api_rss_bp)
    return app.test_client()


def test_collection_endpoints_are_blocked_in_scheduler_only_mode(monkeypatch):
    for module in (
        'coinx.web.routes.api_data',
        'coinx.web.routes.api_funding_rate',
        'coinx.web.routes.api_config',
        'coinx.web.routes.api_rss',
    ):
        monkeypatch.setattr(f'{module}.COLLECTION_SCHEDULER_ONLY', True, raising=False)

    client = create_test_client()
    requests = [
        client.get('/api/update'),
        client.post('/api/market-structure-score/refresh'),
        client.post('/api/market-rank/refresh'),
        client.get('/api/funding-rate/refresh'),
        client.post('/api/coins-config/update-from-binance'),
        client.post('/api/rss/subscriptions/1/refresh', json={}),
    ]

    assert [response.status_code for response in requests] == [409] * len(requests)
    assert all(response.get_json()['code'] == 'COLLECTION_SCHEDULER_ONLY' for response in requests)


def test_homepage_read_does_not_start_repair_in_scheduler_only_mode(monkeypatch):
    monkeypatch.setattr('coinx.web.routes.api_data.COLLECTION_SCHEDULER_ONLY', True)
    monkeypatch.setattr('coinx.web.routes.api_data.HOMEPAGE_SERIES_REPAIR_ENABLED', True)
    monkeypatch.setattr('coinx.web.routes.api_data.get_active_coins', lambda: ['BTCUSDT'])
    monkeypatch.setattr('coinx.web.routes.api_data._get_homepage_cache_anchor', lambda *args, **kwargs: 1)
    monkeypatch.setattr(
        'coinx.web.routes.api_data.get_homepage_series_snapshot',
        lambda **kwargs: {'data': [], 'cache_update_time': None},
    )
    monkeypatch.setattr(
        'coinx.web.routes.api_data._start_homepage_refresh_async',
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('homepage read started collection')),
    )

    response = create_test_client().get('/api/coins?nocache=1')

    assert response.status_code == 200
    assert response.get_json()['homepage_complete'] is False


def test_collection_job_can_run_from_task_console_in_scheduler_only_mode(monkeypatch):
    monkeypatch.setattr('coinx.web.routes.api_data.COLLECTION_SCHEDULER_ONLY', True)
    monkeypatch.setattr('coinx.web.routes.api_data.SCHEDULER_ENABLED', False)
    job = SimpleNamespace(id='market_rank_refresh_job')
    monkeypatch.setattr(
        'coinx.web.routes.api_data.scheduler',
        SimpleNamespace(get_job=lambda job_id: job if job_id == job.id else None),
    )
    monkeypatch.setattr('coinx.web.routes.api_data._start_manual_task_job', lambda selected_job: True)
    monkeypatch.setattr('coinx.web.routes.api_data._list_scheduler_jobs', lambda: [])

    response = create_test_client().post(
        '/api/task-jobs/market_rank_refresh_job/action',
        json={'action': 'run'},
    )

    assert response.status_code == 200
    assert response.get_json()['status'] == 'success'
