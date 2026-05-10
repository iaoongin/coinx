from types import SimpleNamespace
from datetime import datetime, timedelta

from flask import Flask
import werkzeug

from coinx.web.routes.api_data import api_data_bp


def create_test_client():
    if not hasattr(werkzeug, '__version__'):
        werkzeug.__version__ = '3'
    app = Flask(__name__)
    app.register_blueprint(api_data_bp)
    return app.test_client()


class FakeJob:
    def __init__(self, job_id='job-a', next_run_time=None):
        self.id = job_id
        self.name = 'Fake Job'
        self.trigger = 'interval[0:05:00]'
        self.executor = 'default'
        self.max_instances = 1
        self.coalesce = True
        self.misfire_grace_time = None
        self.next_run_time = next_run_time
        self.paused = False
        self.resumed = False
        self.modified = False

    def modify(self, **kwargs):
        self.modified = True

    def pause(self):
        self.paused = True
        self.next_run_time = None

    def resume(self):
        self.resumed = True
        self.next_run_time = None


def test_list_task_jobs_returns_scheduler_snapshot(monkeypatch):
    fake_job = FakeJob(next_run_time=datetime.now() + timedelta(minutes=5))
    monkeypatch.setattr('coinx.web.routes.api_data.scheduler', SimpleNamespace(
        running=True,
        get_jobs=lambda: [fake_job],
        modify_job=lambda job_id, **kwargs: None,
        get_job=lambda job_id: fake_job if job_id == fake_job.id else None,
    ))
    monkeypatch.setattr(
        'coinx.web.routes.api_data.get_all_job_runtime_metadata',
        lambda: {'job-a': {'running': False, 'last_status': 'success'}},
    )

    client = create_test_client()
    response = client.get('/api/task-jobs')

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['status'] == 'success'
    assert payload['data']['scheduler_running'] is True
    assert payload['data']['jobs'][0]['id'] == 'job-a'
    assert payload['data']['jobs'][0]['registered'] is True
    assert payload['data']['jobs'][0]['paused'] is False


def test_control_task_job_runs_job(monkeypatch):
    fake_job = FakeJob()
    wakeup_calls = []
    modify_calls = []
    monkeypatch.setattr('coinx.web.routes.api_data.scheduler', SimpleNamespace(
        running=True,
        get_jobs=lambda: [fake_job],
        get_job=lambda job_id: fake_job if job_id == fake_job.id else None,
        modify_job=lambda job_id, **kwargs: modify_calls.append((job_id, kwargs)),
        wakeup=lambda: wakeup_calls.append('wakeup'),
    ))
    monkeypatch.setattr('coinx.web.routes.api_data.get_all_job_runtime_metadata', lambda: {})

    client = create_test_client()
    response = client.post('/api/task-jobs/job-a/action', json={'action': 'run'})

    assert response.status_code == 200
    payload = response.get_json()
    assert payload['status'] == 'success'
    assert modify_calls
    assert wakeup_calls == ['wakeup']


def test_control_task_job_rejects_unsupported_action():
    client = create_test_client()
    response = client.post('/api/task-jobs/job-a/action', json={'action': 'remove'})

    assert response.status_code == 400
    payload = response.get_json()
    assert payload['status'] == 'error'
