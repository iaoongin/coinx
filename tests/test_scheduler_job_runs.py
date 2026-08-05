from types import SimpleNamespace

import coinx.scheduler as scheduler_module


def test_job_metadata_persists_successful_lifecycle(monkeypatch):
    created = []
    completed = []
    monkeypatch.setattr(scheduler_module, 'JOB_METADATA', {})
    monkeypatch.setattr(
        scheduler_module,
        'create_job_run',
        lambda job_id, started_at: created.append((job_id, started_at)) or 7,
    )
    monkeypatch.setattr(
        scheduler_module,
        'finish_job_run',
        lambda run_id, **kwargs: completed.append((run_id, kwargs)) or True,
    )
    monkeypatch.setattr('coinx.notifications.evaluate_scheduled_rules', lambda *_args, **_kwargs: None)

    scheduler_module._mark_job_started('job-a')
    scheduler_module._mark_job_finished('job-a', status='success', summary={'count': 3}, started_at=0)

    assert created[0][0] == 'job-a'
    assert completed[0][0] == 7
    assert completed[0][1]['status'] == 'success'
    assert completed[0][1]['summary'] == {'count': 3}


def test_job_run_persistence_failure_does_not_block_metadata(monkeypatch):
    monkeypatch.setattr(scheduler_module, 'JOB_METADATA', {})
    monkeypatch.setattr(scheduler_module, 'create_job_run', lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError('db down')))

    metadata = scheduler_module._mark_job_started('job-a')

    assert metadata['running'] is True
    assert metadata['run_id'] is None


def test_initialize_job_run_history_recovers_interrupted_runs(monkeypatch):
    calls = []
    monkeypatch.setattr(scheduler_module, 'ensure_job_run_schema', lambda: calls.append('schema'))
    monkeypatch.setattr(scheduler_module, 'recover_interrupted_job_runs', lambda: calls.append('recover') or 1)

    scheduler_module.initialize_job_run_history()

    assert calls == ['schema', 'recover']


def test_trade_opportunity_notification_uses_scheduled_event(monkeypatch):
    calls = []
    monkeypatch.setattr(
        'coinx.notifications.evaluate_scheduled_rules',
        lambda event_type: calls.append(event_type) or {'status': 'success'},
    )

    scheduler_module._evaluate_market_notifications('trade_opportunity')

    assert calls == ['market.trade_opportunity.actionable']
