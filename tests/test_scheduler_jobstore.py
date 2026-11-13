import logging

import pytest
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler

import coinx.scheduler as scheduler_module


def scheduler_jobstore_probe():
    return None


def test_sqlalchemy_jobstore_persists_pause_across_restart(tmp_path):
    database_url = f'sqlite:///{tmp_path / "scheduler.db"}'

    first = BackgroundScheduler(jobstores={'default': SQLAlchemyJobStore(url=database_url)})
    first.add_job(scheduler_jobstore_probe, 'interval', seconds=60, id='persistent-job')
    first.start()
    first.pause_job('persistent-job')
    first.shutdown(wait=False)

    second = BackgroundScheduler(jobstores={'default': SQLAlchemyJobStore(url=database_url)})
    second.start(paused=True)
    try:
        job = second.get_job('persistent-job')
        assert job is not None
        assert job.next_run_time is None

        second.resume_job('persistent-job')
        assert second.get_job('persistent-job').next_run_time is not None
    finally:
        second.shutdown(wait=False)


def test_persisted_pause_ids_are_loaded_before_pending_registration(monkeypatch, tmp_path):
    database_url = f'sqlite:///{tmp_path / "scheduler.db"}'
    jobstore = SQLAlchemyJobStore(url=database_url)
    scheduler = BackgroundScheduler(jobstores={'default': jobstore})
    scheduler.add_job(scheduler_jobstore_probe, 'interval', seconds=60, id='paused-job')
    scheduler.start()
    scheduler.pause_job('paused-job')
    scheduler.shutdown(wait=False)

    monkeypatch.setattr(scheduler_module, 'scheduler_jobstore', SQLAlchemyJobStore(url=database_url))

    assert scheduler_module._get_persisted_paused_job_ids() == {'paused-job'}


def test_smart_money_job_is_registered_with_fifteen_minute_interval():
    job = scheduler_module.scheduler.get_job('collect_smart_money_signals_job')

    assert job is not None
    assert job.max_instances == 1
    assert job.coalesce is True
    assert job.trigger.interval.total_seconds() == 900


def test_data_cleanup_job_is_registered_at_twenty_past_midnight():
    job = scheduler_module.scheduler.get_job('cleanup_data_job')

    assert job is not None
    assert str(job.trigger).startswith('cron[')
    assert job.trigger.fields[6].expressions[0].first == 20


def test_obsolete_cleanup_jobs_are_removed(monkeypatch):
    removed = []

    class FakeScheduler:
        def get_job(self, job_id):
            return object() if job_id in {'cleanup_market_data_job', 'cleanup_task_run_history_job'} else None

        def remove_job(self, job_id):
            removed.append(job_id)

    monkeypatch.setattr(scheduler_module, 'scheduler', FakeScheduler())

    scheduler_module._remove_obsolete_cleanup_jobs()

    assert removed == ['cleanup_market_data_job', 'cleanup_task_run_history_job']


def test_data_cleanup_runs_all_scopes_with_readable_summary(monkeypatch, caplog):
    finished = {}
    calls = []

    monkeypatch.setattr(scheduler_module, 'MARKET_DATA_RETENTION_DAYS', 15)
    monkeypatch.setattr(scheduler_module, 'SMART_MONEY_RETENTION_DAYS', 15)
    monkeypatch.setattr(scheduler_module, 'TASK_RUN_HISTORY_RETENTION_DAYS', 15)
    monkeypatch.setattr(scheduler_module, 'optimize_cleaned_tables', lambda *_args, **_kwargs: [])
    monkeypatch.setattr(scheduler_module, '_mark_job_started', lambda job_id: calls.append(('start', job_id)))
    monkeypatch.setattr(
        scheduler_module,
        '_mark_job_finished',
        lambda job_id, **kwargs: finished.update({'job_id': job_id, **kwargs}),
    )
    monkeypatch.setattr(
        scheduler_module,
        'cleanup_expired_market_data',
        lambda days, progress_callback=None: calls.append(('market', days)) or {'market_klines': 12, 'market_funding_rate': 3},
    )
    monkeypatch.setattr(
        scheduler_module,
        'cleanup_expired_smart_money_data',
        lambda days, progress_callback=None: calls.append(('smart_money', days)) or {'market_futures_smart_money_signal': 4},
    )
    monkeypatch.setattr(
        scheduler_module,
        'delete_expired_job_runs',
        lambda days: calls.append(('run_logs', days)) or 5,
    )
    caplog.set_level(logging.INFO)

    scheduler_module.scheduled_cleanup_data()

    assert calls == [
        ('start', 'cleanup_data_job'),
        ('market', 15),
        ('smart_money', 15),
        ('run_logs', 15),
    ]
    assert finished['job_id'] == 'cleanup_data_job'
    assert finished['status'] == 'success'
    assert finished['summary']['message'] == '数据清理完成'
    assert finished['summary']['total_deleted_count'] == 24
    assert [scope['label'] for scope in finished['summary']['cleanup_scopes']] == [
        '行情', '聪明钱', '运行日志',
    ]
    messages = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith('[数据清理]')
    ]
    assert messages[0].startswith('[数据清理] 开始 | ')
    assert messages[-1].startswith('[数据清理] 完成 | ')


def test_data_cleanup_skips_optimization_when_a_scope_fails(monkeypatch):
    finished = {}
    monkeypatch.setattr(scheduler_module, '_mark_job_started', lambda *_args: None)
    monkeypatch.setattr(scheduler_module, '_mark_job_progress', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(scheduler_module, '_mark_job_finished', lambda _job, **kwargs: finished.update(kwargs))
    monkeypatch.setattr(scheduler_module, 'cleanup_expired_market_data', lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError('failed')))
    monkeypatch.setattr(scheduler_module, 'cleanup_expired_smart_money_data', lambda *_args, **_kwargs: {'market_futures_smart_money_signal': 0})
    monkeypatch.setattr(scheduler_module, 'delete_expired_job_runs', lambda *_args: 0)
    monkeypatch.setattr(scheduler_module, 'optimize_cleaned_tables', lambda *_args, **_kwargs: pytest.fail('must skip optimization'))

    scheduler_module.scheduled_cleanup_data()

    assert finished['status'] == 'partial'
    assert finished['summary']['optimize_results'] == []


def test_smart_money_job_collects_all_configured_windows_in_one_snapshot(monkeypatch):
    calls = []
    finished = {}

    def fake_collect(**kwargs):
        calls.append(kwargs)
        return {
            'status': 'success',
            'time_range': kwargs['time_range'],
            'list_rows': 2,
            'list_written': 2,
        }

    monkeypatch.setattr(scheduler_module, 'SMART_MONEY_TIME_RANGES', ['30m', 'ALL'])
    monkeypatch.setattr(scheduler_module, 'align_snapshot_time', lambda: 1_700_000_000_000)
    monkeypatch.setattr(scheduler_module, 'collect_and_save_smart_money_signals', fake_collect)
    monkeypatch.setattr(scheduler_module, '_mark_job_started', lambda job_id: None)
    monkeypatch.setattr(
        scheduler_module,
        '_mark_job_finished',
        lambda job_id, **kwargs: finished.update({'job_id': job_id, **kwargs}),
    )

    scheduler_module.scheduled_collect_smart_money_signals()

    assert [call['time_range'] for call in calls] == ['30m', 'ALL']
    assert {call['snapshot_time'] for call in calls} == {1_700_000_000_000}
    assert finished['status'] == 'success'
    assert finished['summary']['list_rows'] == 4
