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
