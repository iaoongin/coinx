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
