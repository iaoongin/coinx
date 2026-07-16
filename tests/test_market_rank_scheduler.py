import coinx.scheduler as scheduler_module


scheduler = scheduler_module.scheduler


def test_market_rank_scheduler_registration_follows_global_switch():
    job = scheduler.get_job('market_rank_refresh_job')

    if scheduler_module.SCHEDULER_ENABLED:
        assert job is not None
        assert job.trigger.__class__.__name__.lower().startswith('interval')
    else:
        assert job is None


def test_scheduled_job_does_not_register_when_scheduler_disabled(monkeypatch):
    monkeypatch.setattr(scheduler_module, 'SCHEDULER_ENABLED', False)

    @scheduler_module.scheduled_job('interval', seconds=60, id='disabled_scheduler_test_job')
    def disabled_scheduler_test_job():
        return None

    assert scheduler.get_job('disabled_scheduler_test_job') is None
    assert disabled_scheduler_test_job() is None
