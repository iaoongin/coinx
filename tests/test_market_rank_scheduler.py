import coinx.scheduler as scheduler_module


scheduler = scheduler_module.scheduler


def test_market_rank_scheduler_registers_even_when_global_switch_is_disabled():
    job = scheduler.get_job('market_rank_refresh_job')

    assert job is not None
    assert job.trigger.__class__.__name__.lower().startswith('interval')


def test_scheduled_job_registers_when_scheduler_disabled(monkeypatch):
    monkeypatch.setattr(scheduler_module, 'SCHEDULER_ENABLED', False)

    @scheduler_module.scheduled_job('interval', seconds=60, id='disabled_scheduler_test_job')
    def disabled_scheduler_test_job():
        return None

    assert scheduler.get_job('disabled_scheduler_test_job') is not None
    assert disabled_scheduler_test_job() is None
