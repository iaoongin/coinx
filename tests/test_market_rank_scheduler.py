from coinx.scheduler import scheduler


def test_market_rank_scheduler_registers_refresh_job():
    job = scheduler.get_job('market_rank_refresh_job')

    assert job is not None
    assert job.trigger.__class__.__name__.lower().startswith('interval')
