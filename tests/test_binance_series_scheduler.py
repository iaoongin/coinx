from coinx.scheduler import scheduler


def test_binance_series_scheduler_only_registers_repair_job():
    assert scheduler.get_job('binance_series_job') is None
    assert scheduler.get_job('binance_series_repair_job') is not None
