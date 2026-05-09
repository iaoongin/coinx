from types import SimpleNamespace

import coinx.runtime as runtime


def test_start_runtime_services_starts_scheduler_and_bootstrap(monkeypatch):
    calls = []

    def fake_start_scheduler():
        calls.append('scheduler_started')

    def fake_startup_repair():
        calls.append('repair_started')

    class FakeThread:
        def __init__(self, target=None, daemon=None):
            self.target = target
            self.daemon = daemon
            self.started = False

        def start(self):
            self.started = True
            calls.append(self.target.__name__)
            self.target()

    monkeypatch.setattr(runtime, 'scheduler', SimpleNamespace(running=True))
    monkeypatch.setattr(runtime, 'get_active_coins', lambda: ['BTCUSDT'])
    monkeypatch.setattr(runtime, 'start_scheduler', fake_start_scheduler)
    monkeypatch.setattr(runtime, 'scheduled_repair_market_rolling', fake_startup_repair)
    monkeypatch.setattr(runtime.threading, 'Thread', FakeThread)
    monkeypatch.setattr(runtime.time, 'sleep', lambda seconds: None)

    result = runtime.start_runtime_services(with_startup_repair=True, startup_delay_seconds=1)

    assert result['tracked_coins'] == ['BTCUSDT']
    assert result['scheduler_thread'].started is True
    assert result['repair_thread'].started is True
    assert calls == ['fake_start_scheduler', 'scheduler_started', 'fake_startup_repair', 'repair_started']
