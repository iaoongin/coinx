import importlib.util
from pathlib import Path
from types import SimpleNamespace


def _load_start_app_module():
    path = Path('scripts/start_app.py')
    spec = importlib.util.spec_from_file_location('coinx_start_app_test', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_app_command_matches_relative_main_path():
    module = _load_start_app_module()
    manager = module.FlaskAppManager()

    assert manager._is_app_command(['python', 'src/coinx/main.py'])
    assert manager._is_app_command(['python', '-m', 'coinx.main'])
    assert manager._is_app_command(['python', 'scripts/start_app.py', 'run'])
    assert not manager._is_app_command(['python', 'scripts/start_app.py', 'run'], allow_manager_run=False)
    assert not manager._is_app_command(['python', 'scripts/start_app.py', 'stop'])
    assert not manager._is_app_command(['python', 'scripts/start_app.py', 'status'])


def test_listener_lookup_only_returns_recognized_coinx_process(monkeypatch):
    module = _load_start_app_module()
    manager = module.FlaskAppManager()
    current_pid = module.os.getpid()
    monkeypatch.setattr(
        module.psutil,
        'net_connections',
        lambda kind: [
            SimpleNamespace(status=module.psutil.CONN_LISTEN, laddr=SimpleNamespace(port=module.WEB_PORT), pid=101),
            SimpleNamespace(status=module.psutil.CONN_LISTEN, laddr=SimpleNamespace(port=module.WEB_PORT), pid=102),
            SimpleNamespace(status=module.psutil.CONN_LISTEN, laddr=SimpleNamespace(port=module.WEB_PORT), pid=current_pid),
        ],
    )

    processes = {
        101: SimpleNamespace(name=lambda: 'python.exe', cmdline=lambda: ['python', 'src/coinx/main.py']),
        102: SimpleNamespace(name=lambda: 'other.exe', cmdline=lambda: ['other', 'server']),
    }
    monkeypatch.setattr(module.psutil, 'Process', lambda pid: processes[pid])

    assert manager._find_app_listener_processes() == [101]
