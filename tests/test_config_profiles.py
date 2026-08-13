import os

from coinx.config import _load_env_profiles


def test_env_profile_merges_base_and_profile_without_overriding_process(tmp_path, monkeypatch):
    (tmp_path / '.env').write_text(
        'BASE_ONLY=base\nSHARED=base\nPROCESS_VALUE=from_file\n',
        encoding='utf-8',
    )
    (tmp_path / '.env.prod').write_text(
        'SHARED=prod\nPROFILE_ONLY=prod\n',
        encoding='utf-8',
    )
    monkeypatch.setenv('PROCESS_VALUE', 'from_process')
    monkeypatch.delenv('COINX_ENV', raising=False)
    monkeypatch.delenv('BASE_ONLY', raising=False)
    monkeypatch.delenv('SHARED', raising=False)
    monkeypatch.delenv('PROFILE_ONLY', raising=False)

    assert _load_env_profiles(str(tmp_path), 'prod') == 'prod'
    assert os.environ['BASE_ONLY'] == 'base'
    assert os.environ['SHARED'] == 'prod'
    assert os.environ['PROFILE_ONLY'] == 'prod'
    assert os.environ['PROCESS_VALUE'] == 'from_process'


def test_env_profile_rejects_unsafe_name(tmp_path):
    try:
        _load_env_profiles(str(tmp_path), '../prod')
    except ValueError as exc:
        assert 'COINX_ENV' in str(exc)
    else:
        raise AssertionError('unsafe profile name was accepted')
