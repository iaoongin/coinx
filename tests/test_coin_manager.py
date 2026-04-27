from types import SimpleNamespace

import coinx.coin_manager as coin_manager


class _FakeQuery:
    def __init__(self, rows):
        self._rows = rows

    def filter(self, *args, **kwargs):
        return self

    def all(self):
        return self._rows


def test_load_coins_config_bootstraps_defaults_when_database_is_empty(monkeypatch):
    added = []

    monkeypatch.setattr(coin_manager.os.path, 'exists', lambda path: False)
    monkeypatch.setattr(coin_manager, 'add_coin', lambda symbol, tracked=True: added.append((symbol, tracked)))
    monkeypatch.setattr(coin_manager.db_session, 'query', lambda model: _FakeQuery([]))

    coins = coin_manager.load_coins_config()

    assert coins == coin_manager.DEFAULT_TRACKED_COINS
    assert added == [(symbol, True) for symbol in coin_manager.DEFAULT_TRACKED_COINS]


def test_load_coins_config_falls_back_to_defaults_when_no_coins_are_tracked(monkeypatch):
    added = []

    monkeypatch.setattr(coin_manager.os.path, 'exists', lambda path: True)
    monkeypatch.setattr(coin_manager, 'migrate_from_file', lambda: None)
    monkeypatch.setattr(coin_manager, 'add_coin', lambda symbol, tracked=True: added.append((symbol, tracked)))
    monkeypatch.setattr(coin_manager.db_session, 'query', lambda model: _FakeQuery([]))

    coins = coin_manager.load_coins_config()

    assert coins == coin_manager.DEFAULT_TRACKED_COINS
    assert added == []
