import pytest

from coinx.config import get_env, resolve_market_backends


def test_get_env_converts_typed_default_values(monkeypatch):
    monkeypatch.delenv('COINX_TEST_INTERVALS', raising=False)
    monkeypatch.delenv('COINX_TEST_ENABLED', raising=False)

    assert get_env('COINX_TEST_INTERVALS', '5m,15m', list) == ['5m', '15m']
    assert get_env('COINX_TEST_ENABLED', False, bool) is False


def test_market_backend_selects_both_directions():
    assert resolve_market_backends("clickhouse") == (
        "clickhouse",
        "clickhouse",
        "clickhouse",
    )
    assert resolve_market_backends("mysql") == ("mysql", "mysql", "mysql")


def test_legacy_backend_variables_can_override_one_direction():
    assert resolve_market_backends(
        "clickhouse",
        read_backend="mysql",
    ) == ("clickhouse", "mysql", "clickhouse")
    assert resolve_market_backends(
        "clickhouse",
        market_write_backend="mysql",
    ) == ("clickhouse", "clickhouse", "mysql")


def test_missing_market_backend_defaults_to_mysql():
    assert resolve_market_backends() == ("mysql", "mysql", "mysql")


@pytest.mark.parametrize(
    ("kwargs", "variable"),
    [
        ({"market_backend": "postgres"}, "MARKET_BACKEND"),
        ({"read_backend": "postgres"}, "READ_BACKEND"),
        ({"market_write_backend": "postgres"}, "MARKET_WRITE_BACKEND"),
    ],
)
def test_invalid_backend_is_rejected(kwargs, variable):
    with pytest.raises(ValueError, match=variable):
        resolve_market_backends(**kwargs)
