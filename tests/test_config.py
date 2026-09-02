import pytest

from coinx.config import (
    MAX_TIME_INTERVAL_HOURS,
    REPAIR_HISTORY_COVERAGE_HOURS,
    build_time_interval_specs,
    get_env,
    interval_to_ms,
    resolve_market_backends,
)


def test_get_env_converts_typed_default_values(monkeypatch):
    monkeypatch.delenv('COINX_TEST_INTERVALS', raising=False)
    monkeypatch.delenv('COINX_TEST_ENABLED', raising=False)

    assert get_env('COINX_TEST_INTERVALS', '5m,15m', list) == ['5m', '15m']
    assert get_env('COINX_TEST_ENABLED', False, bool) is False


def test_time_interval_specs_convert_windows_to_base_points():
    assert interval_to_ms('8h') == 8 * 60 * 60 * 1000
    assert build_time_interval_specs(['5m', '8h', '24h']) == (
        ('5m', 5 * 60 * 1000, 1),
        ('8h', 8 * 60 * 60 * 1000, 96),
        ('24h', 24 * 60 * 60 * 1000, 288),
    )


def test_time_interval_specs_deduplicate_while_preserving_order():
    specs = build_time_interval_specs(['8h', '5m', '8h', '1h'])

    assert [interval for interval, _duration_ms, _points in specs] == ['8h', '5m', '1h']


def test_history_coverage_covers_largest_configured_window():
    assert REPAIR_HISTORY_COVERAGE_HOURS >= MAX_TIME_INTERVAL_HOURS


@pytest.mark.parametrize('interval', ['4m', '7m', '0h', 'invalid'])
def test_time_interval_specs_reject_invalid_windows(interval):
    with pytest.raises(ValueError):
        build_time_interval_specs([interval])


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
