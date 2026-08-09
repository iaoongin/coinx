import importlib.util
import sys
from pathlib import Path


def _load_module():
    path = Path("scripts/verify_dual_backend_api.py")
    spec = importlib.util.spec_from_file_location("coinx_dual_backend_verify_test", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_json_diffs_compares_complete_nested_payloads():
    module = _load_module()
    assert module.json_diffs({"data": [{"value": 1.0}]}, {"data": [{"value": 1.0}]}) == []
    differences = module.json_diffs(
        {"data": [{"value": 1.0}], "missing": True},
        {"data": [{"value": 2.0}], "extra": True},
    )
    assert any("$.data[0].value" in item for item in differences)
    assert "$.missing: missing_in_clickhouse" in differences
    assert "$.extra: missing_in_mysql" in differences


def test_json_diffs_allows_only_documented_numeric_rounding_noise():
    module = _load_module()
    assert module.json_diffs({"value": 1.0}, {"value": 1.0 + 1e-10}) == []
    assert module.json_diffs({"value": 1.0}, {"value": 1.0 + 1e-5})


def test_with_as_of_preserves_existing_query_parameters():
    module = _load_module()
    result = module.with_as_of("/api/coins?nocache=1", 1785593400000)
    assert "nocache=1" in result
    assert "as_of_ms=1785593400000" in result


def test_named_instance_paths_are_safe():
    module = _load_module()
    assert module.parse_host_port("10.0.0.128:13306") == ("10.0.0.128", 13306)
    assert module.parse_host_port("mysql") == ("mysql", 3306)
