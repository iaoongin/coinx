import importlib.util
import sys
from pathlib import Path


def _load_module():
    path = Path("scripts/benchmark_dual_backend_api.py")
    spec = importlib.util.spec_from_file_location("coinx_benchmark_test", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_percentile_is_deterministic_for_small_samples():
    module = _load_module()
    assert module.percentile([1, 2, 3, 4], 0.50) == 2.5
    assert module.percentile([], 0.95) == 0.0


def test_compare_benchmarks_enforces_p95_and_error_rate():
    module = _load_module()
    mysql = {
        "cases": {
            "endpoint": {"p95_ms": 100.0, "error_rate": 0.0},
            "broken": {"p95_ms": 100.0, "error_rate": 0.1},
        }
    }
    clickhouse = {
        "cases": {
            "endpoint": {"p95_ms": 149.0, "error_rate": 0.0},
            "broken": {"p95_ms": 10.0, "error_rate": 0.0},
        }
    }

    comparisons, failures = module.compare_benchmarks(mysql, clickhouse, 1.5)

    assert {item["label"] for item in comparisons if item["ok"]} == {"endpoint"}
    assert len(failures) == 1
    assert "broken" in failures[0]
