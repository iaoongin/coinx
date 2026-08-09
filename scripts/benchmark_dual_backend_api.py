#!/usr/bin/env python3
"""Measure MySQL/ClickHouse API latency and enforce the migration p95 gate.

The benchmark is read-only. It warms both application instances, runs every
replay endpoint repeatedly, records the complete request samples, and compares
ClickHouse p95 against the MySQL baseline. Use ``--start-local`` to launch the
two process-level backends from the current checkout.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests


ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from verify_dual_backend_api import (  # noqa: E402
    API_CASES,
    LocalProcess,
    choose_auto_as_of,
    request_json,
    start_instance,
    wait_for_health,
    with_as_of,
)


DEFAULT_ITERATIONS = 10
DEFAULT_TIMEOUT_SECONDS = 240.0
DEFAULT_P95_FACTOR = 1.5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mysql-host", default=os.getenv("COINX_MYSQL_TEST_HOST", "127.0.0.1:3306"))
    parser.add_argument("--mysql-database", default=os.getenv("COINX_MYSQL_TEST_DATABASE", "coinx"))
    parser.add_argument("--mysql-user", default=os.getenv("COINX_MYSQL_TEST_USER", "root"))
    parser.add_argument("--mysql-password", default=os.getenv("COINX_MYSQL_TEST_PASSWORD"))
    parser.add_argument("--clickhouse-url", default=os.getenv("COINX_CK_TEST_URL"))
    parser.add_argument("--clickhouse-database", default=os.getenv("COINX_CK_TEST_DATABASE", "coinx"))
    parser.add_argument("--clickhouse-user", default=os.getenv("COINX_CK_TEST_USER", "default"))
    parser.add_argument("--clickhouse-password", default=os.getenv("COINX_CK_TEST_PASSWORD", ""))
    parser.add_argument("--mysql-api-url", default=os.getenv("COINX_MYSQL_API_URL", "http://127.0.0.1:5500"))
    parser.add_argument("--clickhouse-api-url", default=os.getenv("COINX_CLICKHOUSE_API_URL", "http://127.0.0.1:5501"))
    parser.add_argument("--start-local", action="store_true")
    parser.add_argument("--keep-apps", action="store_true")
    parser.add_argument("--as-of-ms", type=int)
    parser.add_argument("--auto-as-of", action="store_true")
    parser.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS)
    parser.add_argument("--http-timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--startup-timeout", type=float, default=90.0)
    parser.add_argument("--p95-factor", type=float, default=DEFAULT_P95_FACTOR)
    parser.add_argument("--report-file", default=os.getenv("COINX_BENCHMARK_REPORT_FILE", "data/clickhouse-dual-backend-benchmark.json"))
    args = parser.parse_args()
    if not args.mysql_password:
        parser.error("--mysql-password or COINX_MYSQL_TEST_PASSWORD is required")
    if not args.clickhouse_url:
        parser.error("--clickhouse-url or COINX_CK_TEST_URL is required")
    if args.as_of_ms is None and not args.auto_as_of:
        parser.error("--as-of-ms or --auto-as-of is required")
    if args.as_of_ms is not None and args.auto_as_of:
        parser.error("--as-of-ms cannot be combined with --auto-as-of")
    if args.iterations < 10:
        parser.error("--iterations must be at least 10")
    if args.http_timeout <= 0 or args.startup_timeout <= 0 or args.p95_factor <= 0:
        parser.error("timeouts and --p95-factor must be greater than zero")
    return args


def percentile(samples: Sequence[float], fraction: float) -> float:
    if not samples:
        return 0.0
    values = sorted(float(value) for value in samples)
    index = (len(values) - 1) * fraction
    lower = int(index)
    upper = min(lower + 1, len(values) - 1)
    weight = index - lower
    return values[lower] + (values[upper] - values[lower]) * weight


def disk_bytes(clickhouse_url: str, database: str, user: str, password: str) -> Optional[int]:
    """Read active ClickHouse part bytes without mutating the server."""
    try:
        sys.path.insert(0, str(ROOT / "src"))
        from coinx.read_clients import ClickHouseReadClient

        with ClickHouseReadClient(clickhouse_url, database, user, password) as client:
            value = client.query_scalar(
                "SELECT sum(bytes_on_disk) FROM system.parts "
                f"WHERE active AND database = '{database.replace(chr(39), chr(39) * 2)}'"
            )
        return int(value or 0)
    except Exception as exc:
        print(f"[benchmark] disk size unavailable: {exc!r}", flush=True)
        return None


def benchmark_backend(
    name: str,
    base_url: str,
    as_of_ms: int,
    iterations: int,
    timeout: float,
) -> Dict[str, Any]:
    session = requests.Session()
    results: Dict[str, Any] = {"backend": name, "base_url": base_url, "cases": {}}
    try:
        for label, raw_path in API_CASES:
            path = with_as_of(raw_path, as_of_ms)
            # Warm the route and its process-local caches before timed samples.
            try:
                warm = request_json(session, base_url, path, timeout)
            except Exception as exc:
                warm = {"status_code": None, "payload": None, "error": repr(exc)}
            samples: List[Dict[str, Any]] = []
            for index in range(iterations):
                try:
                    result = request_json(session, base_url, path, timeout)
                    payload = result.get("payload")
                    ok = (
                        result.get("status_code") == 200
                        and isinstance(payload, dict)
                        and payload.get("status") == "success"
                    )
                    samples.append({"iteration": index + 1, "ok": ok, **result})
                except Exception as exc:
                    samples.append({"iteration": index + 1, "ok": False, "error": repr(exc)})
                print(
                    f"[{name}] {label} {index + 1}/{iterations} "
                    f"{'OK' if samples[-1].get('ok') else 'FAIL'} "
                    f"{samples[-1].get('elapsed_ms', '-')} ms",
                    flush=True,
                )
            durations = [float(item["elapsed_ms"]) for item in samples if item.get("elapsed_ms") is not None]
            successes = sum(1 for item in samples if item.get("ok"))
            case = {
                "path": path,
                "warmup": warm,
                "iterations": iterations,
                "samples": samples,
                "request_count": len(samples),
                "success_count": successes,
                "error_count": len(samples) - successes,
                "error_rate": (len(samples) - successes) / len(samples) if samples else 1.0,
                "p50_ms": round(percentile(durations, 0.50), 2),
                "p95_ms": round(percentile(durations, 0.95), 2),
                "max_ms": round(max(durations), 2) if durations else None,
                "mean_ms": round(statistics.fmean(durations), 2) if durations else None,
            }
            results["cases"][label] = case
    finally:
        session.close()
    return results


def compare_benchmarks(mysql: Dict[str, Any], clickhouse: Dict[str, Any], factor: float) -> Tuple[List[Dict[str, Any]], List[str]]:
    comparisons: List[Dict[str, Any]] = []
    failures: List[str] = []
    labels = sorted(set(mysql.get("cases", {})) | set(clickhouse.get("cases", {})))
    for label in labels:
        left = mysql.get("cases", {}).get(label, {})
        right = clickhouse.get("cases", {}).get(label, {})
        mysql_p95 = left.get("p95_ms")
        clickhouse_p95 = right.get("p95_ms")
        mysql_error = left.get("error_rate", 1.0)
        clickhouse_error = right.get("error_rate", 1.0)
        ratio = None
        p95_ok = False
        if mysql_p95 is not None and clickhouse_p95 is not None:
            ratio = clickhouse_p95 / max(float(mysql_p95), 0.001)
            p95_ok = float(clickhouse_p95) <= max(float(mysql_p95) * factor, 1.0)
        ok = p95_ok and mysql_error == 0 and clickhouse_error == 0
        item = {
            "label": label,
            "ok": ok,
            "mysql_p95_ms": mysql_p95,
            "clickhouse_p95_ms": clickhouse_p95,
            "p95_ratio": round(ratio, 3) if ratio is not None else None,
            "p95_factor": factor,
            "mysql_error_rate": mysql_error,
            "clickhouse_error_rate": clickhouse_error,
        }
        comparisons.append(item)
        if not ok:
            failures.append(f"{label}: p95/error gate failed: {item}")
    return comparisons, failures


def markdown_report(report: Dict[str, Any]) -> str:
    metadata = report["metadata"]
    summary = report["summary"]
    lines = [
        "# ClickHouse Dual Backend Benchmark",
        "",
        f"Generated: `{metadata['generated_at']}`",
        f"Replay timestamp: `{metadata['as_of_ms']}`",
        f"Iterations per endpoint: `{metadata['iterations']}`",
        f"P95 gate factor: `{metadata['p95_factor']}`",
        "",
        "## Summary",
        "",
        f"- Cases: {summary['cases']}",
        f"- Failed cases: {summary['failed_cases']}",
        f"- ClickHouse disk before/after/delta: {metadata.get('clickhouse_disk_before_bytes')} / {metadata.get('clickhouse_disk_after_bytes')} / {metadata.get('clickhouse_disk_delta_bytes')} bytes",
        f"- Overall gate: **{'PASS' if summary['gate_passed'] else 'FAIL'}**",
        "",
        "## p95 Comparison",
        "",
        "| Endpoint | MySQL p95 (ms) | ClickHouse p95 (ms) | Ratio | Result |",
        "|---|---:|---:|---:|---:|",
    ]
    for item in report["comparisons"]:
        lines.append(
            f"| `{item['label']}` | {item.get('mysql_p95_ms', '-')} | "
            f"{item.get('clickhouse_p95_ms', '-')} | {item.get('p95_ratio', '-')} | "
            f"{'PASS' if item['ok'] else 'FAIL'} |"
        )
    lines.extend(["", "## Failures", ""])
    lines.extend(f"- {failure}" for failure in report.get("failures", [])) or lines.append("- None")
    lines.append("")
    return "\n".join(lines)


def write_report(report: Dict[str, Any], report_file: str) -> Tuple[Path, Path]:
    path = Path(report_file)
    if not path.is_absolute():
        path = ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except PermissionError:
        path = Path(tempfile.gettempdir()) / path.name
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown = path.with_suffix(".md")
    markdown.write_text(markdown_report(report), encoding="utf-8")
    return path, markdown


def main() -> int:
    args = parse_args()
    time_selection: Dict[str, Any] = {}
    if args.auto_as_of:
        # Reuse the verifier's read-only common-bound selection.
        time_selection = choose_auto_as_of(args)
        args.as_of_ms = int(time_selection["as_of_ms"])
    local_processes: List[LocalProcess] = []
    failures: List[str] = []
    try:
        if args.start_local:
            local_processes = [
                start_instance(args, "mysql", "mysql", 5500),
                start_instance(args, "clickhouse", "clickhouse", 5501),
            ]
            wait_for_health(args.mysql_api_url, args.startup_timeout, "mysql")
            wait_for_health(args.clickhouse_api_url, args.startup_timeout, "clickhouse")
        else:
            wait_for_health(args.mysql_api_url, min(args.startup_timeout, 15.0), "mysql")
            wait_for_health(args.clickhouse_api_url, min(args.startup_timeout, 15.0), "clickhouse")

        disk_before = disk_bytes(args.clickhouse_url, args.clickhouse_database, args.clickhouse_user, args.clickhouse_password)
        mysql_result = benchmark_backend("mysql", args.mysql_api_url, args.as_of_ms, args.iterations, args.http_timeout)
        clickhouse_result = benchmark_backend("clickhouse", args.clickhouse_api_url, args.as_of_ms, args.iterations, args.http_timeout)
        disk_after = disk_bytes(args.clickhouse_url, args.clickhouse_database, args.clickhouse_user, args.clickhouse_password)
        comparisons, compare_failures = compare_benchmarks(mysql_result, clickhouse_result, args.p95_factor)
        failures.extend(compare_failures)
        if disk_before is None or disk_after is None:
            failures.append("ClickHouse disk size could not be measured; resource gate is inconclusive")
        summary = {
            "cases": len(comparisons),
            "failed_cases": sum(1 for item in comparisons if not item["ok"]),
            "gate_passed": not failures,
        }
        report = {
            "metadata": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "as_of_ms": args.as_of_ms,
                "as_of_mode": "auto" if args.auto_as_of else "manual",
                "common_upper_ms": time_selection.get("common_upper_ms"),
                "iterations": args.iterations,
                "http_timeout": args.http_timeout,
                "p95_factor": args.p95_factor,
                "mysql_api_url": args.mysql_api_url,
                "clickhouse_api_url": args.clickhouse_api_url,
                "mysql_target": f"{args.mysql_host}/{args.mysql_database}",
                "clickhouse_target": f"{args.clickhouse_url}/{args.clickhouse_database}",
                "clickhouse_disk_before_bytes": disk_before,
                "clickhouse_disk_after_bytes": disk_after,
                "clickhouse_disk_delta_bytes": (disk_after - disk_before) if disk_before is not None and disk_after is not None else None,
            },
            "backends": {"mysql": mysql_result, "clickhouse": clickhouse_result},
            "comparisons": comparisons,
            "failures": failures,
            "summary": summary,
        }
        json_path, markdown_path = write_report(report, args.report_file)
        print(f"JSON report: {json_path}", flush=True)
        print(f"Markdown report: {markdown_path}", flush=True)
        print(f"Overall gate: {'PASS' if summary['gate_passed'] else 'FAIL'}", flush=True)
        return 0 if summary["gate_passed"] else 1
    finally:
        if args.start_local and not args.keep_apps:
            for process in reversed(local_processes):
                process.stop()


if __name__ == "__main__":
    raise SystemExit(main())
