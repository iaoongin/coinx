#!/usr/bin/env python3
"""Run an auditable MySQL/ClickHouse API parity check.

The verifier talks to two real HTTP application instances.  It does not use
Flask's test client or a temporary database, so a PASS proves the deployed
process-level ``READ_BACKEND`` switch and the cache behavior users will see.
Both instances are read-only from this script's point of view.  The only POST
requests are sent to the ClickHouse instance to verify that its application
guard returns 503 before any write handler runs.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl

import requests


ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
DEFAULT_AS_OF_FLOOR_MS = 300_000
DEFAULT_TIMEOUT_SECONDS = 180.0
NUMERIC_ABS_TOLERANCE = 1e-8
NUMERIC_REL_TOLERANCE = 1e-12

API_CASES: Tuple[Tuple[str, str], ...] = (
    ("funding page", "/api/funding-rate?page=1&page_size=20"),
    ("abnormal funding", "/api/funding-rate/abnormal"),
    ("funding history BTCUSDT", "/api/funding-rate/history/BTCUSDT?hours=24"),
    ("funding history ETHUSDT", "/api/funding-rate/history/ETHUSDT?hours=24"),
    ("market rank", "/api/market-rank?type=price_change&direction=down&limit=20"),
    ("homepage cold bypass", "/api/coins?nocache=1"),
    ("coin detail", "/api/coin-detail/BTCUSDT"),
    ("coin detail series", "/api/coin-detail/BTCUSDT/series?range=24h"),
    ("coin detail structure score", "/api/coin-detail/BTCUSDT/structure-score"),
    ("coin detail trade opportunity", "/api/coin-detail/BTCUSDT/trade-opportunity"),
    ("market structure score", "/api/market-structure-score?symbol=BTCUSDT&limit=1"),
    ("trade opportunities", "/api/trade-opportunities?scope=all&limit=1"),
)

CACHE_CASES = (
    ("homepage cache hit", "/api/coins"),
    ("homepage forced bypass", "/api/coins?nocache=1"),
    ("trade opportunities cache repeat", "/api/trade-opportunities?scope=all&limit=1"),
)

WRITE_GUARD_CASES = (
    ("market rank refresh", "/api/market-rank/refresh"),
    ("homepage update", "/api/update"),
    ("funding refresh", "/api/funding-rate/refresh"),
    ("structure score refresh", "/api/market-structure-score/refresh"),
)

TABLES = (
    "market_klines",
    "market_open_interest_hist",
    "market_taker_buy_sell_vol",
    "market_funding_rate",
    "market_snapshots",
    "market_tickers",
)


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
    parser.add_argument("--start-local", action="store_true", help="start both local app instances and stop them at the end")
    parser.add_argument("--keep-apps", action="store_true", help="keep --start-local processes running after verification")
    parser.add_argument(
        "--exercise-rollback",
        action="store_true",
        help="stop ClickHouse, verify MySQL remains healthy, restart ClickHouse to verify data, then stop it again",
    )
    parser.add_argument("--as-of-ms", type=int, help="fixed replay timestamp in Unix milliseconds")
    parser.add_argument("--auto-as-of", action="store_true", help="choose the latest common closed five-minute timestamp")
    parser.add_argument("--http-timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--startup-timeout", type=float, default=60.0)
    parser.add_argument("--report-file", default=os.getenv("COINX_DUAL_REPORT_FILE", "data/clickhouse-dual-backend-report.json"))
    parser.add_argument("--require-complete", action="store_true", help="fail the gate when homepage_complete is false or score data is empty")
    args = parser.parse_args()
    if not args.mysql_password:
        parser.error("--mysql-password or COINX_MYSQL_TEST_PASSWORD is required")
    if not args.clickhouse_url:
        parser.error("--clickhouse-url or COINX_CK_TEST_URL is required")
    if args.as_of_ms is not None and args.as_of_ms <= 0:
        parser.error("--as-of-ms must be greater than zero")
    if args.auto_as_of and args.as_of_ms is not None:
        parser.error("--auto-as-of cannot be combined with --as-of-ms")
    if args.http_timeout <= 0 or args.startup_timeout <= 0:
        parser.error("timeouts must be greater than zero")
    if args.exercise_rollback and not args.start_local:
        parser.error("--exercise-rollback requires --start-local so the verifier owns the ClickHouse process")
    if args.exercise_rollback and args.keep_apps:
        parser.error("--exercise-rollback cannot be combined with --keep-apps")
    return args


def parse_host_port(value: str) -> Tuple[str, int]:
    value = (value or "").strip()
    if value.startswith("[") and "]" in value:
        host, suffix = value[1:].split("]", 1)
        return host, int(suffix[1:]) if suffix.startswith(":") else 3306
    if value.count(":") == 1:
        host, port = value.rsplit(":", 1)
        if port.isdigit():
            return host, int(port)
    return value, 3306


def with_as_of(path: str, as_of_ms: Optional[int]) -> str:
    if as_of_ms is None:
        return path
    parts = urlsplit(path)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["as_of_ms"] = str(int(as_of_ms))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def choose_auto_as_of(args: argparse.Namespace) -> Dict[str, Any]:
    """Find a common closed snapshot using read-only database clients."""
    sys.path.insert(0, str(SRC))
    from coinx.read_clients import ClickHouseReadClient, MySQLReadClient
    from coinx.repositories.market_read import ClickHouseMarketReadRepository, MySQLMarketReadRepository

    bounds: Dict[str, Any] = {}
    candidates: List[int] = []
    with ClickHouseReadClient(
        args.clickhouse_url, args.clickhouse_database, args.clickhouse_user, args.clickhouse_password,
    ) as clickhouse_client, MySQLReadClient(
        args.mysql_host, args.mysql_database, args.mysql_user, args.mysql_password,
    ) as mysql_client:
        clickhouse = ClickHouseMarketReadRepository(clickhouse_client, args.clickhouse_database)
        mysql = MySQLMarketReadRepository(mysql_client)
        for table in TABLES:
            mysql_bounds = mysql.time_bounds(table)
            clickhouse_bounds = clickhouse.time_bounds(table)
            bounds[table] = {"mysql": mysql_bounds, "clickhouse": clickhouse_bounds}
            mysql_max = mysql_bounds.get("max_time")
            clickhouse_max = clickhouse_bounds.get("max_time")
            if mysql_max is not None and clickhouse_max is not None:
                candidates.append(min(int(mysql_max), int(clickhouse_max)))
    if not candidates:
        raise RuntimeError("no common non-empty table range was found")
    common_upper = min(candidates)
    as_of_ms = (common_upper // DEFAULT_AS_OF_FLOOR_MS) * DEFAULT_AS_OF_FLOOR_MS
    if as_of_ms <= 0:
        raise RuntimeError(f"invalid common timestamp: {common_upper}")
    return {"as_of_ms": as_of_ms, "common_upper_ms": common_upper, "time_bounds": bounds}


def _json_payload(response: requests.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return None


def _number_equal(left: Any, right: Any) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return left == right
    if not isinstance(left, (int, float)) or not isinstance(right, (int, float)):
        return left == right
    difference = abs(float(left) - float(right))
    scale = max(abs(float(left)), abs(float(right)), 1.0)
    return difference <= max(NUMERIC_ABS_TOLERANCE, NUMERIC_REL_TOLERANCE * scale)


def json_diffs(left: Any, right: Any, path: str = "$", limit: int = 100) -> List[str]:
    """Compare complete JSON values and return deterministic paths."""
    differences: List[str] = []

    def walk(a: Any, b: Any, current: str) -> None:
        if len(differences) >= limit:
            return
        if isinstance(a, dict) and isinstance(b, dict):
            for key in sorted(set(a) | set(b)):
                child = f"{current}.{key}"
                if key not in a:
                    differences.append(f"{child}: missing_in_mysql")
                elif key not in b:
                    differences.append(f"{child}: missing_in_clickhouse")
                else:
                    walk(a[key], b[key], child)
            return
        if isinstance(a, list) and isinstance(b, list):
            if len(a) != len(b):
                differences.append(f"{current}: mysql_length={len(a)} clickhouse_length={len(b)}")
            for index, (item_a, item_b) in enumerate(zip(a, b)):
                walk(item_a, item_b, f"{current}[{index}]")
            return
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            if not _number_equal(a, b):
                differences.append(f"{current}: mysql={a!r} clickhouse={b!r}")
            return
        if a != b:
            differences.append(f"{current}: mysql={a!r} clickhouse={b!r}")

    walk(left, right, path)
    return differences


def payload_summary(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {"type": type(payload).__name__}
    data = payload.get("data")
    result: Dict[str, Any] = {"status": payload.get("status")}
    if isinstance(data, (list, dict, str)):
        result["data_type"] = type(data).__name__
        result["data_count"] = len(data)
    for key in ("cache_update_time", "homepage_complete", "snapshot_time", "total_count", "page", "page_size"):
        if key in payload:
            result[key] = payload[key]
    return result


def _url(base: str, path: str) -> str:
    return base.rstrip("/") + "/" + path.lstrip("/")


@dataclass
class LocalProcess:
    name: str
    process: subprocess.Popen
    log_file: Any

    def stop(self) -> None:
        if self.process.poll() is not None:
            self.log_file.close()
            return
        try:
            if os.name == "nt":
                self.process.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                self.process.terminate()
            self.process.wait(timeout=15)
        except (subprocess.TimeoutExpired, OSError):
            self.process.kill()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        finally:
            self.log_file.close()


def start_instance(
    args: argparse.Namespace,
    name: str,
    backend: str,
    port: int,
) -> LocalProcess:
    log_dir = Path(tempfile.gettempdir()) / "coinx-dual-backend"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{name}-{os.getpid()}.log"
    internal_log_path = log_dir / f"{name}-{os.getpid()}.application.log"
    log_file = log_path.open("a", encoding="utf-8")
    env = os.environ.copy()
    mysql_host, mysql_port = parse_host_port(args.mysql_host)
    env.update({
        "PYTHONPATH": str(SRC) + os.pathsep + env.get("PYTHONPATH", ""),
        "WEB_HOST": "127.0.0.1",
        "WEB_PORT": str(port),
        "WEB_AUTH_DISABLED": "true",
        "SCHEDULER_ENABLED": "false",
        "READ_BACKEND": backend,
        "INSTANCE_NAME": name,
        "APP_LOG_FILE": str(internal_log_path),
        "DB_HOST": mysql_host,
        "DB_PORT": str(mysql_port),
        "DB_NAME": args.mysql_database,
        "DB_USER": args.mysql_user,
        "DB_PASSWORD": args.mysql_password,
        "CLICKHOUSE_URL": args.clickhouse_url,
        "CLICKHOUSE_DATABASE": args.clickhouse_database,
        "CLICKHOUSE_USER": args.clickhouse_user,
        "CLICKHOUSE_PASSWORD": args.clickhouse_password,
        "CLICKHOUSE_READ_SHADOW": "false",
    })
    command = [sys.executable, "-u", str(SRC / "coinx" / "main.py")]
    process = subprocess.Popen(
        command,
        cwd=str(ROOT),
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
    )
    return LocalProcess(name, process, log_file)


def wait_for_health(base_url: str, timeout: float, expected_backend: str) -> Dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_error: Optional[str] = None
    while time.monotonic() < deadline:
        try:
            response = requests.get(_url(base_url, "/api/health/read-backend"), timeout=5)
            payload = _json_payload(response)
            if response.status_code == 200 and isinstance(payload, dict) and payload.get("backend") == expected_backend:
                return {
                    "status_code": response.status_code,
                    "payload": payload,
                    "header_backend": response.headers.get("X-Read-Backend"),
                }
            last_error = f"status={response.status_code} payload={payload!r}"
        except requests.RequestException as exc:
            last_error = repr(exc)
        time.sleep(0.5)
    raise RuntimeError(f"{expected_backend} instance did not become healthy: {last_error}")


def wait_for_unreachable(base_url: str, timeout: float) -> Dict[str, Any]:
    """Confirm an application port is no longer accepting HTTP connections."""
    deadline = time.monotonic() + timeout
    last_response: Optional[str] = None
    while time.monotonic() < deadline:
        try:
            response = requests.get(_url(base_url, "/api/health/read-backend"), timeout=2)
            last_response = f"status={response.status_code}"
        except requests.RequestException as exc:
            return {"unreachable": True, "last_response": last_response, "error": repr(exc)}
        time.sleep(0.25)
    return {"unreachable": False, "last_response": last_response}


def read_table_counts(args: argparse.Namespace, backend: str) -> Dict[str, int]:
    """Read table counts for rollback evidence without issuing mutations."""
    sys.path.insert(0, str(SRC))
    from coinx.read_clients import ClickHouseReadClient, MySQLReadClient
    from coinx.repositories.market_read import ClickHouseMarketReadRepository, MySQLMarketReadRepository

    if backend == "mysql":
        with MySQLReadClient(args.mysql_host, args.mysql_database, args.mysql_user, args.mysql_password) as client:
            repository = MySQLMarketReadRepository(client)
            return {table: repository.table_count(table) for table in TABLES}
    with ClickHouseReadClient(
        args.clickhouse_url,
        args.clickhouse_database,
        args.clickhouse_user,
        args.clickhouse_password,
    ) as client:
        repository = ClickHouseMarketReadRepository(client, args.clickhouse_database)
        return {table: repository.table_count(table) for table in TABLES}


def exercise_rollback(
    args: argparse.Namespace,
    local_processes: List[LocalProcess],
    startup_timeout: float,
) -> Tuple[Dict[str, Any], List[str]]:
    """Exercise an application-level ClickHouse rollback without deleting data."""
    failures: List[str] = []
    evidence: Dict[str, Any] = {
        "requested": True,
        "application_api_requests_after_stop": 0,
        "data_deletion_commands_issued": False,
    }
    mysql_before = read_table_counts(args, "mysql")
    clickhouse_before = read_table_counts(args, "clickhouse")
    evidence["mysql_counts_before"] = mysql_before
    evidence["clickhouse_counts_before"] = clickhouse_before

    clickhouse_process = next((item for item in local_processes if item.name == "clickhouse"), None)
    if clickhouse_process is None:
        failures.append("rollback: clickhouse process was not started")
        evidence["gate_passed"] = False
        return evidence, failures

    clickhouse_process.stop()
    stopped_state = wait_for_unreachable(args.clickhouse_api_url, min(startup_timeout, 30.0))
    evidence["clickhouse_unreachable_after_stop"] = bool(stopped_state.get("unreachable"))
    evidence["clickhouse_stop_probe"] = stopped_state
    if not stopped_state.get("unreachable"):
        failures.append(f"rollback: ClickHouse API remained reachable: {stopped_state}")

    try:
        mysql_health = wait_for_health(args.mysql_api_url, min(startup_timeout, 30.0), "mysql")
        evidence["mysql_health_after_stop"] = mysql_health
        with requests.Session() as mysql_session:
            mysql_probe = request_json(
                mysql_session,
                args.mysql_api_url,
                "/api/health/read-backend",
                timeout=10.0,
            )
        evidence["mysql_backend_after_stop"] = mysql_probe
        if mysql_probe.get("status_code") != 200 or mysql_probe.get("headers", {}).get("X-Read-Backend") != "mysql":
            failures.append(f"rollback: MySQL backend health failed: {mysql_probe}")
    except Exception as exc:
        evidence["mysql_health_after_stop"] = {"error": repr(exc)}
        failures.append(f"rollback: MySQL health failed after ClickHouse stop: {exc!r}")

    mysql_after = read_table_counts(args, "mysql")
    evidence["mysql_counts_after_stop"] = mysql_after
    evidence["mysql_counts_unchanged"] = mysql_after == mysql_before
    if mysql_after != mysql_before:
        failures.append(f"rollback: MySQL row counts changed: before={mysql_before} after={mysql_after}")

    # Restart only to prove the ClickHouse data was not removed, then stop it
    # again so the rollback ends with zero ClickHouse application traffic.
    restarted = start_instance(args, "clickhouse", "clickhouse", 5501)
    local_processes[local_processes.index(clickhouse_process)] = restarted
    try:
        restarted_health = wait_for_health(args.clickhouse_api_url, startup_timeout, "clickhouse")
        evidence["clickhouse_health_after_restart"] = restarted_health
        clickhouse_after = read_table_counts(args, "clickhouse")
        evidence["clickhouse_counts_after_restart"] = clickhouse_after
        evidence["clickhouse_counts_unchanged"] = clickhouse_after == clickhouse_before
        if clickhouse_after != clickhouse_before:
            failures.append(
                f"rollback: ClickHouse row counts changed: before={clickhouse_before} after={clickhouse_after}"
            )
    except Exception as exc:
        evidence["clickhouse_health_after_restart"] = {"error": repr(exc)}
        failures.append(f"rollback: ClickHouse could not be restored for data-preservation probe: {exc!r}")
    finally:
        restarted.stop()
        final_state = wait_for_unreachable(args.clickhouse_api_url, min(startup_timeout, 30.0))
        evidence["clickhouse_unreachable_at_end"] = bool(final_state.get("unreachable"))
        evidence["clickhouse_final_stop_probe"] = final_state
        if not final_state.get("unreachable"):
            failures.append(f"rollback: ClickHouse API remained reachable at end: {final_state}")

    evidence["clickhouse_traffic_zero_after_stop"] = bool(
        evidence.get("clickhouse_unreachable_after_stop")
        and evidence.get("clickhouse_unreachable_at_end")
        and evidence.get("application_api_requests_after_stop") == 0
    )
    if not evidence["clickhouse_traffic_zero_after_stop"]:
        failures.append("rollback: ClickHouse application traffic was not proven to be zero after stop")
    evidence["gate_passed"] = not failures
    return evidence, failures


def request_json(session: requests.Session, base_url: str, path: str, timeout: float, method: str = "GET") -> Dict[str, Any]:
    started = time.perf_counter()
    response = session.request(method, _url(base_url, path), timeout=timeout)
    elapsed_ms = (time.perf_counter() - started) * 1000
    return {
        "status_code": response.status_code,
        "elapsed_ms": round(elapsed_ms, 1),
        "payload": _json_payload(response),
        "headers": {"X-Read-Backend": response.headers.get("X-Read-Backend")},
        "body_preview": response.text[:1000] if response.headers.get("Content-Type", "").lower().find("json") < 0 else None,
    }


def run_backend_cases(
    name: str,
    base_url: str,
    as_of_ms: Optional[int],
    timeout: float,
    require_complete: bool,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
    session = requests.Session()
    api_results: List[Dict[str, Any]] = []
    cache_results: List[Dict[str, Any]] = []
    failures: List[str] = []
    try:
        for label, raw_path in API_CASES:
            path = with_as_of(raw_path, as_of_ms)
            try:
                result = request_json(session, base_url, path, timeout)
                payload = result["payload"]
                ok = result["status_code"] == 200 and isinstance(payload, dict) and payload.get("status") == "success"
                warnings: List[str] = []
                if isinstance(payload, dict) and payload.get("homepage_complete") is False:
                    warnings.append("homepage_complete=false")
                    if require_complete:
                        ok = False
                if label == "market structure score" and isinstance(payload, dict):
                    data = payload.get("data")
                    if isinstance(data, list) and not data:
                        warnings.append("empty structure score data")
                        if require_complete:
                            ok = False
                record = {
                    "backend": name,
                    "label": label,
                    "path": path,
                    "ok": ok,
                    "warnings": warnings,
                    **result,
                    "payload_summary": payload_summary(payload),
                }
                if not ok:
                    failures.append(f"{name}/{label}: HTTP or payload gate failed: {result}")
            except Exception as exc:
                record = {"backend": name, "label": label, "path": path, "ok": False, "error": repr(exc)}
                failures.append(f"{name}/{label}: request failed: {exc!r}")
            api_results.append(record)
            print(f"[{name}] [{'OK' if record['ok'] else 'FAIL'}] {label}", flush=True)

        homepage_cold = next((item for item in api_results if item["label"] == "homepage cold bypass"), None)
        homepage_cold_payload = (homepage_cold or {}).get("payload")
        trade_first = next((item for item in api_results if item["label"] == "trade opportunities"), None)
        trade_first_payload = (trade_first or {}).get("payload")
        cache_cases = [(label, with_as_of(path, as_of_ms)) for label, path in CACHE_CASES]
        for label, path in cache_cases:
            try:
                result = request_json(session, base_url, path, timeout)
                payload = result["payload"]
                ok = result["status_code"] == 200 and isinstance(payload, dict) and payload.get("status") == "success"
                details: Dict[str, Any] = {"payload_summary": payload_summary(payload)}
                if label == "homepage cache hit":
                    details["equal_to_cold"] = payload == homepage_cold_payload
                    ok = ok and details["equal_to_cold"]
                elif label == "homepage forced bypass":
                    details["equal_to_cold"] = payload == homepage_cold_payload
                    ok = ok and details["equal_to_cold"]
                else:
                    details["equal_to_first"] = payload == trade_first_payload
                    ok = ok and details["equal_to_first"]
                record = {
                    "backend": name,
                    "label": label,
                    "path": path,
                    "ok": ok,
                    "details": details,
                    **result,
                }
                if not ok:
                    failures.append(f"{name}/{label}: cache check failed: {details}")
            except Exception as exc:
                record = {"backend": name, "label": label, "path": path, "ok": False, "error": repr(exc), "details": {}}
                failures.append(f"{name}/{label}: cache request failed: {exc!r}")
            cache_results.append(record)
            print(f"[{name}] [{'OK' if record['ok'] else 'FAIL'}] {label}", flush=True)
    finally:
        session.close()
    return api_results, cache_results, failures


def compare_api_results(mysql_results: Sequence[Dict[str, Any]], clickhouse_results: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_mysql = {item["label"]: item for item in mysql_results}
    by_clickhouse = {item["label"]: item for item in clickhouse_results}
    comparisons: List[Dict[str, Any]] = []
    for label in sorted(set(by_mysql) | set(by_clickhouse)):
        left = by_mysql.get(label)
        right = by_clickhouse.get(label)
        diffs: List[str] = []
        if left is None:
            diffs.append("missing_in_mysql")
        elif right is None:
            diffs.append("missing_in_clickhouse")
        else:
            if left.get("status_code") != right.get("status_code"):
                diffs.append(f"$.http_status: mysql={left.get('status_code')} clickhouse={right.get('status_code')}")
            diffs.extend(json_diffs(left.get("payload"), right.get("payload")))
        comparisons.append({
            "label": label,
            "path": (left or right or {}).get("path", ""),
            "status": "PASS" if not diffs else "DIFF",
            "diffs": diffs,
            "mysql_summary": payload_summary((left or {}).get("payload")),
            "clickhouse_summary": payload_summary((right or {}).get("payload")),
        })
    return comparisons


def check_write_guard(base_url: str, timeout: float) -> Tuple[List[Dict[str, Any]], List[str]]:
    session = requests.Session()
    checks: List[Dict[str, Any]] = []
    failures: List[str] = []
    try:
        for label, path in WRITE_GUARD_CASES:
            try:
                result = request_json(session, base_url, path, timeout, method="POST")
                ok = result["status_code"] == 503 and isinstance(result.get("payload"), dict) and result["payload"].get("read_backend") == "clickhouse"
                check = {"label": label, "path": path, "ok": ok, **result}
                if not ok:
                    failures.append(f"clickhouse/{label}: write guard failed: {result}")
            except Exception as exc:
                check = {"label": label, "path": path, "ok": False, "error": repr(exc)}
                failures.append(f"clickhouse/{label}: write guard request failed: {exc!r}")
            checks.append(check)
            print(f"[clickhouse] [{'OK' if check['ok'] else 'FAIL'}] write guard {label}", flush=True)
    finally:
        session.close()
    return checks, failures


def report_markdown(report: Dict[str, Any]) -> str:
    metadata = report["metadata"]
    summary = report["summary"]
    lines = [
        "# ClickHouse Dual Backend API Verification",
        "",
        f"Generated: `{metadata['generated_at']}`",
        f"MySQL API: `{metadata['mysql_api_url']}`",
        f"ClickHouse API: `{metadata['clickhouse_api_url']}`",
        f"Replay timestamp: `{metadata.get('as_of_ms')}`",
        "",
        "## Gate Summary",
        "",
        f"- API checks: {summary['api_checks']} ({summary['api_failures']} failed)",
        f"- Cache checks: {summary['cache_checks']} ({summary['cache_failures']} failed)",
        f"- JSON comparisons: {summary['comparisons']} ({summary['comparison_diffs']} diffs)",
        f"- ClickHouse write guards: {summary['write_guards']} ({summary['write_guard_failures']} failed)",
        f"- Content warnings: {summary['content_warnings']}",
        f"- Overall gate: **{'PASS' if summary['gate_passed'] else 'FAIL'}**",
        "",
        "## Backend Health",
        "",
        "| Backend | Status | Payload | Header |",
        "|---|---:|---|---|",
    ]
    for item in report["health"]:
        lines.append(
            f"| {item['backend']} | {item.get('status_code', '-')} | "
            f"`{json.dumps(item.get('payload'), ensure_ascii=False, separators=(',', ':'))}` | "
            f"`{item.get('header_backend', '-')}` |"
        )
    lines.extend(["", "## API Comparisons", "", "| Endpoint | Result | MySQL | ClickHouse |", "|---|---:|---|---|"])
    for item in report["comparisons"]:
        lines.append(
            f"| `{item['path']}` | {item['status']} | "
            f"`{json.dumps(item['mysql_summary'], ensure_ascii=False, separators=(',', ':'))}` | "
            f"`{json.dumps(item['clickhouse_summary'], ensure_ascii=False, separators=(',', ':'))}` |"
        )
        for diff in item.get("diffs", [])[:10]:
            lines.append(f"|  |  |  | `{diff}` |")
    lines.extend(["", "## Cache Checks", "", "| Backend | Check | Result | Details |", "|---|---|---:|---|"])
    for item in report["cache_checks"]:
        lines.append(
            f"| {item['backend']} | `{item['label']}` | {'PASS' if item['ok'] else 'FAIL'} | "
            f"`{json.dumps(item.get('details', {}), ensure_ascii=False, separators=(',', ':'))}` |"
        )
    lines.extend(["", "## ClickHouse Write Guards", "", "| Check | Result | HTTP |", "|---|---:|---:|"])
    for item in report["write_guards"]:
        lines.append(f"| `{item['label']}` | {'PASS' if item['ok'] else 'FAIL'} | {item.get('status_code', '-')} |")
    rollback = report.get("rollback")
    if rollback is not None:
        lines.extend([
            "",
            "## Rollback Exercise",
            "",
            f"- Result: **{'PASS' if rollback.get('gate_passed') else 'FAIL'}**",
            f"- MySQL counts unchanged: `{rollback.get('mysql_counts_unchanged')}`",
            f"- ClickHouse counts unchanged after restart: `{rollback.get('clickhouse_counts_unchanged')}`",
            f"- ClickHouse unreachable after stop: `{rollback.get('clickhouse_unreachable_after_stop')}`",
            f"- ClickHouse traffic zero at end: `{rollback.get('clickhouse_traffic_zero_after_stop')}`",
            f"- Data deletion commands issued: `{rollback.get('data_deletion_commands_issued')}`",
        ])
    lines.extend(["", "## Failures", ""])
    lines.extend(f"- {failure}" for failure in report["failures"]) or lines.append("- None")
    lines.extend(["", "Full response payloads and diff paths are stored in the JSON report.", ""])
    return "\n".join(lines)


def write_report(report: Dict[str, Any], report_file: str) -> Tuple[Path, Path]:
    json_path = Path(report_file)
    if not json_path.is_absolute():
        json_path = ROOT / json_path
    json_path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    try:
        json_path.write_text(payload, encoding="utf-8")
    except PermissionError:
        # Reports are evidence, not application state.  A read-only or
        # locked data directory must not discard the result; use the system
        # temp directory and print the effective paths to the caller.
        fallback_dir = Path(tempfile.gettempdir())
        json_path = fallback_dir / json_path.name
        json_path.write_text(payload, encoding="utf-8")
    markdown_path = json_path.with_suffix(".md")
    markdown_path.write_text(report_markdown(report), encoding="utf-8")
    return json_path, markdown_path


def main() -> int:
    args = parse_args()
    time_selection: Dict[str, Any] = {}
    if args.auto_as_of:
        time_selection = choose_auto_as_of(args)
        args.as_of_ms = int(time_selection["as_of_ms"])
        print(
            f"Common replay timestamp: as_of_ms={args.as_of_ms} (upper={time_selection['common_upper_ms']})",
            flush=True,
        )
    if args.as_of_ms is None:
        raise SystemExit("--as-of-ms or --auto-as-of is required for a deterministic dual-backend comparison")

    local_processes: List[LocalProcess] = []
    failures: List[str] = []
    rollback_evidence: Optional[Dict[str, Any]] = None
    try:
        if args.start_local:
            local_processes.append(start_instance(args, "mysql", "mysql", 5500))
            local_processes.append(start_instance(args, "clickhouse", "clickhouse", 5501))
            try:
                wait_for_health(args.mysql_api_url, args.startup_timeout, "mysql")
                wait_for_health(args.clickhouse_api_url, args.startup_timeout, "clickhouse")
            except Exception:
                for process in local_processes:
                    process.stop()
                raise

        health: List[Dict[str, Any]] = []
        for backend, url in (("mysql", args.mysql_api_url), ("clickhouse", args.clickhouse_api_url)):
            try:
                item = wait_for_health(url, min(args.startup_timeout, 15.0), backend)
                health.append({"backend": backend, **item})
                print(f"[{backend}] health OK", flush=True)
            except Exception as exc:
                health.append({"backend": backend, "ok": False, "error": repr(exc)})
                failures.append(f"{backend} health failed: {exc!r}")

        mysql_api, mysql_cache, mysql_failures = run_backend_cases(
            "mysql", args.mysql_api_url, args.as_of_ms, args.http_timeout, args.require_complete,
        )
        clickhouse_api, clickhouse_cache, clickhouse_failures = run_backend_cases(
            "clickhouse", args.clickhouse_api_url, args.as_of_ms, args.http_timeout, args.require_complete,
        )
        failures.extend(mysql_failures)
        failures.extend(clickhouse_failures)

        comparisons = compare_api_results(mysql_api, clickhouse_api)
        for item in comparisons:
            if item["status"] != "PASS":
                failures.append(f"API JSON diff: {item['label']}: {item['diffs'][:10]}")
            print(f"[compare] [{'PASS' if item['status'] == 'PASS' else 'DIFF'}] {item['label']}", flush=True)

        write_guards, guard_failures = check_write_guard(args.clickhouse_api_url, args.http_timeout)
        failures.extend(guard_failures)

        if args.exercise_rollback:
            print("[rollback] starting ClickHouse stop/restart/data-preservation exercise", flush=True)
            rollback_evidence, rollback_failures = exercise_rollback(
                args,
                local_processes,
                args.startup_timeout,
            )
            failures.extend(rollback_failures)
            print(
                f"[rollback] [{'PASS' if rollback_evidence.get('gate_passed') else 'FAIL'}] "
                f"MySQL preserved={rollback_evidence.get('mysql_counts_unchanged')} "
                f"ClickHouse preserved={rollback_evidence.get('clickhouse_counts_unchanged')} "
                f"traffic_zero={rollback_evidence.get('clickhouse_traffic_zero_after_stop')}",
                flush=True,
            )

        cache_checks = mysql_cache + clickhouse_cache
        content_warnings = [
            f"{item['backend']}/{item['label']}: {warning}"
            for item in mysql_api + clickhouse_api
            for warning in item.get("warnings", [])
        ]
        summary = {
            "api_checks": len(mysql_api) + len(clickhouse_api),
            "api_failures": sum(1 for item in mysql_api + clickhouse_api if not item.get("ok")),
            "cache_checks": len(cache_checks),
            "cache_failures": sum(1 for item in cache_checks if not item.get("ok")),
            "comparisons": len(comparisons),
            "comparison_diffs": sum(1 for item in comparisons if item["status"] != "PASS"),
            "write_guards": len(write_guards),
            "write_guard_failures": sum(1 for item in write_guards if not item.get("ok")),
            "content_warnings": len(content_warnings),
            "rollback_requested": bool(args.exercise_rollback),
            "rollback_passed": rollback_evidence.get("gate_passed") if rollback_evidence is not None else None,
            "gate_passed": not failures and all(item["status"] == "PASS" for item in comparisons),
        }
        report = {
            "metadata": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "mysql_api_url": args.mysql_api_url,
                "clickhouse_api_url": args.clickhouse_api_url,
                "mysql_target": f"{args.mysql_host}/{args.mysql_database}",
                "clickhouse_target": f"{args.clickhouse_url}/{args.clickhouse_database}",
                "as_of_ms": args.as_of_ms,
                "as_of_mode": "auto" if args.auto_as_of else "manual",
                "common_upper_ms": time_selection.get("common_upper_ms"),
                "time_bounds": time_selection.get("time_bounds", {}),
                "read_only": True,
                "numeric_abs_tolerance": NUMERIC_ABS_TOLERANCE,
                "numeric_rel_tolerance": NUMERIC_REL_TOLERANCE,
            },
            "health": health,
            "api_checks": {"mysql": mysql_api, "clickhouse": clickhouse_api},
            "cache_checks": cache_checks,
            "comparisons": comparisons,
            "write_guards": write_guards,
            "rollback": rollback_evidence,
            "content_warnings": content_warnings,
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
