#!/usr/bin/env python3
"""Exercise market-data GET APIs against MySQL with cache-aware checks.

The script uses Flask's test client, never calls refresh/write endpoints, and
does not persist credentials. It is intended for a local migration smoke test.
"""

from __future__ import annotations

import argparse
import json
import logging
import logging.handlers
import os
import sys
import tempfile
import time
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))


class _NoFileHandler(logging.StreamHandler):
    """Keep this verification from opening the application's log file."""

    def __init__(self, *_args, **_kwargs):
        super().__init__()


class _ShadowCaptureHandler(logging.Handler):
    def __init__(self, events: List[Dict[str, Any]]):
        super().__init__(level=logging.WARNING)
        self.events = events

    def emit(self, record: logging.LogRecord) -> None:
        if "market_shadow" not in record.name:
            return
        self.events.append(
            {
                "level": record.levelname,
                "message": record.getMessage(),
            }
        )


def _parse_host_port(value: str) -> tuple[str, int]:
    value = (value or "").strip()
    if value.count(":") == 1:
        host, port = value.rsplit(":", 1)
        if port.isdigit():
            return host, int(port)
    return value, 3306


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mysql-host", default=os.getenv("COINX_MYSQL_TEST_HOST", "127.0.0.1:3306"))
    parser.add_argument("--mysql-database", default=os.getenv("COINX_MYSQL_TEST_DATABASE", "coinx"))
    parser.add_argument("--mysql-user", default=os.getenv("COINX_MYSQL_TEST_USER", "root"))
    parser.add_argument("--mysql-password", default=os.getenv("COINX_MYSQL_TEST_PASSWORD"), required=False)
    parser.add_argument("--clickhouse-url", default=os.getenv("COINX_CK_TEST_URL"))
    parser.add_argument("--clickhouse-database", default=os.getenv("COINX_CK_TEST_DATABASE", "coinx"))
    parser.add_argument("--clickhouse-user", default=os.getenv("COINX_CK_TEST_USER", "default"))
    parser.add_argument("--clickhouse-password", default=os.getenv("COINX_CK_TEST_PASSWORD", ""))
    parser.add_argument("--shadow", action="store_true", help="enable asynchronous ClickHouse shadow reads")
    parser.add_argument(
        "--read-backend",
        choices=("mysql", "clickhouse"),
        default=os.getenv("READ_BACKEND", os.getenv("MARKET_BACKEND", "mysql")),
        help="run the Flask API against the selected primary read backend",
    )
    parser.add_argument(
        "--compare-clickhouse",
        action="store_true",
        help="compare concrete API JSON with equivalent ClickHouse read queries",
    )
    parser.add_argument(
        "--as-of-ms",
        type=int,
        help="replay all time-sensitive reads at this Unix timestamp in milliseconds",
    )
    parser.add_argument(
        "--auto-as-of",
        action="store_true",
        help="read both databases and choose their latest common closed 5-minute snapshot",
    )
    parser.add_argument(
        "--report-file",
        default=os.getenv("COINX_API_REPORT_FILE", "data/clickhouse-api-readonly-report.json"),
        help="JSON report path; a Markdown report is written alongside it",
    )
    args = parser.parse_args()
    if not args.mysql_password:
        parser.error("--mysql-password or COINX_MYSQL_TEST_PASSWORD is required")
    if args.as_of_ms is not None and args.as_of_ms <= 0:
        parser.error("--as-of-ms must be greater than zero")
    if args.auto_as_of and args.as_of_ms is not None:
        parser.error("--auto-as-of cannot be combined with --as-of-ms")
    if (args.compare_clickhouse or args.auto_as_of) and not args.clickhouse_url:
        parser.error("--clickhouse-url is required with --compare-clickhouse/--auto-as-of")
    if args.compare_clickhouse and args.as_of_ms is None and not args.auto_as_of:
        parser.error("--as-of-ms or --auto-as-of is required with --compare-clickhouse")
    return args


def _resolve_auto_as_of(args: argparse.Namespace) -> Dict[str, Any]:
    """Choose a common read upper bound without changing either database."""
    from coinx.read_clients import ClickHouseReadClient, MySQLReadClient
    from coinx.repositories.market_read import (
        ClickHouseMarketReadRepository,
        MySQLMarketReadRepository,
        TABLES,
    )

    bounds: Dict[str, Any] = {}
    with ClickHouseReadClient(
        args.clickhouse_url,
        args.clickhouse_database,
        args.clickhouse_user,
        args.clickhouse_password,
    ) as clickhouse_client, MySQLReadClient(
        args.mysql_host,
        args.mysql_database,
        args.mysql_user,
        args.mysql_password,
    ) as mysql_client:
        clickhouse = ClickHouseMarketReadRepository(clickhouse_client, args.clickhouse_database)
        mysql = MySQLMarketReadRepository(mysql_client)
        common_candidates: List[int] = []
        for table in TABLES:
            mysql_bounds = mysql.time_bounds(table)
            clickhouse_bounds = clickhouse.time_bounds(table)
            bounds[table] = {"mysql": mysql_bounds, "clickhouse": clickhouse_bounds}
            mysql_max = mysql_bounds.get("max_time")
            clickhouse_max = clickhouse_bounds.get("max_time")
            if mysql_max is not None and clickhouse_max is not None:
                common_candidates.append(min(int(mysql_max), int(clickhouse_max)))

    if not common_candidates:
        raise RuntimeError("两边没有任何一张非空表存在共同时间范围")
    common_upper_ms = min(common_candidates)
    # All market series use five-minute collection intervals. Flooring keeps
    # the replay point on a closed interval and avoids looking into a partial
    # latest window.
    as_of_ms = (common_upper_ms // 300000) * 300000
    if as_of_ms <= 0:
        raise RuntimeError(f"共同时间上界无效: {common_upper_ms}")
    return {
        "as_of_ms": as_of_ms,
        "common_upper_ms": common_upper_ms,
        "time_bounds": bounds,
    }


def _with_as_of(path: str, as_of_ms: int | None) -> str:
    if as_of_ms is None:
        return path
    parts = urlsplit(path)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["as_of_ms"] = str(int(as_of_ms))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _configure_environment(args: argparse.Namespace) -> None:
    host, port = _parse_host_port(args.mysql_host)
    os.environ.update(
        {
            "DB_HOST": host,
            "DB_PORT": str(port),
            "DB_NAME": args.mysql_database,
            "DB_USER": args.mysql_user,
            "DB_PASSWORD": args.mysql_password,
            "WEB_AUTH_DISABLED": "true",
            "SCHEDULER_ENABLED": "false",
            "READ_BACKEND": args.read_backend,
        }
    )
    if args.clickhouse_url:
        os.environ.update(
            {
                "CLICKHOUSE_URL": args.clickhouse_url,
                "CLICKHOUSE_DATABASE": args.clickhouse_database,
                "CLICKHOUSE_USER": args.clickhouse_user,
                "CLICKHOUSE_PASSWORD": args.clickhouse_password,
            }
        )
    if args.shadow:
        if not args.clickhouse_url:
            raise ValueError("--clickhouse-url or COINX_CK_TEST_URL is required with --shadow")
        os.environ.update(
            {
                "CLICKHOUSE_URL": args.clickhouse_url,
                "CLICKHOUSE_DATABASE": args.clickhouse_database,
                "CLICKHOUSE_USER": args.clickhouse_user,
                "CLICKHOUSE_PASSWORD": args.clickhouse_password,
                "CLICKHOUSE_READ_SHADOW": "true",
            }
        )


def _load_app():
    logging.handlers.RotatingFileHandler = _NoFileHandler
    from coinx.web.app import app

    return app


def _json_response(response) -> Any:
    try:
        return response.get_json()
    except Exception:
        return None


def _payload_summary(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {"type": type(payload).__name__}
    summary: Dict[str, Any] = {
        "keys": sorted(payload.keys()),
        "status": payload.get("status"),
    }
    data = payload.get("data")
    if isinstance(data, (list, dict, str)):
        summary["data_type"] = type(data).__name__
        summary["data_count"] = len(data)
    if "cache_update_time" in payload:
        summary["cache_update_time"] = payload.get("cache_update_time")
    if "homepage_complete" in payload:
        summary["homepage_complete"] = payload.get("homepage_complete")
    return summary


def _report_payload_summary(payload: Any) -> Dict[str, Any]:
    """Return a compact side-by-side summary for the Markdown report.

    The JSON report keeps the complete payloads. Markdown needs enough shape
    information to show what each database returned without embedding megabytes
    of homepage/chart data in every table row.
    """
    if not isinstance(payload, dict):
        return {"type": type(payload).__name__}

    summary: Dict[str, Any] = {"status": payload.get("status")}
    data = payload.get("data")
    if isinstance(data, list):
        summary["data_type"] = "list"
        summary["data_count"] = len(data)
    elif isinstance(data, dict):
        summary["data_type"] = "dict"
        summary["data_keys"] = sorted(data.keys())
        for key in ("range", "anchor_time", "as_of", "symbol", "data_status", "current_time"):
            if key in data:
                summary[key] = data.get(key)
        for key in ("market", "flow", "funding_rate", "exchange_scores", "intervals"):
            value = data.get(key)
            if isinstance(value, list):
                summary[f"{key}_count"] = len(value)
    elif data is not None:
        summary["data_type"] = type(data).__name__

    for key in (
        "cache_update_time", "homepage_complete", "total_count", "page",
        "page_size", "threshold", "symbol", "hours", "snapshot_time",
    ):
        if key in payload:
            summary[key] = payload.get(key)
    return summary


def _json_diffs(left: Any, right: Any, path: str = "$", limit: int = 100) -> List[str]:
    """Return deterministic, bounded paths for concrete response differences."""
    diffs: List[str] = []

    def walk(a: Any, b: Any, current: str) -> None:
        if len(diffs) >= limit:
            return
        if isinstance(a, dict) and isinstance(b, dict):
            for key in sorted(set(a) | set(b)):
                child = f"{current}.{key}"
                if key not in a:
                    diffs.append(f"{child}: missing_in_mysql")
                elif key not in b:
                    diffs.append(f"{child}: missing_in_clickhouse")
                else:
                    walk(a[key], b[key], child)
            return
        if isinstance(a, list) and isinstance(b, list):
            if len(a) != len(b):
                diffs.append(f"{current}: mysql_length={len(a)} clickhouse_length={len(b)}")
            for index, (item_a, item_b) in enumerate(zip(a, b)):
                walk(item_a, item_b, f"{current}[{index}]")
            return
        if isinstance(a, (int, float)) and not isinstance(a, bool) and isinstance(b, (int, float)) and not isinstance(b, bool):
            # MySQL JSON serialization uses binary doubles while ClickHouse
            # Decimal aggregation is converted to float at the API boundary.
            # Allow a tiny absolute/relative conversion error; larger
            # differences remain concrete field mismatches.
            difference = abs(float(a) - float(b))
            tolerance = max(1e-8, 1e-12 * max(abs(float(a)), abs(float(b)), 1.0))
            if difference > tolerance:
                diffs.append(f"{current}: mysql={a!r} clickhouse={b!r}")
            return
        if a != b:
            diffs.append(f"{current}: mysql={a!r} clickhouse={b!r}")

    walk(left, right, path)
    return diffs


def _format_ck_funding_row(row: Dict[str, Any], as_of_ms: int | None) -> Dict[str, Any]:
    from coinx.repositories.homepage_series import format_funding_countdown, format_funding_rate

    predicted_rate = row.get("predicted_rate")
    funding_rate = row.get("funding_rate")
    next_funding_time = row.get("next_funding_time")
    return {
        "symbol": row.get("symbol"),
        "predicted_rate": predicted_rate,
        "predicted_rate_formatted": format_funding_rate(predicted_rate),
        "funding_rate": funding_rate,
        "funding_rate_formatted": format_funding_rate(funding_rate),
        "next_funding_time": next_funding_time,
        "next_funding_time_formatted": format_funding_countdown(next_funding_time, as_of_ms),
        "mark_price": row.get("mark_price"),
        "is_abnormal": bool(
            abs(float(predicted_rate if predicted_rate is not None else funding_rate or 0))
            >= 0.001
        ),
        "event_time": row.get("event_time"),
    }


def _build_clickhouse_business_payloads(args: argparse.Namespace, mysql_payloads: Dict[str, Any]) -> Dict[str, Any]:
    """Run the existing business aggregators over a temporary CK-backed DB.

    ClickHouse remains the only source for market rows.  A temporary SQLite
    database is used solely because the current repositories accept a
    SQLAlchemy session; no production connection is patched or mutated.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from coinx import database
    from coinx.models import (
        MarketFundingRate,
        MarketKline,
        MarketOpenInterestHist,
        MarketTakerBuySellVol,
    )
    from coinx.read_clients import ClickHouseReadClient
    from coinx.repositories import contract_detail, funding_rate, homepage_series
    from coinx.repositories import market_structure_score, market_structure_series, trade_opportunities
    from coinx.web.routes import api_data

    homepage_result = mysql_payloads.get("homepage cold bypass") or {}
    homepage_symbols = [
        str(item.get("symbol"))
        for item in (homepage_result.get("data") or [])
        if item.get("symbol")
    ]
    trade_result = mysql_payloads.get("trade opportunities") or {}
    trade_symbol = next(
        (
            str(item.get("symbol"))
            for item in (trade_result.get("data") or [])
            if item.get("symbol")
        ),
        "BTCUSDT",
    )
    symbols = list(dict.fromkeys(homepage_symbols + ["BTCUSDT", trade_symbol]))
    if not symbols:
        raise RuntimeError("MySQL 首页没有可用于 CK 聚合验证的币种")

    as_of_ms = int(args.as_of_ms)
    # The 4h trade-opportunity trend uses 72 complete buckets (12 days).
    # Keep two extra days so the first aligned bucket is available as well;
    # shorter windows silently turn a valid historical result into
    # ``数据不足`` in the temporary validation database.
    lower_bound = max(0, as_of_ms - 14 * 24 * 3600000)
    symbol_sql = ", ".join("'" + symbol.replace("'", "''") + "'" for symbol in symbols)
    temp_path = Path(tempfile.gettempdir()) / f"coinx-clickhouse-business-{os.getpid()}-{int(time.time() * 1000)}.sqlite"
    engine = create_engine(
        f"sqlite:///{temp_path}",
        connect_args={"check_same_thread": False},
    )
    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    shadow_session = None
    original_get_sessions: Dict[Any, Any] = {}

    def _fetch(client, table: str, columns: str, time_column: str, lower: int = lower_bound, upper: int = as_of_ms):
        return client.query_rows(
            f"SELECT {columns} FROM coinx.{table} "
            f"WHERE symbol IN ({symbol_sql}) AND period = '5m' "
            f"AND {time_column} BETWEEN {int(lower)} AND {int(upper)}"
        )

    try:
        from coinx.database import Base

        Base.metadata.create_all(engine)
        with ClickHouseReadClient(
            args.clickhouse_url,
            args.clickhouse_database,
            args.clickhouse_user,
            args.clickhouse_password,
        ) as client:
            kline_rows = _fetch(
                client,
                "market_klines",
                "exchange, symbol, period, open_time, close_time, open_price, high_price, low_price, close_price, volume, quote_volume, trade_count, taker_buy_base_volume, taker_buy_quote_volume",
                "open_time",
            )
            oi_rows = _fetch(
                client,
                "market_open_interest_hist",
                "exchange, symbol, period, event_time, sum_open_interest, sum_open_interest_value",
                "event_time",
            )
            taker_rows = _fetch(
                client,
                "market_taker_buy_sell_vol",
                "exchange, symbol, period, event_time, buy_sell_ratio, buy_vol, sell_vol",
                "event_time",
            )
            funding_rows = _fetch(
                client,
                "market_funding_rate",
                "exchange, symbol, period, event_time, funding_rate, predicted_rate, next_funding_time, mark_price",
                "event_time",
                lower=max(0, as_of_ms - 24 * 3600000),
            )

        shadow_session = session_factory()
        for model, rows in (
            (MarketKline, kline_rows),
            (MarketOpenInterestHist, oi_rows),
            (MarketTakerBuySellVol, taker_rows),
            (MarketFundingRate, funding_rows),
        ):
            for offset in range(0, len(rows), 10000):
                shadow_session.bulk_insert_mappings(model, rows[offset:offset + 10000])
        shadow_session.commit()

        def shadow_get_session():
            return session_factory()

        # These modules imported get_session directly, so patch only their
        # function references for the duration of this local calculation.
        for module in (database, funding_rate, homepage_series, market_structure_series):
            original_get_sessions[module] = module.get_session
            module.get_session = shadow_get_session

        homepage_snapshot = homepage_series.get_homepage_series_snapshot(
            symbols=homepage_symbols,
            session=shadow_session,
            now_ms=as_of_ms,
        )
        homepage_data = api_data._format_homepage_coins_payload(homepage_snapshot.get("data") or [])
        payloads: Dict[str, Any] = {
            "homepage cold bypass": {
                "status": "success",
                "message": "homepage data loaded",
                "data": homepage_data,
                "cache_update_time": homepage_snapshot.get("cache_update_time"),
                "homepage_complete": api_data._is_complete_homepage_payload(homepage_data),
            },
        }
        payloads["homepage cache hit"] = dict(payloads["homepage cold bypass"])
        payloads["homepage forced bypass"] = dict(payloads["homepage cold bypass"])

        ck_funding = lambda requested, as_of_ms=None: funding_rate.load_latest_funding_rates(
            requested, session=shadow_session, as_of_ms=as_of_ms,
        )
        ck_homepage = lambda requested, now_ms=None: homepage_series.get_homepage_series_snapshot(
            symbols=requested, session=shadow_session, now_ms=now_ms,
        )
        detail = contract_detail.get_contract_detail(
            "BTCUSDT",
            homepage_loader=ck_homepage,
            funding_loader=ck_funding,
            now_ms=as_of_ms,
        )
        payloads["coin detail"] = {
            "status": "success", "message": "coin detail loaded", "data": detail,
        }
        score_snapshot = market_structure_score.get_market_structure_score_snapshot(
            symbols=["BTCUSDT"], session=shadow_session, now_ms=as_of_ms,
        )
        score_row = next(
            (item for item in score_snapshot.get("data") or [] if item.get("symbol") == "BTCUSDT"),
            None,
        )
        payloads["coin detail structure score"] = {
            "status": "success", "message": "contract structure score loaded",
            "data": {
                "symbol": "BTCUSDT",
                "as_of": score_snapshot.get("cache_update_time"),
                "structure_score": score_row,
            },
        }
        payloads["market structure score"] = {
            "status": "success", "message": "market structure score loaded",
            "data": score_snapshot.get("data") or [],
            "cache_update_time": score_snapshot.get("cache_update_time"),
            "summary": score_snapshot.get("summary") or {},
        }

        trade_opportunities._SNAPSHOT_CACHE.clear()
        trade_opportunities._SNAPSHOT_INFLIGHT.clear()
        trade_detail_snapshot = trade_opportunities.get_trade_opportunity_snapshot(
            symbols=["BTCUSDT"], now_ms=as_of_ms,
        )
        detail_opportunity = next(
            (
                item
                for item in trade_detail_snapshot.get("data") or []
                if item.get("symbol") == "BTCUSDT"
            ),
            None,
        )
        trade_snapshot = trade_opportunities.get_trade_opportunity_snapshot(
            symbols=[trade_symbol], now_ms=as_of_ms,
        )
        opportunity = next(
            (item for item in trade_snapshot.get("data") or [] if item.get("symbol") == trade_symbol),
            None,
        )
        payloads["coin detail trade opportunity"] = {
            "status": "success", "message": "contract trade opportunity loaded",
            "data": {
                "symbol": "BTCUSDT",
                "as_of": trade_detail_snapshot.get("cache_update_time"),
                "opportunity": detail_opportunity,
            },
        }
        payloads["trade opportunities"] = {
            "status": "success",
            "data": trade_snapshot.get("data") or [],
            "cache_update_time": trade_snapshot.get("cache_update_time"),
            "summary": trade_snapshot.get("summary") or {},
        }
        payloads["trade opportunities cache repeat"] = dict(payloads["trade opportunities"])
        return payloads
    finally:
        for module, original in original_get_sessions.items():
            module.get_session = original
        if shadow_session is not None:
            shadow_session.close()
        engine.dispose()
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def _build_clickhouse_comparable_payloads(args: argparse.Namespace) -> Dict[str, Any]:
    """Build the exact API shapes supported by the read-only ClickHouse contract."""
    from coinx.config import FUNDING_RATE_ABNORMAL_THRESHOLD
    from coinx.read_clients import ClickHouseReadClient
    from coinx.repositories.market_read import ClickHouseMarketReadRepository
    from coinx.repositories.homepage_series import format_funding_rate

    payloads: Dict[str, Any] = {}
    with ClickHouseReadClient(
        args.clickhouse_url,
        args.clickhouse_database,
        args.clickhouse_user,
        args.clickhouse_password,
    ) as client:
        repo = ClickHouseMarketReadRepository(client, args.clickhouse_database)
        page_result = repo.latest_funding_rate_page(
            page=1, page_size=20, threshold=FUNDING_RATE_ABNORMAL_THRESHOLD, as_of_ms=args.as_of_ms,
        )
        page_data = [_format_ck_funding_row(row, args.as_of_ms) for row in page_result["data"]]
        sparklines = repo.funding_rate_sparklines(
            [row["symbol"] for row in page_data], hours=24, as_of_ms=args.as_of_ms,
        )
        for row in page_data:
            row["sparkline"] = sparklines.get(row["symbol"], [])
        payloads["funding page"] = {
            "status": "success", "message": "funding rates loaded", "data": page_data,
            "total_count": page_result["total_count"], "page": 1, "page_size": 20,
            "threshold": FUNDING_RATE_ABNORMAL_THRESHOLD, "stats": page_result["stats"],
        }

        abnormal_data = [_format_ck_funding_row(row, args.as_of_ms) for row in repo.abnormal_funding_rates(
            threshold=FUNDING_RATE_ABNORMAL_THRESHOLD, as_of_ms=args.as_of_ms,
        )]
        abnormal_data.sort(key=lambda row: abs(row.get("predicted_rate") or 0), reverse=True)
        for row in abnormal_data:
            row.pop("is_abnormal", None)
            row.pop("sparkline", None)
        payloads["abnormal funding"] = {
            "status": "success", "message": "abnormal funding rates loaded",
            "data": abnormal_data, "threshold": FUNDING_RATE_ABNORMAL_THRESHOLD,
        }

        for symbol in ("BTCUSDT", "ETHUSDT"):
            history = repo.funding_rate_history(symbol, hours=24, now_ms=args.as_of_ms)
            payloads[f"funding history {symbol}"] = {
                "status": "success", "message": f"funding rate history loaded for {symbol}",
                "data": [
                    {
                        "symbol": row.get("symbol"), "event_time": int(row["event_time"]),
                        "predicted_rate": row.get("predicted_rate"),
                        "predicted_rate_formatted": format_funding_rate(row.get("predicted_rate")),
                        "funding_rate": row.get("funding_rate"),
                        "funding_rate_formatted": format_funding_rate(row.get("funding_rate")),
                        "mark_price": row.get("mark_price"),
                    }
                    for row in history
                ],
                "symbol": symbol, "hours": 24,
            }

        ticker_rows = repo.latest_tickers(
            rank_type="price_change", direction="down", limit=20, as_of_ms=args.as_of_ms,
        )
        payloads["market rank"] = {
            "status": "success", "message": "market rank data loaded",
            "data": [
                {
                    "symbol": row.get("symbol"), "rank_index": index,
                    "price": float(row["last_price"]) if row.get("last_price") else None,
                    "price_change_percent": float(row["price_change_percent"]) if row.get("price_change_percent") else None,
                    "volume": float(row["volume"]) if row.get("volume") else None,
                    "quote_volume": float(row["quote_volume"]) if row.get("quote_volume") else None,
                }
                for index, row in enumerate(ticker_rows, 1)
            ],
            "snapshot_time": ticker_rows[0].get("close_time") if ticker_rows else None,
        }
        payloads["coin detail series"] = {
            "status": "success",
            "message": "contract series loaded",
            "data": repo.contract_chart_series("BTCUSDT", hours=24, as_of_ms=args.as_of_ms),
        }
    return payloads


def _run_clickhouse_comparisons(
    args: argparse.Namespace,
    mysql_payloads: Dict[str, Any],
    report: Dict[str, Any],
    failures: List[str],
) -> None:
    comparable = _build_clickhouse_comparable_payloads(args)
    comparable.update(_build_clickhouse_business_payloads(args, mysql_payloads))
    comparable_labels = set(comparable)
    for label in sorted(comparable_labels):
        mysql_payload = mysql_payloads.get(label)
        clickhouse_payload = comparable.get(label)
        diffs = _json_diffs(mysql_payload, clickhouse_payload)
        status = "PASS" if not diffs else "DIFF"
        report["comparisons"].append({
            "label": label,
            "path": next((item["path"] for item in report["api_checks"] if item["label"] == label), ""),
            "status": status,
            "diffs": diffs,
            "mysql_summary": _report_payload_summary(mysql_payload),
            "clickhouse_summary": _report_payload_summary(clickhouse_payload),
            "mysql_payload": mysql_payload,
            "clickhouse_payload": clickhouse_payload,
        })
        if diffs:
            failures.append(f"{label}: ClickHouse 接口结果存在差异（{len(diffs)} 个字段）")

    not_comparable_labels = [
        item["label"] for item in report["api_checks"] if item["label"] not in comparable_labels
    ]
    for label in not_comparable_labels:
        item = next(item for item in report["api_checks"] if item["label"] == label)
        report["comparisons"].append({
            "label": label,
            "path": item["path"],
            "status": "NOT_COMPARABLE",
            "diffs": ["尚未实现 ClickHouse 等价查询"],
            "mysql_summary": _report_payload_summary(mysql_payloads.get(label)),
            "clickhouse_summary": {"status": "未执行"},
        })
        failures.append(f"{label}: 尚未实现 ClickHouse 等价查询")


def _write_reports(report: Dict[str, Any], report_file: str) -> tuple[Path, Path]:
    json_path = Path(report_file)
    if not json_path.is_absolute():
        json_path = Path(ROOT) / json_path
    payload_text = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    try:
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(payload_text, encoding="utf-8")
    except PermissionError:
        fallback_dir = Path(os.getenv("TEMP") or os.getenv("TMP") or ".")
        json_path = fallback_dir / json_path.name
        json_path.write_text(payload_text, encoding="utf-8")

    markdown_path = json_path.with_suffix(".md")
    label_map = {
        "funding page": "资金费率分页",
        "abnormal funding": "异常资金费率",
        "funding history BTCUSDT": "BTCUSDT 资金费率历史",
        "funding history ETHUSDT": "ETHUSDT 资金费率历史",
        "market rank": "行情排行",
        "homepage cold bypass": "首页首次请求",
        "homepage cache hit": "首页缓存命中",
        "homepage forced bypass": "首页强制绕过缓存",
        "coin detail": "合约详情",
        "coin detail series": "合约详情图表",
        "coin detail structure score": "合约详情结构评分",
        "coin detail trade opportunity": "合约详情交易机会",
        "market structure score": "市场结构评分",
        "trade opportunities": "交易机会列表",
        "trade opportunities cache repeat": "交易机会缓存重复请求",
        "homepage cache identity/equality check": "首页缓存对象与内容检查",
        "homepage nocache bypass check": "首页强制绕过检查",
        "trade opportunity cache check": "交易机会缓存检查",
    }
    def localize_message(message: str) -> str:
        localized = str(message)
        for source in sorted(label_map, key=len, reverse=True):
            target = label_map[source]
            localized = localized.replace(source, target)
        return localized
    status_map = {
        "PASS": "通过",
        "FAIL": "失败",
        "DIFF": "差异",
        "NOT_COMPARABLE": "不可比较",
        "ERROR": "错误",
    }
    metadata = report["metadata"]
    summary = report["summary"]
    if metadata.get("compare_clickhouse"):
        gate = "通过" if summary.get("comparison_ready") else "阻塞"
    else:
        gate = "未执行"
    lines = [
        "# ClickHouse API 只读验证报告",
        "",
        f"生成时间：`{metadata['generated_at']}`",
        f"MySQL：`{metadata['mysql_target']}`",
        f"ClickHouse：`{metadata.get('clickhouse_target') or '-'}`",
        f"Shadow 只读对比：`{'开启' if metadata['shadow_enabled'] else '关闭'}`",
        f"回放模式：`{'自动共同时间' if metadata.get('as_of_mode') == 'auto' else ('手工时间' if metadata.get('as_of_mode') == 'manual' else '当前时间')}`",
        f"回放时间（Unix 毫秒）：`{metadata.get('as_of_ms') or '当前时间'}`",
        "数值字段比较容差：`1e-8`（仅忽略 MySQL double 与 ClickHouse Decimal 转 float 的舍入噪声）",
        "",
        "## 结论摘要",
        "",
        f"- 接口请求：{summary['api_requests']} 个，失败 {summary['api_failures']} 个",
        f"- 缓存检查：{summary['cache_checks']} 个，失败 {summary['cache_failures']} 个",
        f"- 性能告警：{summary['latency_warnings']} 个",
        f"- Shadow 事件：{summary['shadow_events']} 个",
        f"- 逐字段比较：{summary.get('comparisons', 0)} 个，通过 {summary.get('comparisons', 0) - summary.get('comparison_diffs', 0) - summary.get('not_comparable', 0)} 个",
        f"- 字段差异：{summary.get('comparison_diffs', 0)} 个接口；不可比较：{summary.get('not_comparable', 0)} 个接口",
        f"- 比较门禁：**{gate}**",
    ]
    time_bounds = metadata.get("time_bounds") or {}
    if time_bounds:
        lines.extend([
            "",
            "## 两库时间边界",
            "",
            "自动模式取所有非空表的 MySQL/ClickHouse 最大时间较小值，再向下对齐到 5 分钟；查询条件均为 `时间 <= as_of_ms`。",
            "",
            "| 表 | MySQL 最小/最大时间 | ClickHouse 最小/最大时间 |",
            "|---|---:|---:|",
        ])
        for table, item in time_bounds.items():
            mysql_bounds = item.get("mysql", {})
            ck_bounds = item.get("clickhouse", {})
            lines.append(
                f"| `{table}` | `{mysql_bounds.get('min_time')}` / `{mysql_bounds.get('max_time')}` | "
                f"`{ck_bounds.get('min_time')}` / `{ck_bounds.get('max_time')}` |"
            )
        if metadata.get("common_upper_ms") is not None:
            lines.extend(["", f"共同原始上界：`{metadata['common_upper_ms']}`；最终回放点：`{metadata.get('as_of_ms')}`。"])
    lines.extend([
        "",
        "## 接口检查",
        "",
        "接口检查同时列出 MySQL API 与 ClickHouse 等价读取的返回摘要；完整 JSON 值保存在同名 `.json` 报告中。",
        "",
        "| 接口 | 路径 | MySQL HTTP | MySQL 返回 | ClickHouse 返回 | 全量 JSON 比较 |",
        "|---|---|---:|---|---|---:|",
    ])
    comparison_by_label = {
        item["label"]: item for item in report.get("comparisons", [])
    }
    for item in report["api_checks"]:
        comparison = comparison_by_label.get(item["label"])
        mysql_summary = _report_payload_summary(
            comparison.get("mysql_payload") if comparison else item.get("payload_summary")
        )
        clickhouse_summary = (
            comparison.get("clickhouse_summary", {"status": "未执行"})
            if comparison
            else {"status": "未执行"}
        )
        comparison_status = (
            status_map.get(comparison.get("status"), comparison.get("status"))
            if comparison
            else "未执行"
        )
        mysql_text = json.dumps(mysql_summary, ensure_ascii=False, separators=(",", ":"))
        clickhouse_text = json.dumps(clickhouse_summary, ensure_ascii=False, separators=(",", ":"))
        lines.append(
            f"| {label_map.get(item['label'], item['label'])} | `{item['path']}` | {item.get('status_code', '-') } | "
            f"`{mysql_text}` | `{clickhouse_text}` | {comparison_status} |"
        )
    lines.extend(["", "## 缓存检查", "", "| 检查项 | 结果 | 详情 |", "|---|---:|---|"])
    for item in report["cache_checks"]:
        result = "通过" if item["ok"] else "失败"
        details = json.dumps(item.get("details", {}), ensure_ascii=False, separators=(",", ":"))
        lines.append(f"| {label_map.get(item['label'], item['label'])} | {result} | `{details}` |")
    lines.extend(["", "## 接口逐字段比较（完整 JSON）", "", "| 接口 | 路径 | 结果 | 差异数 |", "|---|---|---:|---:|"])
    for item in report.get("comparisons", []):
        lines.append(
            f"| {label_map.get(item['label'], item['label'])} | `{item['path']}` | {status_map.get(item['status'], item['status'])} | {len(item.get('diffs', []))} |"
        )
        for diff in item.get("diffs", [])[:10]:
            lines.append(f"|  |  |  | `{diff}` |")
    lines.extend(["", "## 告警", ""])
    if report["warnings"]:
        lines.extend(f"- {localize_message(warning)}" for warning in report["warnings"])
    else:
        lines.append("- 无")
    lines.extend(["", "## Shadow 事件", ""])
    if report.get("shadow_events"):
        lines.extend(
            f"- `{event['level']}` {event['message']}" for event in report["shadow_events"]
        )
    else:
        lines.append("- 无")
    lines.extend(["", "## 失败项", ""])
    if report["failures"]:
        lines.extend(f"- {localize_message(failure)}" for failure in report["failures"])
    else:
        lines.append("- 无")
    try:
        markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    except PermissionError:
        markdown_path = Path(os.getenv("TEMP") or os.getenv("TMP") or ".") / markdown_path.name
        markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, markdown_path


def main() -> int:
    args = parse_args()
    time_selection: Dict[str, Any] = {}
    if args.auto_as_of:
        try:
            time_selection = _resolve_auto_as_of(args)
            args.as_of_ms = int(time_selection["as_of_ms"])
            print(
                "自动选择共同时间: "
                f"as_of_ms={args.as_of_ms} "
                f"(共同上界={time_selection['common_upper_ms']})",
                flush=True,
            )
        except Exception as exc:
            print(f"自动选择共同时间失败: {exc!r}", file=sys.stderr, flush=True)
            return 2
    _configure_environment(args)
    app = _load_app()
    shadow_events: List[Dict[str, Any]] = []
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)
    for handler in root_logger.handlers:
        handler.setLevel(logging.CRITICAL)
    root_logger.addHandler(_ShadowCaptureHandler(shadow_events))

    from coinx.web.routes import api_data
    from coinx.repositories import trade_opportunities

    failures: List[str] = []
    warnings: List[str] = []
    report: Dict[str, Any] = {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "mysql_target": f"{args.mysql_host}/{args.mysql_database}",
            "clickhouse_target": args.clickhouse_url,
            "shadow_enabled": bool(args.shadow),
            "compare_clickhouse": bool(args.compare_clickhouse),
            "as_of_ms": args.as_of_ms,
            "as_of_mode": "auto" if args.auto_as_of else ("manual" if args.as_of_ms else "wall_clock"),
            "common_upper_ms": time_selection.get("common_upper_ms"),
            "time_bounds": time_selection.get("time_bounds", {}),
            "read_only": True,
        },
        "api_checks": [],
        "cache_checks": [],
        "comparisons": [],
        "warnings": warnings,
        "shadow_events": shadow_events,
        "failures": failures,
    }
    api_payloads: Dict[str, Any] = {}

    def get(label: str, path: str, check: Callable[[Any], bool] | None = None) -> Any:
        path = _with_as_of(path, args.as_of_ms)
        started = time.perf_counter()
        status_code = None
        try:
            response = client.get(path)
            elapsed = (time.perf_counter() - started) * 1000
            payload = _json_response(response)
            status_code = response.status_code
            check_ok = check(payload) if check else True
            ok = status_code == 200 and check_ok
            record = {
                "label": label,
                "path": path,
                "ok": ok,
                "status_code": status_code,
                "elapsed_ms": round(elapsed, 1),
                "payload_summary": _payload_summary(payload),
                "payload": payload,
            }
            if isinstance(payload, dict) and payload.get("homepage_complete") is False:
                warning = f"{label} 返回 homepage_complete=false，首页数据不完整"
                warnings.append(warning)
                record["content_warning"] = warning
            if label == "market structure score" and record["payload_summary"].get("data_count") == 0:
                warning = "市场结构评分返回空数据列表"
                warnings.append(warning)
                record["content_warning"] = warning
            if not ok:
                record["error"] = "unexpected HTTP status or payload shape"
            if elapsed >= 10000:
                warning = f"{label} 耗时 {elapsed / 1000:.1f} 秒"
                warnings.append(warning)
                record["latency_warning"] = warning
        except Exception as exc:
            elapsed = (time.perf_counter() - started) * 1000
            payload = None
            ok = False
            record = {
                "label": label,
                "path": path,
                "ok": False,
                "status_code": None,
                "elapsed_ms": round(elapsed, 1),
                "payload_summary": {},
                "error": repr(exc),
            }
        report["api_checks"].append(record)
        api_payloads[label] = payload
        if not ok:
            failures.append(f"{label}: HTTP {status_code} 或响应结构不符合预期，payload={payload!r}")
        print(f"[{('OK' if ok else 'FAIL')}] {label}: status={status_code} ms={elapsed:.0f}", flush=True)
        return payload

    def cache_check(label: str, ok: bool, details: Dict[str, Any]) -> None:
        item = {"label": label, "ok": bool(ok), "details": details}
        report["cache_checks"].append(item)
        if not ok:
            failures.append(f"{label}: {details}")
        print(f"[{('OK' if ok else 'FAIL')}] {label}: {details}", flush=True)

    with app.test_client() as client:
        print("Read-only API verification (no refresh/write endpoints)", flush=True)

        def has_data(payload: Any) -> bool:
            return isinstance(payload, dict) and payload.get("status") == "success"

        get("funding page", "/api/funding-rate?page=1&page_size=20", has_data)
        get("abnormal funding", "/api/funding-rate/abnormal", has_data)
        get("funding history BTCUSDT", "/api/funding-rate/history/BTCUSDT?hours=24", has_data)
        get("funding history ETHUSDT", "/api/funding-rate/history/ETHUSDT?hours=24", has_data)
        get("market rank", "/api/market-rank?type=price_change&direction=down&limit=20", has_data)

        api_data.HOMEPAGE_SNAPSHOT_CACHE.clear()
        first = get("homepage cold bypass", "/api/coins?nocache=1", has_data)
        cache_values = list(api_data.HOMEPAGE_SNAPSHOT_CACHE.values())
        cold_cache_id = id(cache_values[0]) if cache_values else None
        second = get("homepage cache hit", "/api/coins", has_data)
        cache_values = list(api_data.HOMEPAGE_SNAPSHOT_CACHE.values())
        hit_cache_id = id(cache_values[0]) if cache_values else None
        cache_check(
            "homepage cache identity/equality check",
            cold_cache_id is not None and hit_cache_id == cold_cache_id and second == first,
            {"cold_cache_id": cold_cache_id, "hit_cache_id": hit_cache_id, "payload_equal": second == first},
        )

        third = get("homepage forced bypass", "/api/coins?nocache=1", has_data)
        cache_values = list(api_data.HOMEPAGE_SNAPSHOT_CACHE.values())
        bypass_cache_id = id(cache_values[0]) if cache_values else None
        cache_check(
            "homepage nocache bypass check",
            bypass_cache_id is not None and bypass_cache_id != hit_cache_id and third is not None,
            {"hit_cache_id": hit_cache_id, "bypass_cache_id": bypass_cache_id},
        )

        get("coin detail", "/api/coin-detail/BTCUSDT", has_data)
        get("coin detail series", "/api/coin-detail/BTCUSDT/series?range=24h", has_data)
        get("coin detail structure score", "/api/coin-detail/BTCUSDT/structure-score", has_data)
        get("coin detail trade opportunity", "/api/coin-detail/BTCUSDT/trade-opportunity", has_data)
        get("market structure score", "/api/market-structure-score?symbol=BTCUSDT&limit=1", has_data)
        get("trade opportunities", "/api/trade-opportunities?scope=all&limit=1", has_data)

        before = dict(trade_opportunities._SNAPSHOT_CACHE)
        get("trade opportunities cache repeat", "/api/trade-opportunities?scope=all&limit=1", has_data)
        after = dict(trade_opportunities._SNAPSHOT_CACHE)
        common_keys = set(before) & set(after)
        reused = bool(common_keys) and any(before[key] is after[key] for key in common_keys)
        cache_check(
            "trade opportunity cache check",
            bool(after) and (not before or reused),
            {"before_entries": len(before), "after_entries": len(after), "reused_object": reused},
        )

    if args.compare_clickhouse:
        try:
            print("Comparing concrete API results with ClickHouse...", flush=True)
            _run_clickhouse_comparisons(args, api_payloads, report, failures)
            for item in report["comparisons"]:
                print(
                    f"[{item['status']}] {item['label']}: diffs={len(item.get('diffs', []))}",
                    flush=True,
                )
        except Exception as exc:
            failures.append(f"ClickHouse 接口比较失败：{exc!r}")
            report["comparisons"].append({
                "label": "ClickHouse API comparison", "path": "", "status": "ERROR",
                "diffs": [repr(exc)],
            })

    report["summary"] = {
        "api_requests": len(report["api_checks"]),
        "api_failures": sum(1 for item in report["api_checks"] if not item["ok"]),
        "cache_checks": len(report["cache_checks"]),
        "cache_failures": sum(1 for item in report["cache_checks"] if not item["ok"]),
        "latency_warnings": len(warnings),
        "shadow_events": len(shadow_events),
        "comparisons": len(report.get("comparisons", [])),
        "comparison_diffs": sum(
            1 for item in report.get("comparisons", []) if item.get("status") == "DIFF"
        ),
        "not_comparable": sum(
            1 for item in report.get("comparisons", []) if item.get("status") == "NOT_COMPARABLE"
        ),
        "comparison_ready": bool(report.get("comparisons")) and all(
            item.get("status") == "PASS" for item in report.get("comparisons", [])
        ) if args.compare_clickhouse else None,
    }
    json_path, markdown_path = _write_reports(report, args.report_file)
    print(f"API verification complete: failures={len(failures)} warnings={len(warnings)}", flush=True)
    print(f"Detailed JSON report: {json_path}", flush=True)
    print(f"Detailed Markdown report: {markdown_path}", flush=True)
    if failures:
        for failure in failures:
            print(f"  - {failure}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
