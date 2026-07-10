"""
首页 API 真实数据库性能分析

统计首页数据加载各环节在真实数据库上的执行时间。
在 SQLite 测试库中跑没有意义（SQLite 性能特征与 MySQL/StarRocks 不同），
必须连接真实数据库运行。

运行方式:
    HOMEPAGE_PROFILE=1 python -m pytest tests/test_homepage_profile.py -s -v

    # 可指定部分币种以快速验证:
    HOMEPAGE_PROFILE=1 HOMEPAGE_PROFILE_SYMBOLS=BTCUSDT,ETHUSDT python -m pytest tests/test_homepage_profile.py -s -v
"""
import os
import threading
import time
from datetime import datetime

import pytest
from flask import Flask
from sqlalchemy import event, text
from sqlalchemy.orm import sessionmaker

PROFILE_REPORT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs', 'profile_reports')

from coinx.database import engine
from coinx.repositories import homepage_series as _hps
from coinx.repositories import funding_rate as _fr
from coinx.web.routes.api_data import (
    _clear_homepage_snapshot_cache,
    api_data_bp,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get('HOMEPAGE_PROFILE'),
    reason="Set HOMEPAGE_PROFILE=1 to run homepage profiling against real database",
)

# 线程局部变量：用于 SQL 拦截器判断当前所属阶段
_tls = threading.local()


def _current_stage():
    return getattr(_tls, 'stage', None)


def _current_exchange():
    return getattr(_tls, 'exchange', None)


def _fmt_val(v):
    if isinstance(v, str):
        return f"'{v}'"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, (list, tuple)):
        return '(' + ', '.join(_fmt_val(x) for x in v) + ')'
    if v is None:
        return 'NULL'
    if hasattr(v, 'isoformat'):
        return f"'{v.isoformat()}'"
    return str(v)


def _interp_sql(sql, params):
    import re
    params = params or {}

    def _repl_list(m):
        v = params.get(m.group(1), '')
        if isinstance(v, (list, tuple)):
            return ', '.join(_fmt_val(x) for x in v)
        return _fmt_val(v)

    def _repl_val(m):
        return _fmt_val(params.get(m.group(1), m.group(0)))

    sql = re.sub(r'__\[POSTCOMPILE_(\w+)\]', _repl_list, sql)
    sql = re.sub(r'%\((\w+)\)s', _repl_val, sql)
    return sql




class TimingCollector:
    """线程安全的时间记录器"""

    def __init__(self):
        self._lock = threading.Lock()
        self._entries = []
        self._sqls = []

    def record(self, stage, elapsed_ms, exchange=None, detail=None):
        with self._lock:
            self._entries.append({
                'stage': stage,
                'elapsed_ms': elapsed_ms,
                'exchange': exchange,
                'detail': detail,
            })

    def record_sql(self, stage, exchange, sql, params=None):
        with self._lock:
            self._sqls.append({
                'stage': stage,
                'exchange': exchange,
                'sql': sql,
                'params': params or {},
            })

    def _fmt_explain(self, sql_text, params):
        interp = _interp_sql(sql_text, params)
        lines = [f"        {interp}"]
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"EXPLAIN {interp}"))
                for row in result:
                    detail = ' '.join(f"{c}={v}" for c, v in zip(result.keys(), row) if v is not None)
                    lines.append(f"          EXPLAIN: {detail}")
        except Exception as ex:
            lines.append(f"          EXPLAIN error: {ex}")
        return lines


    def report(self, api_elapsed=None, report_dir=None):
        lines = []
        lines.append('=' * 72)
        db_url = str(engine.url)
        masked = db_url.split('@')[1] if '@' in db_url else db_url
        lines.append(f' 首页 API 真实数据库性能分析               {masked}')
        lines.append('=' * 72)

        def _sql_label(stage, idx, total):
            if stage in ('OI查询', 'Kline查询', 'Taker查询'):
                if total == 2:
                    return ['取最新时间点  (MAX+GROUP BY)', '取完整范围数据'][idx]
                return f'SQL-{idx + 1}'
            if stage == '资金费率查询':
                return '子查询 JOIN 取最新费率'
            return f'SQL-{idx + 1}'

        exchanges = sorted({e['exchange'] for e in self._entries if e['exchange']})
        for ex in exchanges:
            ex_entries = [e for e in self._entries if e['exchange'] == ex]
            lines.append(f'\n  [{ex}]')
            for e in ex_entries:
                stage_timing = f"    {e['stage']:25s} [{e['elapsed_ms']:>9.2f}ms]"
                if e['detail'] and e['detail'] != e['exchange']:
                    stage_timing += f'  ({e["detail"]})'
                lines.append(stage_timing)
                stage_sqls = [s for s in self._sqls
                              if s['stage'] == e['stage'] and s['exchange'] == ex]
                for idx, s in enumerate(stage_sqls):
                    label = _sql_label(e['stage'], idx, len(stage_sqls))
                    lines.append(f"      [{label}]")
                    lines.extend(self._fmt_explain(s['sql'], s['params']))

        others = [e for e in self._entries if not e['exchange']]
        if others:
            lines.append('\n  [其他]')
            for e in others:
                stage_timing = f"    {e['stage']:25s} [{e['elapsed_ms']:>9.2f}ms]"
                lines.append(stage_timing)
                stage_sqls = [s for s in self._sqls
                              if s['stage'] == e['stage'] and not s['exchange']]
                for idx, s in enumerate(stage_sqls):
                    label = _sql_label(e['stage'], idx, len(stage_sqls))
                    lines.append(f"      [{label}]")
                    lines.extend(self._fmt_explain(s['sql'], s['params']))

        lines.append('\n' + '-' * 72)
        summary_stages = {'交易所加载合计', '资金费率查询'}
        stage_totals = {}
        for e in self._entries:
            if e['stage'] in summary_stages:
                stage_totals.setdefault(e['stage'], []).append(e['elapsed_ms'])
        grand = 0
        for stage, durations in sorted(stage_totals.items()):
            total = sum(durations)
            grand += total
            lines.append(f'  {stage:30s}: {total:>9.2f}ms  ({len(durations)} calls)')
        if grand > 0:
            lines.append(f'  {"查询累计（上级指标）":30s}: {grand:>9.2f}ms')
        if api_elapsed is not None:
            lines.append(f'\n  API 总耗时（含序列化）: {api_elapsed:.2f}ms')
        lines.append('=' * 72)
        text = '\n'.join(lines)

        if report_dir:
            os.makedirs(report_dir, exist_ok=True)
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_path = os.path.join(report_dir, f'homepage_profile_{ts}.log')
            with open(report_path, 'w', encoding='utf8') as f:
                f.write(text)
                f.write('\n')
            print(f'\n  >>> 报告已保存: {report_path}')

        return text


def _make_timed(original_fn, collector, stage, exchange_pos=None):
    def wrapper(*args, **kwargs):
        prev_stage = _current_stage()
        prev_exchange = _current_exchange()

        _tls.stage = stage
        exchange = kwargs.get('exchange')
        if exchange is None and exchange_pos is not None and len(args) > exchange_pos:
            exchange = args[exchange_pos]
        _tls.exchange = exchange

        start = time.perf_counter()
        result = original_fn(*args, **kwargs)
        elapsed = (time.perf_counter() - start) * 1000

        _tls.stage = prev_stage
        _tls.exchange = prev_exchange

        detail = None
        if stage == 'Taker查询':
            detail = f"{exchange}/{kwargs.get('period', '5m')}" if exchange else kwargs.get('period')
        elif stage in ('OI查询', 'Kline查询'):
            detail = exchange
        else:
            detail = exchange
        collector.record(stage, elapsed, exchange=exchange, detail=detail)
        return result
    return wrapper


def create_client():
    import werkzeug
    if not hasattr(werkzeug, '__version__'):
        werkzeug.__version__ = '3'
    app = Flask(__name__)
    app.register_blueprint(api_data_bp)
    return app.test_client()


def test_homepage_real_db_profile(monkeypatch):
    """在真实数据库上统计首页接口各环节执行时间"""
    collector = TimingCollector()

    # SQL 拦截器（必须在 collector 创建之后注册）
    def on_before_execute(conn, clause, multiparams, params, execution_options=None):
        stage = _current_stage()
        if stage is None:
            return
        exchange = _current_exchange()
        try:
            compiled = clause.compile(dialect=engine.dialect)
            sql_str = str(compiled)
            # 优先取 compiled.params（ORM 模式下 event.params 可能为空）
            captured = compiled.params if not params else params
        except Exception:
            sql_str = str(clause)
            captured = {}
        collector.record_sql(stage, exchange, sql_str, params=captured)

    event.listen(engine, "before_execute", on_before_execute)

    # 1. 恢复真实数据库连接（conftest.py 已将 get_session 指向 SQLite）
    real_maker = sessionmaker(bind=engine)
    monkeypatch.setattr('coinx.database.get_session', real_maker)

    # 2. 获取活跃币种
    from coinx.coin_manager import get_active_coins
    active_coins = get_active_coins()
    assert active_coins, '数据库中没有 is_tracking=TRUE 的币种，请先配置 coins 表'

    # 3. 按环境变量过滤币种
    filter_raw = os.environ.get('HOMEPAGE_PROFILE_SYMBOLS')
    if filter_raw:
        symbols = [s.strip() for s in filter_raw.split(',') if s.strip()]
        symbols = [s for s in symbols if s in active_coins]
        assert symbols, f'指定的币种 {filter_raw} 均不在活跃列表中'
    else:
        symbols = active_coins

    print(f'\n  币种数: {len(symbols)}')
    if len(symbols) <= 20:
        print(f'  币种: {symbols}')

    # 4. 清理 API 内存缓存
    _clear_homepage_snapshot_cache()

    # 5. 避免触发后台修补干扰分析
    monkeypatch.setattr('coinx.web.routes.api_data._start_homepage_refresh_async',
                        lambda *a, **kw: False)
    monkeypatch.setattr('coinx.web.routes.api_data._start_market_structure_refresh_async',
                        lambda *a, **kw: False)
    monkeypatch.setattr('coinx.web.routes.api_data.get_market_structure_score_symbols',
                        lambda: [])

    # 6. 包装关键函数，自动计时
    for fn_name, stage, ex_pos in [
        ('_load_open_interest_model_map', 'OI查询', None),
        ('_load_kline_model_map', 'Kline查询', None),
        ('_load_net_inflow_sql', '净流入SQL查询', 1),
        ('_load_exchange_homepage_maps', '交易所加载合计', 1),
    ]:
        original = getattr(_hps, fn_name)
        monkeypatch.setattr(
            _hps, fn_name,
            _make_timed(original, collector, stage, exchange_pos=ex_pos),
        )

    original_fr = _fr.load_latest_funding_rates

    def timed_funding_rate(symbols, **kwargs):
        prev_stage = _current_stage()
        prev_exchange = _current_exchange()
        _tls.stage = '资金费率查询'
        _tls.exchange = 'binance'

        start = time.perf_counter()
        result = original_fr(symbols, **kwargs)
        elapsed = (time.perf_counter() - start) * 1000

        _tls.stage = prev_stage
        _tls.exchange = prev_exchange

        collector.record('资金费率查询', elapsed, exchange='binance')
        return result

    monkeypatch.setattr(_fr, 'load_latest_funding_rates', timed_funding_rate)
    monkeypatch.setattr(_hps, 'load_latest_funding_rates', timed_funding_rate)

    # 7. 调用首页 API（强制跳过内存缓存）
    client = create_client()
    api_start = time.perf_counter()
    response = client.get('/api/coins?wait=true&nocache=1')
    api_elapsed = (time.perf_counter() - api_start) * 1000

    # 8. 输出报告并保存到文件
    print()
    report_text = collector.report(api_elapsed=api_elapsed, report_dir=PROFILE_REPORT_DIR)
    print(report_text)
    print()

    # 9. 清理事件监听（必须在验证前处理，避免干扰后续测试）
    event.remove(engine, "before_execute", on_before_execute)
    _clear_homepage_snapshot_cache()

    # 10. 验证请求正常
    assert response.status_code == 200
    assert response.get_json()['status'] == 'success'
