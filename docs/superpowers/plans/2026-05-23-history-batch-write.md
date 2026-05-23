# History Batch Write Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make history repair defer writes and use the same grouped batch flush path as rolling repair.

**Architecture:** Keep fetch and parse behavior inside history workers, but return `pending_records` instead of writing per page. Extract the grouped exchange-level flush logic into one shared helper that both rolling and history call after worker execution. Preserve current repository upsert and deadlock retry behavior.

**Tech Stack:** Python, pytest, SQLAlchemy, APScheduler-driven repair pipeline

---

### Task 1: Add a failing history grouped-write test

**Files:**
- Modify: `tests/test_exchange_repair.py`
- Test: `tests/test_exchange_repair.py`

- [ ] **Step 1: Write the failing test**

```python
def test_exchange_history_repair_batches_group_writes(monkeypatch):
    _clear_rate_limit_states()
    calls = []
    upsert_calls = []
    adapter = FakeAdapter('binance', ('klines',), ('klines',), calls)

    monkeypatch.setattr('coinx.collector.exchange_repair.get_exchange_adapters', lambda exchanges: [adapter])
    monkeypatch.setattr(
        'coinx.collector.exchange_repair.upsert_series_records',
        lambda exchange, series_type, records, session=None: upsert_calls.append(
            (exchange, series_type, [record['open_time'] for record in records])
        ) or len(records),
    )

    summary = repair_history_symbols(
        symbols=['BTCUSDT', 'ETHUSDT'],
        series_types=['klines'],
        exchanges=['binance'],
        now_ms=1500000,
        full_scan=True,
        max_workers=1,
        coverage_hours=1,
        db_session=None,
    )

    assert summary['success_count'] == 2
    assert len(calls) == 2
    assert upsert_calls == [('binance', 'klines', [0, 0])]
    assert [item['affected'] for item in summary['results']] == [1, 1]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_exchange_repair.py::test_exchange_history_repair_batches_group_writes -v`
Expected: FAIL because current history flow writes inside the worker loop instead of one grouped flush.

- [ ] **Step 3: Write minimal implementation**

```python
# In src/coinx/collector/exchange_repair.py history worker:
pending_records = []
...
pending_records.extend(filtered_records)
...
return _result_with_breakdown(
    {
        ...
        'affected': 0,
        'records': record_count,
        'pages': pages,
        'pending_records': pending_records,
    },
    breakdown,
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_exchange_repair.py::test_exchange_history_repair_batches_group_writes -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_exchange_repair.py src/coinx/collector/exchange_repair.py
git commit -m "test: cover history grouped batch writes"
```

### Task 2: Extract the shared grouped flush helper

**Files:**
- Modify: `src/coinx/collector/exchange_repair.py`
- Test: `tests/test_exchange_repair.py`

- [ ] **Step 1: Write the failing shared-helper test**

```python
def test_exchange_repair_grouped_flush_handles_multiple_series(monkeypatch):
    _clear_rate_limit_states()
    upsert_calls = []

    monkeypatch.setattr(
        'coinx.collector.exchange_repair.upsert_series_records',
        lambda exchange, series_type, records, session=None: upsert_calls.append(
            (exchange, series_type, len(records))
        ) or len(records),
    )

    group_results = [
        {'series_type': 'klines', 'pending_records': [{'open_time': 1}], 'duration_breakdown_ms': {}},
        {'series_type': 'open_interest_hist', 'pending_records': [{'event_time': 2}], 'duration_breakdown_ms': {}},
    ]

    flushed = _flush_group_records('binance', group_results, db_session=None)

    assert upsert_calls == [('binance', 'klines', 1), ('binance', 'open_interest_hist', 1)]
    assert all('pending_records' not in item for item in flushed)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_exchange_repair.py::test_exchange_repair_grouped_flush_handles_multiple_series -v`
Expected: FAIL because `_flush_group_records` does not exist yet.

- [ ] **Step 3: Write minimal implementation**

```python
def _flush_group_records(exchange, group_results, db_session=None):
    pending_by_series = {}
    result_refs_by_series = {}
    ...
    for series_type, records in pending_by_series.items():
        with timed_category(write_breakdown, 'db_write_ms'):
            for batch in _chunks(records, REPAIR_ROLLING_WRITE_BATCH_SIZE):
                affected += upsert_series_records(exchange, series_type, batch, session=db_session)
    ...
    return group_results
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_exchange_repair.py::test_exchange_repair_grouped_flush_handles_multiple_series -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_exchange_repair.py src/coinx/collector/exchange_repair.py
git commit -m "refactor: share grouped repair flush logic"
```

### Task 3: Wire rolling and history through the shared flush path

**Files:**
- Modify: `src/coinx/collector/exchange_repair.py`
- Test: `tests/test_exchange_repair.py`

- [ ] **Step 1: Add the final integration assertions**

```python
def test_exchange_rolling_repair_batches_group_writes(...):
    ...
    assert upsert_calls == [('binance', 'klines', [1200000, 1200000])]


def test_exchange_history_repair_batches_group_writes(...):
    ...
    assert upsert_calls == [('binance', 'klines', [0, 0])]
```

- [ ] **Step 2: Run focused tests**

Run: `pytest tests/test_exchange_repair.py -k "group_writes or grouped_flush" -v`
Expected: PASS for rolling, shared flush, and history grouped-write tests.

- [ ] **Step 3: Write minimal implementation**

```python
# rolling exchange group
group_results = _flush_group_records(exchange, group_results, db_session=db_session)

# history exchange group
group_results = skipped_results + _run_tasks(runnable_tasks, group_worker, 1, db_session=db_session)
group_results = _flush_group_records(exchange, group_results, db_session=db_session)
```

- [ ] **Step 4: Run broader repair tests**

Run: `pytest tests/test_exchange_repair.py tests/test_series_repository.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_exchange_repair.py src/coinx/collector/exchange_repair.py
git commit -m "perf: batch history repair writes by exchange group"
```
