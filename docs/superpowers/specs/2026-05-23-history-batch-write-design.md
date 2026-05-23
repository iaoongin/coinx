# History Batch Write Design

## Goal

Make `history` repair use the same grouped batch-write pattern as `rolling` repair so database writes are concentrated into fewer transactions and share one flush path.

## Scope

This change only affects market series repair in `src/coinx/collector/exchange_repair.py`.

- Keep existing fetch, parse, filtering, and summary behavior.
- Change `history` tasks from immediate per-page writes to deferred grouped writes.
- Extract the grouped flush logic into a reusable helper shared by `rolling` and `history`.
- Preserve current write batching via `upsert_series_records()` and existing retry behavior in `src/coinx/repositories/series.py`.

## Approaches Considered

### Recommended: shared deferred flush by exchange and series type

Each `history` worker accumulates `pending_records` during page iteration and returns them with task metadata. After all tasks in one exchange group finish, a shared flush helper groups records by `series_type`, writes them in batches, and attributes `db_write_ms` and affected rows back to task results by record-count ratio.

Why this is the best fit:

- Matches the existing `rolling` design, reducing mental overhead.
- Concentrates writes into fewer transactions.
- Creates one code path for grouped batch writes, so future tuning happens in one place.

### Alternative: defer writes only within each history task

This would remove per-page commits, but each task would still write independently. It is simpler, but it keeps much of the lock contention surface and does not unify with `rolling`.

## Design

### Worker behavior

`history` workers will:

- fetch and parse pages exactly as today
- filter records exactly as today
- append filtered records to `pending_records`
- report `records`, `pages`, `start_time`, `end_time`, and `window_precise`
- stop performing `db_write_ms` timing locally

`rolling` workers already return `pending_records`, so their contract stays compatible with the shared flush helper.

### Shared flush helper

Extract `_flush_rolling_group_records()` into a generic helper that:

- consumes grouped task results for a single exchange
- removes `pending_records` from each result
- groups all pending records by `series_type`
- writes each series group in `REPAIR_ROLLING_WRITE_BATCH_SIZE` chunks through `upsert_series_records()`
- measures total `db_write_ms`
- distributes `affected` and `db_write_ms` back to source task results proportionally by record count

The helper remains exchange-scoped because both repair modes already group execution by exchange.

### Logging and summaries

- Exchange-level duration summaries continue to include `db_write_ms`.
- Per-task results still expose `affected` and write duration after proportional allocation.
- No schema or repository API changes are required.

## Error Handling

- Fetch and parse failures remain on the worker path and continue to mark the task as `error`.
- Write failures still surface from `upsert_series_records()`, including current MySQL deadlock retries.
- If grouped flushing fails for an exchange group, the exception should propagate just as the rolling flush currently does.

## Testing

Add or update tests to verify:

- `history` tasks return deferred `pending_records` instead of writing within the worker loop
- grouped flush is shared by `history` and `rolling`
- `history` grouped writes batch by `series_type`
- per-task `affected` attribution still works after grouped flush

## Non-Goals

- No change to scheduler overlap behavior
- No change to batch-size configuration names
- No change to MySQL SQL shape in `upsert_series_records()`
