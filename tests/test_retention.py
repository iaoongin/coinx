import logging
import time

import pytest

from coinx.models import MarketFundingRate, MarketFuturesSmartMoneySignal
from coinx.repositories import retention


def test_cleanup_mysql_removes_records_older_than_retention_window(db_session, monkeypatch):
    now_ms = int(time.time() * 1000)
    cutoff_ms = retention.retention_cutoff_ms(30, now_ms=now_ms)
    old_event_time = cutoff_ms - 1
    recent_event_time = cutoff_ms + 60 * 1000

    db_session.add_all([
        MarketFundingRate(
            symbol='OLDUSDT',
            period='5m',
            event_time=old_event_time,
            exchange='binance',
        ),
        MarketFundingRate(
            symbol='NEWUSDT',
            period='5m',
            event_time=recent_event_time,
            exchange='binance',
        ),
    ])
    db_session.commit()

    monkeypatch.setattr(
        retention,
        'MYSQL_RETENTION_TARGETS',
        (
            (MarketFundingRate, MarketFundingRate.event_time),
        ),
    )

    deleted = retention._cleanup_mysql_market_data(30, session=db_session)

    assert deleted['market_funding_rate'] == 1
    assert db_session.query(MarketFundingRate).count() == 1


def test_cleanup_smart_money_removes_expired_list_snapshots(db_session):
    now_ms = int(time.time() * 1000)
    cutoff_ms = retention.retention_cutoff_ms(30, now_ms=now_ms)
    db_session.add_all([
        MarketFuturesSmartMoneySignal(
            exchange='binance',
            time_range='30m',
            symbol='OLDUSDT',
            period='15m',
            snapshot_time=cutoff_ms - 1,
        ),
        MarketFuturesSmartMoneySignal(
            exchange='binance',
            time_range='30m',
            symbol='NEWUSDT',
            period='15m',
            snapshot_time=cutoff_ms + 60 * 1000,
        ),
    ])
    db_session.commit()

    deleted = retention.cleanup_expired_smart_money_data(30, session=db_session)

    assert deleted['market_futures_smart_money_signal'] == 1
    assert db_session.query(MarketFuturesSmartMoneySignal).count() == 1


def test_market_cleanup_does_not_delete_smart_money_snapshots(db_session, monkeypatch):
    now_ms = int(time.time() * 1000)
    cutoff_ms = retention.retention_cutoff_ms(30, now_ms=now_ms)
    db_session.add(MarketFuturesSmartMoneySignal(
        exchange='binance',
        time_range='30m',
        symbol='OLDUSDT',
        period='15m',
        snapshot_time=cutoff_ms - 1,
    ))
    db_session.commit()
    monkeypatch.setattr(
        retention,
        'MYSQL_RETENTION_TARGETS',
        ((MarketFundingRate, MarketFundingRate.event_time),),
    )

    deleted = retention._cleanup_mysql_market_data(30, session=db_session)

    assert 'market_futures_smart_money_signal' not in deleted
    assert db_session.query(MarketFuturesSmartMoneySignal).count() == 1


def test_mysql_cleanup_logs_table_progress_and_totals(db_session, monkeypatch, caplog):
    now_ms = int(time.time() * 1000)
    cutoff_ms = retention.retention_cutoff_ms(30, now_ms=now_ms)
    db_session.add_all([
        MarketFundingRate(
            symbol='OLD1USDT',
            period='5m',
            event_time=cutoff_ms - 2,
            exchange='binance',
        ),
        MarketFundingRate(
            symbol='OLD2USDT',
            period='5m',
            event_time=cutoff_ms - 1,
            exchange='binance',
        ),
    ])
    db_session.commit()
    monkeypatch.setattr(
        retention,
        'MYSQL_RETENTION_TARGETS',
        ((MarketFundingRate, MarketFundingRate.event_time),),
    )
    monkeypatch.setattr(retention, 'DELETE_BATCH_SIZE', 1)
    caplog.set_level(logging.INFO)

    retention._cleanup_mysql_market_data(30, session=db_session)

    messages = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith('[数据清理]')
    ]
    assert messages[0] == '[数据清理] 表开始 | market_funding_rate | 正在统计过期记录'
    assert messages[1] == '[数据清理] 表准备完成 | market_funding_rate | 待清理 2 条 | 预计 2 批'
    assert '进行中 | market_funding_rate | 第 1/2 批（预计） | 本批删除 1 条 | 累计 1 条' in messages[2]
    assert all(label in messages[2] for label in ('查询 ', '删除 ', '提交 '))
    assert '预计剩余 ' in messages[2]
    assert '进行中 | market_funding_rate | 第 2/2 批（预计） | 本批删除 1 条 | 累计 2 条' in messages[3]
    assert '预计剩余 0ms' in messages[3]
    assert '表完成 | market_funding_rate | 删除 2 条 | 共 2 批' in messages[4]


def test_cleanup_scans_expired_times_and_preserves_recent_and_new_rows(db_session, monkeypatch):
    cutoff = 1_000
    db_session.add_all([
        MarketFundingRate(id=1, symbol='OLD1', period='5m', event_time=999, exchange='binance'),
        MarketFundingRate(id=2, symbol='RECENT', period='5m', event_time=1001, exchange='binance'),
        MarketFundingRate(id=4, symbol='BOUNDARY', period='5m', event_time=1000, exchange='binance'),
        MarketFundingRate(id=6, symbol='SAME_TIME_1', period='5m', event_time=998, exchange='binance'),
        MarketFundingRate(id=7, symbol='BACKFILL', period='5m', event_time=998, exchange='binance'),
        MarketFundingRate(id=8, symbol='SAME_TIME', period='5m', event_time=998, exchange='binance'),
    ])
    db_session.commit()
    monkeypatch.setattr(retention, 'DELETE_BATCH_SIZE', 2)
    progress = []

    def record_progress(table, batch, total, deleted, **details):
        progress.append((batch, total, deleted, details))
        if batch == 1:
            db_session.add(MarketFundingRate(
                id=9, symbol='NEW_BACKFILL', period='5m', event_time=997, exchange='binance',
            ))
            db_session.commit()

    deleted = retention._delete_mysql_model_rows(
        db_session, MarketFundingRate, MarketFundingRate.event_time, cutoff,
        progress_callback=record_progress,
    )

    remaining = db_session.query(MarketFundingRate).order_by(MarketFundingRate.id).all()
    assert deleted == 4
    assert [row.id for row in remaining] == [2, 4, 9]
    batches = [event for event in progress if event[0]]
    assert [(batch, total, count) for batch, total, count, _ in batches] == [
        (1, 2, 2), (2, 2, 4),
    ]
    assert all(
        all(key in details for key in ('query_duration_ms', 'delete_duration_ms', 'commit_duration_ms'))
        for _, _, _, details in batches
    )


def test_cleanup_handles_empty_table(db_session):
    progress = []
    deleted = retention._delete_mysql_model_rows(
        db_session, MarketFundingRate, MarketFundingRate.event_time, 1_000,
        progress_callback=lambda *args, **kwargs: progress.append((args, kwargs)),
    )

    assert deleted == 0
    assert [item[1]['stage'] for item in progress] == ['table_started', 'counting_completed', 'table_finished']
    assert progress[-1][0][1:4] == (0, 0, 0)


def test_cleanup_rechecks_time_before_deleting_candidates(db_session, monkeypatch):
    from sqlalchemy import event

    db_session.add(MarketFundingRate(
        id=1, symbol='UPDATED', period='5m', event_time=999, exchange='binance',
    ))
    db_session.commit()
    connection = db_session.connection()

    def update_before_delete(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith('DELETE FROM MARKET_FUNDING_RATE'):
            cursor.execute('UPDATE market_funding_rate SET event_time = 1001 WHERE id = 1')

    event.listen(connection, 'before_cursor_execute', update_before_delete)
    try:
        deleted = retention._delete_mysql_model_rows(
            db_session, MarketFundingRate, MarketFundingRate.event_time, 1000,
        )
    finally:
        event.remove(connection, 'before_cursor_execute', update_before_delete)

    assert deleted == 0
    assert db_session.get(MarketFundingRate, 1).event_time == 1001


def test_mysql_cleanup_requires_time_index(db_session, monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setattr(db_session, 'get_bind', lambda: SimpleNamespace(dialect=SimpleNamespace(name='mysql')))
    monkeypatch.setattr(retention, 'inspect', lambda _bind: SimpleNamespace(
        get_indexes=lambda _table: [{'column_names': ['symbol', 'event_time']}],
    ))

    with pytest.raises(RuntimeError, match='缺少以 event_time 为首列的清理索引'):
        retention._delete_mysql_model_rows(db_session, MarketFundingRate, MarketFundingRate.event_time, 1000)
